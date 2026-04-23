use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use tokio::sync::oneshot;

use crate::config::{AppConfig, ColumnType};
use crate::error::{AppError, AppResult};
use crate::execution::aggregator::{Aggregator, PartialResult};
use crate::execution::chunk::{ChunkId, Task};
use crate::execution::worker::Worker;
use crate::query::{ColumnKind, Comparison, Query, QueryId, RangeFilter, Response};
use crate::resource;
use crate::storage::{Column, ColumnStore};

/// Ensures fair scheduling in case of a large number of concurrent queries.
///
/// Responsibilities:
/// - assign a `QueryId` to every incoming query,
/// - break each query into per-chunk `Task`s,
/// - dispatch tasks via an MPMC channel so workers pull them round-robin
///   across active queries (this is what prevents FIFO head-of-line blocking),
/// - route each worker's partial result into the owning query's `Aggregator`,
/// - send the final `Response` back through a `oneshot` channel.
pub struct Coordinator {
    cfg: AppConfig,
    store: Arc<ColumnStore>,
    next_id: AtomicU64,
    inflight: Arc<Mutex<HashMap<QueryId, InflightQuery>>>,
    task_tx: flume::Sender<Task>,
}

/// State kept for every submitted query until all of its chunks have returned.
/// The `reply` channel is an `Option` so we can `take()` it once — completion
/// is a one-shot event.
struct InflightQuery {
    aggregator: Aggregator,
    reply: Option<oneshot::Sender<AppResult<Response>>>,
}

struct WorkerOutput {
    query_id: QueryId,
    partial: PartialResult,
}

impl Coordinator {
    /// Spawn the dedicated worker threads and the async result pump
    pub fn start(cfg: AppConfig, store: Arc<ColumnStore>) -> Self {
        let (task_tx, task_rx) = flume::unbounded::<Task>();
        let (result_tx, result_rx) = flume::unbounded::<WorkerOutput>();
        let inflight: Arc<Mutex<HashMap<QueryId, InflightQuery>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let worker_threads = cfg.engine.worker_threads.max(1);
        let string_value_counts = cfg.engine.string_value_counts;
        for wid in 0..worker_threads {
            let rx = task_rx.clone();
            let tx = result_tx.clone();
            let store = store.clone();
            thread::Builder::new()
                .name(format!("stats-worker-{wid}"))
                .spawn(move || {
                    let worker = Worker::new(store, string_value_counts);
                    worker.run(rx.into_iter(), |task, partial| {
                        let _ = tx.send(WorkerOutput { query_id: task.query_id, partial });
                    });
                })
                .expect("failed to spawn worker thread");
        }
        // Keep only the cloned senders/receivers owned by workers and the pump,
        // so the channels close naturally when the pool shuts down.
        drop(task_rx);
        drop(result_tx);

        let pump_state = inflight.clone();
        tokio::spawn(async move {
            while let Ok(output) = result_rx.recv_async().await {
                finalize(&pump_state, output);
            }
        });

        Self {
            cfg,
            store,
            next_id: AtomicU64::new(1),
            inflight,
            task_tx,
        }
    }

    /// Submit a query and asynchronously await its completion.
    ///
    /// Admission control: reject up front if the process is already over the
    /// configured memory limit.
    pub async fn submit(&self, query: Query) -> AppResult<Response> {
        resource::check_memory(self.cfg.engine.memory_limit)?;
        let (tx, rx) = oneshot::channel::<AppResult<Response>>();
        self.enqueue(query, tx)?;
        let mut response = rx.await.expect("coordinator dropped before responding")?;
        for (name, col) in &self.cfg.searchable {
            if col.hidden {
                response.numeric.remove(name.as_str());
            } else if response.numeric.contains_key(name.as_str()) {
                let kind = if matches!(col.column_type, ColumnType::DateTime) {
                    ColumnKind::DateTime
                } else {
                    ColumnKind::Numeric
                };
                response.column_types.insert(name.clone(), kind);
            }
        }
        Ok(response)
    }

    fn enqueue(
        &self,
        query: Query,
        reply: oneshot::Sender<AppResult<Response>>,
    ) -> AppResult<()> {
        validate(&query, &self.store)?;

        let row_count = self.store.row_count();
        let chunk_size = self.store.chunk_size();
        let chunk_count = self.store.chunk_count();

        if chunk_count == 0 {
            let _ = reply.send(Ok(Response::default()));
            return Ok(());
        }

        // Build skip mask before query is moved into Arc
        let skip_mask = build_skip_mask(chunk_count, &self.store, &query);
        let dispatched = skip_mask.iter().filter(|&&s| !s).count();

        if dispatched == 0 {
            let _ = reply.send(Ok(Response::default()));
            return Ok(());
        }

        let query_id = self.next_query_id();
        let query_arc = Arc::new(query);

        {
            let mut guard = self.inflight.lock().expect("inflight mutex poisoned");
            guard.insert(
                query_id,
                InflightQuery {
                    aggregator: Aggregator::new(dispatched),
                    reply: Some(reply),
                },
            );
        }

        let mut chunk_idx: usize = 0;
        let mut start = 0usize;
        while start < row_count {
            let end = (start + chunk_size).min(row_count);
            if !skip_mask[chunk_idx] {
                let task = Task {
                    chunk_id: ChunkId(chunk_idx as u64),
                    query_id,
                    query: query_arc.clone(),
                    rows: start..end,
                };
                if self.task_tx.send(task).is_err() {
                    let mut guard = self
                        .inflight
                        .lock()
                        .expect("inflight mutex poisoned");
                    if let Some(mut state) = guard.remove(&query_id) {
                        if let Some(tx) = state.reply.take() {
                            let _ = tx.send(Err(AppError::Execution(
                                "worker pool closed before query completed".into(),
                            )));
                        }
                    }
                    return Err(AppError::Execution("worker pool closed".into()));
                }
            }
            chunk_idx += 1;
            start = end;
        }

        Ok(())
    }

    fn next_query_id(&self) -> QueryId {
        QueryId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

fn finalize(state: &Mutex<HashMap<QueryId, InflightQuery>>, output: WorkerOutput) {
    let mut guard = state.lock().expect("inflight mutex poisoned");
    let Some(entry) = guard.get_mut(&output.query_id) else {
        return;
    };
    let Some(response) = entry.aggregator.merge(output.partial) else {
        return;
    };
    let mut done = guard.remove(&output.query_id).expect("entry present");
    drop(guard);
    if let Some(tx) = done.reply.take() {
        let _ = tx.send(Ok(response));
    }
}

fn build_skip_mask(chunk_count: usize, store: &ColumnStore, query: &Query) -> Vec<bool> {
    let Some((col, stats)) = store.zone_stats() else {
        return vec![false; chunk_count];
    };
    let Some(filters) = query.ranges.get(col) else {
        return vec![false; chunk_count];
    };
    // Skip a chunk only if it overlaps none of the filters (OR semantics across filters).
    stats
        .iter()
        .map(|&(lo, hi)| !filters.iter().any(|rf| zone_overlaps(lo, hi, rf)))
        .collect()
}

fn zone_overlaps(chunk_min: f64, chunk_max: f64, rf: &RangeFilter) -> bool {
    if let Some(upper) = rf.upper {
        let in_range = match upper.comparison {
            Comparison::Lt => chunk_min < upper.value,
            Comparison::Lte => chunk_min <= upper.value,
            _ => true,
        };
        if !in_range {
            return false;
        }
    }
    if let Some(lower) = rf.lower {
        let in_range = match lower.comparison {
            Comparison::Gt => chunk_max > lower.value,
            Comparison::Gte => chunk_max >= lower.value,
            _ => true,
        };
        if !in_range {
            return false;
        }
    }
    true
}

fn validate(query: &Query, store: &ColumnStore) -> AppResult<()> {
    for (name, _) in &query.must.0 {
        require_string_column(store, name, "must")?;
    }
    for (name, _) in &query.must_not.0 {
        require_string_column(store, name, "must_not")?;
    }
    for (name, _) in &query.ranges {
        require_numeric_column(store, name)?;
    }
    Ok(())
}

fn require_string_column(store: &ColumnStore, name: &str, clause: &str) -> AppResult<()> {
    match store.column(name) {
        Some(Column::String(_)) => Ok(()),
        Some(col) => Err(AppError::Query(format!(
            "{clause} filter on {name:?}: column is {}, expected string",
            col.type_name()
        ))),
        None => Err(AppError::Query(format!(
            "{clause} filter references unknown column {name:?}"
        ))),
    }
}

fn require_numeric_column(store: &ColumnStore, name: &str) -> AppResult<()> {
    match store.column(name) {
        Some(Column::Integer(_) | Column::Float(_) | Column::DateTime(_)) => Ok(()),
        Some(Column::String(_)) => Err(AppError::Query(format!(
            "range filter on {name:?}: column is string, expected numeric or date-time"
        ))),
        None => Err(AppError::Query(format!(
            "range filter references unknown column {name:?}"
        ))),
    }
}
