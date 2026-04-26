use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::api::AppState;
use crate::config::{self, AppConfig};
use crate::error::{AppError, AppResult};
use crate::execution::Coordinator;
use crate::ingestion::{self, Ingestor};
use crate::storage::ColumnStore;

pub async fn watcher(
    config_path: PathBuf,
    state: AppState,
    initial_cfg: AppConfig,
    initial_ingestor: Box<dyn Ingestor>,
) {
    let interval = Duration::from_secs(initial_cfg.engine.reload_interval_mins as u64 * 60);

    let mut cfg = initial_cfg;
    let mut last_cfg_mtime = mtime(&config_path);
    let mut ingestor: Option<Box<dyn Ingestor>> = Some(initial_ingestor);

    loop {
        tokio::time::sleep(interval).await;

        // Config-file change: re-parse, build a fresh ingestor, run it. On any
        // failure, keep the current state and try again next tick.
        let curr_cfg_mtime = mtime(&config_path);
        if curr_cfg_mtime != last_cfg_mtime {
            if curr_cfg_mtime.is_none() {
                tracing::warn!("reload skipped: config file is missing");
                continue;
            }
            tracing::info!("config changed, reloading");
            match config::load(&config_path) {
                Ok(new_cfg) => {
                    let new_ingestor = ingestion::select(&new_cfg);
                    match run_reload(new_ingestor).await {
                        (next, Ok(Some(store))) => {
                            apply(&state, &new_cfg, store);
                            cfg = new_cfg;
                            ingestor = Some(next);
                            last_cfg_mtime = curr_cfg_mtime;
                            tracing::info!("config reload complete");
                        }
                        (_, Ok(None)) => {
                            tracing::warn!("config reloaded but ingestor produced no data; keeping current state");
                        }
                        (_, Err(e)) => {
                            tracing::error!(error = %e, "config reload failed, keeping current state");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "config parse failed, keeping current state");
                }
            }
            continue;
        }

        // Data reload: hand the long-lived ingestor to a blocking task and put
        // it back, regardless of outcome.
        let owned = ingestor.take().expect("ingestor present");
        let (returned, result) = run_reload(owned).await;
        ingestor = Some(returned);
        match result {
            Ok(Some(store)) => {
                apply(&state, &cfg, store);
                tracing::info!("data reload complete");
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(error = %e, "data reload failed, keeping current state");
            }
        }
    }
}

/// Run a blocking `reload()` call on the worker pool, returning the (still
/// owned) ingestor along with the result.
async fn run_reload(
    mut ingestor: Box<dyn Ingestor>,
) -> (Box<dyn Ingestor>, AppResult<Option<ColumnStore>>) {
    tokio::task::spawn_blocking(move || {
        let result = ingestor.reload();
        (ingestor, result)
    })
    .await
    .expect("ingestion task panicked")
}

fn apply(state: &AppState, cfg: &AppConfig, store: ColumnStore) {
    let store = Arc::new(store);
    let rows = store.row_count() as u64;
    state.coordinator.store(Arc::new(Coordinator::start(cfg.clone(), store)));
    state.metrics.set_rows_loaded(rows);
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok()?.modified().ok()
}

/// Initial bootstrap reload: the first call must produce data, so a `None`
/// result is surfaced as an error rather than silently keeping a stale store.
pub async fn bootstrap_reload(
    ingestor: Box<dyn Ingestor>,
) -> AppResult<(Box<dyn Ingestor>, ColumnStore)> {
    let (ingestor, result) = run_reload(ingestor).await;
    let store = result?
        .ok_or_else(|| AppError::Ingestion("initial ingestion produced no data".into()))?;
    Ok((ingestor, store))
}
