use std::ops::Range;
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::execution::aggregator::PartialResult;
use crate::execution::chunk::Task;
use crate::query::{Comparison, NumericStats, Query, RangeFilter};
use crate::storage::{Column, ColumnStore};

/// A CPU-bound worker. Runs on a dedicated OS thread — never on the Tokio pool
pub struct Worker {
    store: Arc<ColumnStore>,
}

impl Worker {
    pub fn new(store: Arc<ColumnStore>) -> Self {
        Self { store }
    }

    /// Pop tasks off the channel and crunch them until the channel closes.
    /// The `tasks` iterator is typically a `flume::Receiver::into_iter()` and
    /// `results` is a closure that forwards `(Task, PartialResult)` over a
    /// result channel back to the coordinator.
    pub fn run<TaskSource, ResultSink>(&self, tasks: TaskSource, results: ResultSink)
    where
        TaskSource: Iterator<Item = Task>,
        ResultSink: Fn(Task, PartialResult),
    {
        for task in tasks {
            let partial = execute(&task, &self.store);
            results(task, partial);
        }
    }
}

/// Evaluate the query's filters against the chunk and fold numeric stats for
/// every row the chunk contributes.
fn execute(task: &Task, store: &ColumnStore) -> PartialResult {
    let matched = evaluate_filter(&task.query, store, task.rows.clone());
    aggregate(&matched, store)
}

fn evaluate_filter(query: &Query, store: &ColumnStore, rows: Range<usize>) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    bitmap.insert_range(rows.start as u32..rows.end as u32);

    for (name, sm) in &query.must.0 {
        let Some(Column::String(sc)) = store.column(name) else {
            // Unknown column, or column is not dictionary-backed. Either is a
            // filter that matches nothing by construction.
            return RoaringBitmap::new();
        };
        let mut union = RoaringBitmap::new();
        for value in &sm.values {
            if let Some(code) = sc.dictionary.code_of(value) {
                if let Some(p) = sc.dictionary.postings(code) {
                    union |= p;
                }
            }
        }
        bitmap &= union;
        if bitmap.is_empty() {
            return bitmap;
        }
    }

    for (name, sm) in &query.must_not.0 {
        let Some(Column::String(sc)) = store.column(name) else {
            continue;
        };
        let mut union = RoaringBitmap::new();
        for value in &sm.values {
            if let Some(code) = sc.dictionary.code_of(value) {
                if let Some(p) = sc.dictionary.postings(code) {
                    union |= p;
                }
            }
        }
        bitmap -= union;
        if bitmap.is_empty() {
            return bitmap;
        }
    }

    for (name, filters) in &query.ranges {
        match store.column(name) {
            Some(Column::Integer(v)) => retain_numeric(&mut bitmap, |row| {
                filters.iter().any(|rf| range_matches(v[row] as f64, rf))
            }),
            Some(Column::DateTime(v)) => retain_numeric(&mut bitmap, |row| {
                filters.iter().any(|rf| range_matches(v[row] as f64, rf))
            }),
            Some(Column::Float(v)) => retain_numeric(&mut bitmap, |row| {
                filters.iter().any(|rf| range_matches(v[row], rf))
            }),
            _ => return RoaringBitmap::new(),
        }
        // Null rows hold dummy values that must not match any range
        if let Some(nulls) = store.null_rows_for(name) {
            bitmap -= nulls;
        }
        if bitmap.is_empty() {
            return bitmap;
        }
    }

    bitmap
}

fn retain_numeric<F: Fn(usize) -> bool>(bitmap: &mut RoaringBitmap, keep: F) {
    let kept: RoaringBitmap = bitmap.iter().filter(|r| keep(*r as usize)).collect();
    *bitmap = kept;
}

fn range_matches(value: f64, rf: &RangeFilter) -> bool {
    if let Some(lower) = rf.lower {
        let ok = match lower.comparison {
            Comparison::Gt => value > lower.value,
            Comparison::Gte => value >= lower.value,
            Comparison::Lt | Comparison::Lte => false,
        };
        if !ok {
            return false;
        }
    }
    if let Some(upper) = rf.upper {
        let ok = match upper.comparison {
            Comparison::Lt => value < upper.value,
            Comparison::Lte => value <= upper.value,
            Comparison::Gt | Comparison::Gte => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn aggregate(matched: &RoaringBitmap, store: &ColumnStore) -> PartialResult {
    let mut partial = PartialResult {
        matched_rows: matched.len(),
        numeric: Default::default(),
        column_types: Default::default(),
    };
    for name in store.column_names() {
        let Some(col) = store.column(name) else { continue };
        let nulls = store.null_rows_for(name);
        let stats = match col {
            Column::Integer(v) => fold(matched, |row| v[row] as f64, nulls),
            Column::Float(v) => fold(matched, |row| v[row], nulls),
            Column::DateTime(v) => fold(matched, |row| v[row] as f64, nulls),
            Column::String(_) => continue,
        };
        partial.numeric.insert(name.clone(), stats);
    }
    partial
}

fn fold<F: Fn(usize) -> f64>(
    matched: &RoaringBitmap,
    value_at: F,
    nulls: Option<&RoaringBitmap>,
) -> NumericStats {
    let mut s = NumericStats::empty();
    for row in matched.iter() {
        if nulls.map_or(false, |n| n.contains(row)) {
            continue;
        }
        s.observe(value_at(row as usize));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiConfig, ColumnType, EngineConfig, SearchableColumn, SourceConfig};
    use crate::query::{Must, MustNot, NumericBound, RangeFilter, StringMatch};
    use crate::storage::RawValue;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn mk_cfg() -> crate::config::AppConfig {
        let mut searchable = BTreeMap::new();
        searchable.insert("amount".into(), SearchableColumn { column_type: ColumnType::Float, hidden: false });
        searchable.insert("country".into(), SearchableColumn { column_type: ColumnType::String, hidden: false });
        searchable.insert("user_id".into(), SearchableColumn { column_type: ColumnType::Integer, hidden: false });
        crate::config::AppConfig {
            source: SourceConfig::Csv {
                path: PathBuf::from("/dev/null"),
                delimiter: None,
                gzip: false,
            },
            engine: EngineConfig {
                memory_limit: u64::MAX,
                chunk_size_rows: 1024,
                worker_threads: 1,
            },
            api: ApiConfig { bind: "0.0.0.0:0".into() },
            searchable,
            partition_column: None,
        }
    }

    fn mk_store() -> ColumnStore {
        let cfg = mk_cfg();
        let mut store = ColumnStore::new(&cfg);
        // Column order after BTreeMap sort: amount, country, user_id.
        let rows = [
            (10.0_f64, "DE", 1_i64),
            (20.0, "PL", 2),
            (30.0, "DE", 3),
            (40.0, "FR", 4),
            (50.0, "PL", 5),
        ];
        for (a, c, u) in rows {
            store
                .push_row(&[
                    RawValue::Float(a),
                    RawValue::Str(c),
                    RawValue::Integer(u),
                ])
                .unwrap();
        }
        store
    }

    fn mk_query(
        must: &[(&str, &[&str])],
        must_not: &[(&str, &[&str])],
        ranges: &[(&str, Option<NumericBound>, Option<NumericBound>)],
    ) -> Query {
        let mut m = BTreeMap::new();
        for (col, vs) in must {
            m.insert(
                (*col).to_string(),
                StringMatch { values: vs.iter().map(|s| (*s).to_string()).collect() },
            );
        }
        let mut mn = BTreeMap::new();
        for (col, vs) in must_not {
            mn.insert(
                (*col).to_string(),
                StringMatch { values: vs.iter().map(|s| (*s).to_string()).collect() },
            );
        }
        let mut r = BTreeMap::new();
        for (col, lo, up) in ranges {
            r.insert((*col).to_string(), vec![RangeFilter { lower: *lo, upper: *up }]);
        }
        Query { must: Must(m), must_not: MustNot(mn), ranges: r }
    }

    fn run_query(store: &ColumnStore, query: Query) -> PartialResult {
        let task = Task {
            chunk_id: crate::execution::ChunkId(0),
            query_id: crate::query::QueryId(1),
            query: Arc::new(query),
            rows: 0..store.row_count(),
        };
        execute(&task, store)
    }

    #[test]
    fn no_filter_matches_everything_and_computes_stats() {
        let store = mk_store();
        let partial = run_query(&store, mk_query(&[], &[], &[]));
        assert_eq!(partial.matched_rows, 5);
        let amount = partial.numeric.get("amount").unwrap();
        assert_eq!(amount.count, 5);
        assert_eq!(amount.sum, 150.0);
        assert_eq!(amount.min, 10.0);
        assert_eq!(amount.max, 50.0);
        let uid = partial.numeric.get("user_id").unwrap();
        assert_eq!(uid.sum, 15.0);
        assert!(partial.numeric.get("country").is_none());
    }

    #[test]
    fn must_filters_to_matching_rows() {
        let store = mk_store();
        let partial = run_query(&store, mk_query(&[("country", &["DE", "PL"])], &[], &[]));
        assert_eq!(partial.matched_rows, 4);
        let amount = partial.numeric.get("amount").unwrap();
        assert_eq!(amount.sum, 10.0 + 20.0 + 30.0 + 50.0);
    }

    #[test]
    fn must_not_excludes_rows() {
        let store = mk_store();
        let partial = run_query(&store, mk_query(&[], &[("country", &["DE"])], &[]));
        assert_eq!(partial.matched_rows, 3);
        let amount = partial.numeric.get("amount").unwrap();
        assert_eq!(amount.sum, 20.0 + 40.0 + 50.0);
    }

    #[test]
    fn range_filter_applies_to_numeric_column() {
        let store = mk_store();
        let q = mk_query(
            &[],
            &[],
            &[(
                "amount",
                Some(NumericBound { value: 20.0, comparison: Comparison::Gte }),
                Some(NumericBound { value: 40.0, comparison: Comparison::Lt }),
            )],
        );
        let partial = run_query(&store, q);
        assert_eq!(partial.matched_rows, 2);
        assert_eq!(partial.numeric.get("amount").unwrap().sum, 50.0);
    }

    #[test]
    fn empty_match_reports_zero_count() {
        let store = mk_store();
        let partial = run_query(&store, mk_query(&[("country", &["US"])], &[], &[]));
        assert_eq!(partial.matched_rows, 0);
        assert_eq!(partial.numeric.get("amount").unwrap().count, 0);
    }

    fn mk_store_with_nulls() -> ColumnStore {
        // Column order after BTreeMap sort: amount, country, user_id.
        let cfg = mk_cfg();
        let mut store = ColumnStore::new(&cfg);
        let rows: &[(Option<f64>, Option<&str>, i64)] = &[
            (Some(10.0), Some("DE"), 1),
            (Some(20.0), Some("PL"), 2),
            (None,       Some("FR"), 3), // null amount
            (Some(40.0), None,       4), // null country
            (Some(50.0), Some("PL"), 5),
        ];
        for (a, c, u) in rows {
            store.push_row(&[
                a.map_or(RawValue::Null, RawValue::Float),
                c.map_or(RawValue::Null, RawValue::Str),
                RawValue::Integer(*u),
            ]).unwrap();
        }
        store
    }

    #[test]
    fn null_rows_excluded_from_range_filter() {
        let store = mk_store_with_nulls();
        // amount >= 0 would match the dummy 0.0 for the null row if not explicitly excluded
        let q = mk_query(
            &[],
            &[],
            &[("amount", Some(NumericBound { value: 0.0, comparison: Comparison::Gte }), None)],
        );
        let partial = run_query(&store, q);
        // Rows 0,1,3,4 match (amounts 10,20,40,50); row 2 is null → excluded
        assert_eq!(partial.matched_rows, 4);
        assert_eq!(partial.numeric.get("amount").unwrap().sum, 120.0);
    }

    #[test]
    fn null_rows_excluded_from_aggregation() {
        let store = mk_store_with_nulls();
        // No filter — all 5 rows match. Row 2 has no amount value so it contributes
        // to matched_rows but not to amount stats (count=4, not 5).
        let partial = run_query(&store, mk_query(&[], &[], &[]));
        assert_eq!(partial.matched_rows, 5);
        let amount = partial.numeric.get("amount").unwrap();
        assert_eq!(amount.count, 4);
        assert_eq!(amount.sum, 120.0);
    }

    #[test]
    fn null_country_passes_must_not_filter() {
        let store = mk_store_with_nulls();
        // must_not [country = "FR"] — row 3 (null country) has no value, certainly not "FR"
        let partial = run_query(&store, mk_query(&[], &[("country", &["FR"])], &[]));
        // Row 2 (FR) excluded; row 3 (null country) survives
        assert_eq!(partial.matched_rows, 4);
    }

    #[test]
    fn null_country_excluded_from_must_filter() {
        let store = mk_store_with_nulls();
        // must [country = "PL", "FR", "DE"] — row 3 (null country) has no value → excluded
        let partial = run_query(&store, mk_query(&[("country", &["PL", "FR", "DE"])], &[], &[]));
        assert_eq!(partial.matched_rows, 4);
    }
}
