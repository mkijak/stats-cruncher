mod column;
mod dictionary;

pub use column::{Column, ColumnName, StringColumn};
pub use dictionary::StringDictionary;

use std::collections::{BTreeMap, HashMap};

use chrono::DateTime;
use roaring::RoaringBitmap;

use crate::config::{AppConfig, ColumnType};
use crate::error::{AppError, AppResult};

/// The in-memory columnar store. Owns the raw `Vec<T>` columns plus the
/// dictionary-encoded string columns and their bitmap indices.
///
/// Columns are kept in a fixed order matching the config's `[searchable.*]`
/// declaration order. Ingestion adapters push rows positionally; a name -> index
/// lookup exists for query-time column resolution.
pub struct ColumnStore {
    names: Vec<ColumnName>,
    columns: Vec<Column>,
    lookup: HashMap<String, usize>,
    hidden: Vec<bool>,
    row_count: u32,
    chunk_size: usize,
    zone_stats: Option<(String, Vec<(f64, f64)>)>,
    null_rows: BTreeMap<String, RoaringBitmap>,
}

impl ColumnStore {
    pub fn new(cfg: &AppConfig) -> Self {
        let mut names = Vec::with_capacity(cfg.searchable.len());
        let mut columns = Vec::with_capacity(cfg.searchable.len());
        let mut lookup = HashMap::with_capacity(cfg.searchable.len());
        let mut hidden = Vec::with_capacity(cfg.searchable.len());
        for (idx, (name, col)) in cfg.searchable.iter().enumerate() {
            let column = match col.column_type {
                ColumnType::Integer => Column::Integer(Vec::new()),
                ColumnType::Float => Column::Float(Vec::new()),
                ColumnType::String => Column::String(StringColumn::new()),
                ColumnType::DateTime => Column::DateTime(Vec::new()),
            };
            names.push(name.clone());
            columns.push(column);
            lookup.insert(name.clone(), idx);
            hidden.push(col.hidden);
        }
        Self {
            names,
            columns,
            lookup,
            hidden,
            row_count: 0,
            chunk_size: cfg.engine.chunk_size_rows.max(1),
            zone_stats: None,
            null_rows: BTreeMap::new(),
        }
    }

    /// Column names in the fixed schema order. Adapters use this to build the
    /// source-to-store index mapping
    pub fn column_names(&self) -> &[ColumnName] {
        &self.names
    }

    /// Logical type for each column, aligned with [`Self::column_names`].
    pub fn column_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.columns.iter().map(Column::type_name)
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        let idx = *self.lookup.get(name)?;
        Some(&self.columns[idx])
    }

    pub fn row_count(&self) -> usize {
        self.row_count as usize
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Number of chunks the current data set breaks into — the unit of work
    /// the Coordinator hands out to workers.
    pub fn chunk_count(&self) -> usize {
        self.row_count().div_ceil(self.chunk_size)
    }

    /// Per-chunk `(min, max)` for the partition column, if one was configured and
    /// `sort_by` has been called. Indexed by chunk ordinal (chunk 0 = rows 0..chunk_size).
    pub fn zone_stats(&self) -> Option<(&str, &[(f64, f64)])> {
        self.zone_stats.as_ref().map(|(col, stats)| (col.as_str(), stats.as_slice()))
    }

    /// Null row bitmap for a column, if any null values were ingested.
    pub fn null_rows_for(&self, name: &str) -> Option<&RoaringBitmap> {
        self.null_rows.get(name)
    }

    pub fn is_hidden(&self, name: &str) -> bool {
        self.lookup.get(name).map_or(false, |&idx| self.hidden[idx])
    }

    pub fn sort_by(&mut self, partition_column: &str) -> AppResult<()> {
        let col_idx = *self.lookup.get(partition_column).ok_or_else(|| {
            AppError::Config(format!(
                "partition_column {partition_column:?} is not listed in [searchable]"
            ))
        })?;

        let n = self.row_count as usize;
        if n == 0 {
            return Ok(());
        }

        let keys: Vec<f64> = match &self.columns[col_idx] {
            Column::DateTime(v) => v.iter().map(|&x| x as f64).collect(),
            Column::Integer(v) => v.iter().map(|&x| x as f64).collect(),
            Column::Float(v) => v.clone(),
            Column::String(_) => {
                return Err(AppError::Config(format!(
                    "partition_column {partition_column:?}: string columns are not supported"
                )))
            }
        };

        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_by(|&a, &b| keys[a].total_cmp(&keys[b]));

        for col in &mut self.columns {
            apply_perm(col, &perm);
        }

        if !self.null_rows.is_empty() {
            let mut old_to_new = vec![0u32; n];
            for (new_idx, &old_idx) in perm.iter().enumerate() {
                old_to_new[old_idx] = new_idx as u32;
            }
            for nulls in self.null_rows.values_mut() {
                *nulls = nulls.iter().map(|r| old_to_new[r as usize]).collect();
            }
        }

        let sorted_keys: Vec<f64> = perm.iter().map(|&i| keys[i]).collect();
        let zones: Vec<(f64, f64)> = sorted_keys
            .chunks(self.chunk_size)
            .map(|c| (*c.first().unwrap(), *c.last().unwrap()))
            .collect();

        self.zone_stats = Some((partition_column.to_owned(), zones));
        Ok(())
    }

    /// Append a single row. `values` must be in schema order (the same order as
    /// [`Self::column_names`]). The store handles type coercion and dictionary mapping
    pub fn push_row(&mut self, values: &[RawValue<'_>]) -> AppResult<()> {
        if values.len() != self.columns.len() {
            return Err(AppError::Ingestion(format!(
                "row has {} values but schema declares {} columns",
                values.len(),
                self.columns.len(),
            )));
        }
        let row_id = self.row_count;
        for (idx, value) in values.iter().enumerate() {
            let has_value = push_value(&mut self.columns[idx], &self.names[idx], value, row_id)?;
            if !has_value {
                self.null_rows.entry(self.names[idx].clone()).or_default().insert(row_id);
            }
        }
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| AppError::Ingestion("row count exceeds u32::MAX".into()))?;
        Ok(())
    }
}

fn apply_perm(col: &mut Column, perm: &[usize]) {
    match col {
        Column::Integer(v) => {
            let sorted: Vec<i64> = perm.iter().map(|&i| v[i]).collect();
            *v = sorted;
        }
        Column::Float(v) => {
            let sorted: Vec<f64> = perm.iter().map(|&i| v[i]).collect();
            *v = sorted;
        }
        Column::DateTime(v) => {
            let sorted: Vec<i64> = perm.iter().map(|&i| v[i]).collect();
            *v = sorted;
        }
        Column::String(sc) => {
            let n = perm.len();
            let dict = &sc.dictionary;

            // u32::MAX is a sentinel meaning "null — no posting for this row"
            let mut row_to_code = vec![u32::MAX; n];
            for code in 0..dict.cardinality() as u32 {
                for row in dict.postings(code).unwrap().iter() {
                    row_to_code[row as usize] = code;
                }
            }

            // Rebuild dictionary in permuted row order, skipping null rows
            let mut new_sc = StringColumn::new();
            for (new_row, &old_row) in perm.iter().enumerate() {
                let code = row_to_code[old_row];
                if code == u32::MAX {
                    continue;
                }
                let value = dict.value_of(code).unwrap();
                new_sc.dictionary.intern(value, new_row as u32);
                new_sc.count += 1;
            }
            *sc = new_sc;
        }
    }
}

/// Untyped value straight out of the source. The store coerces it into the
/// column's native representation (see coerce_ functions supporting all data types)
pub enum RawValue<'a> {
    Integer(i64),
    Float(f64),
    Str(&'a str),
    Null,
}

impl RawValue<'_> {
    fn kind(&self) -> &'static str {
        match self {
            RawValue::Integer(_) => "integer",
            RawValue::Float(_) => "float",
            RawValue::Str(_) => "string",
            RawValue::Null => "null",
        }
    }
}

/// Returns `true` when a real value was stored, `false` when the value is null/missing
/// (a dummy was stored in numeric columns; string columns simply have no posting for this row).
/// The caller records the row index in `null_rows` when `false` is returned.
fn push_value(
    column: &mut Column,
    name: &str,
    value: &RawValue<'_>,
    row: u32,
) -> AppResult<bool> {
    let is_null = matches!(value, RawValue::Null)
        || matches!(value, RawValue::Str(s) if s.trim().is_empty());
    if is_null {
        match column {
            Column::Integer(v) => v.push(0),
            Column::Float(v) => v.push(0.0),
            Column::DateTime(v) => v.push(0),
            Column::String(_) => {} // null rows have no postings entry
        }
        return Ok(false);
    }
    match column {
        Column::Integer(v) => v.push(coerce_integer(name, value)?),
        Column::Float(v) => v.push(coerce_float(name, value)?),
        Column::String(sc) => {
            let s = coerce_str(name, value)?;
            sc.dictionary.intern(s, row);
            sc.count += 1;
        }
        Column::DateTime(v) => v.push(coerce_datetime(name, value)?),
    }
    Ok(true)
}

fn coerce_integer(name: &str, value: &RawValue<'_>) -> AppResult<i64> {
    match value {
        RawValue::Integer(i) => Ok(*i),
        RawValue::Float(f) if f.is_finite() && f.fract() == 0.0 => Ok(*f as i64),
        RawValue::Str(s) => s.trim().parse::<i64>().map_err(|e| {
            AppError::Ingestion(format!(
                "column {name:?}: expected integer, got {s:?}: {e}"
            ))
        }),
        other => Err(AppError::Ingestion(format!(
            "column {name:?}: expected integer, got {}",
            other.kind()
        ))),
    }
}

fn coerce_float(name: &str, value: &RawValue<'_>) -> AppResult<f64> {
    match value {
        RawValue::Float(f) => Ok(*f),
        RawValue::Integer(i) => Ok(*i as f64),
        RawValue::Str(s) => s.trim().parse::<f64>().map_err(|e| {
            AppError::Ingestion(format!(
                "column {name:?}: expected float, got {s:?}: {e}"
            ))
        }),
        other => Err(AppError::Ingestion(format!(
            "column {name:?}: expected float, got {}",
            other.kind()
        ))),
    }
}

fn coerce_str<'a>(name: &str, value: &'a RawValue<'_>) -> AppResult<&'a str> {
    match value {
        RawValue::Str(s) => Ok(s),
        other => Err(AppError::Ingestion(format!(
            "column {name:?}: expected string, got {}",
            other.kind()
        ))),
    }
}

fn coerce_datetime(name: &str, value: &RawValue<'_>) -> AppResult<i64> {
    match value {
        RawValue::Str(s) => DateTime::parse_from_rfc3339(s.trim())
            .map(|dt| dt.timestamp())
            .map_err(|e| {
                AppError::Ingestion(format!(
                    "column {name:?}: expected RFC3339 date-time, got {s:?}: {e}"
                ))
            }),
        RawValue::Integer(i) => Ok(*i),
        other => Err(AppError::Ingestion(format!(
            "column {name:?}: expected date-time, got {}",
            other.kind()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiConfig, EngineConfig, SearchableColumn, SourceConfig};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn mk_cfg(cols: &[(&str, ColumnType)]) -> AppConfig {
        let mut searchable = BTreeMap::new();
        for (n, t) in cols {
            searchable.insert((*n).to_string(), SearchableColumn { column_type: *t, hidden: false });
        }
        AppConfig {
            source: SourceConfig::Csv {
                path: PathBuf::from("/dev/null"),
                delimiter: None,
                gzip: false,
            },
            engine: EngineConfig {
                memory_limit: u64::MAX,
                chunk_size_rows: 1024,
                worker_threads: 1,
                string_value_counts: false,
            },
            api: ApiConfig { bind: "0.0.0.0:0".to_string() },
            searchable,
            partition_column: None,
        }
    }

    #[test]
    fn push_row_coerces_csv_strings() {
        let cfg = mk_cfg(&[
            ("user_id", ColumnType::Integer),
            ("amount", ColumnType::Float),
            ("country", ColumnType::String),
            ("occurred_at", ColumnType::DateTime),
        ]);
        let mut store = ColumnStore::new(&cfg);
        // BTreeMap sorts columns alphabetically: amount, country, occurred_at, user_id.
        store
            .push_row(&[
                RawValue::Str("12.5"),
                RawValue::Str("DE"),
                RawValue::Str("2026-04-19T00:00:00Z"),
                RawValue::Str("42"),
            ])
            .unwrap();
        store
            .push_row(&[
                RawValue::Str("7"),
                RawValue::Str("DE"),
                RawValue::Str("2026-04-20T12:34:56Z"),
                RawValue::Str("43"),
            ])
            .unwrap();
        assert_eq!(store.row_count(), 2);

        let Column::Float(amt) = store.column("amount").unwrap() else {
            panic!("wrong column kind")
        };
        assert_eq!(amt, &vec![12.5, 7.0]);

        let Column::Integer(uid) = store.column("user_id").unwrap() else {
            panic!("wrong column kind")
        };
        assert_eq!(uid, &vec![42, 43]);

        let Column::DateTime(ts) = store.column("occurred_at").unwrap() else {
            panic!("wrong column kind")
        };
        assert_eq!(ts.len(), 2);
        assert!(ts[1] > ts[0]);

        let Column::String(s) = store.column("country").unwrap() else {
            panic!("wrong column kind")
        };
        assert_eq!(s.dictionary.cardinality(), 1);
        let de_code = s.dictionary.code_of("DE").unwrap();
        let postings = s.dictionary.postings(de_code).unwrap();
        assert_eq!(postings.len(), 2);
    }

    #[test]
    fn push_row_rejects_bad_integer() {
        let cfg = mk_cfg(&[("user_id", ColumnType::Integer)]);
        let mut store = ColumnStore::new(&cfg);
        let err = store.push_row(&[RawValue::Str("not-a-number")]).unwrap_err();
        assert!(err.to_string().contains("user_id"));
    }

    #[test]
    fn push_row_rejects_bad_datetime() {
        let cfg = mk_cfg(&[("occurred_at", ColumnType::DateTime)]);
        let mut store = ColumnStore::new(&cfg);
        let err = store.push_row(&[RawValue::Str("2026-04-19")]).unwrap_err();
        assert!(err.to_string().contains("RFC3339"));
    }

    #[test]
    fn push_row_rejects_wrong_arity() {
        let cfg = mk_cfg(&[("a", ColumnType::Integer), ("b", ColumnType::Integer)]);
        let mut store = ColumnStore::new(&cfg);
        let err = store.push_row(&[RawValue::Integer(1)]).unwrap_err();
        assert!(err.to_string().contains("1 values"));
    }

    #[test]
    fn push_row_accepts_null_and_empty() {
        let cfg = mk_cfg(&[
            ("a", ColumnType::Integer),
            ("b", ColumnType::String),
            ("c", ColumnType::Float),
        ]);
        let mut store = ColumnStore::new(&cfg);
        // Row 0: all null / empty
        store.push_row(&[RawValue::Null, RawValue::Str(""), RawValue::Str("  ")]).unwrap();
        assert_eq!(store.row_count(), 1);
        assert!(store.null_rows_for("a").map_or(false, |n| n.contains(0)));
        assert!(store.null_rows_for("b").map_or(false, |n| n.contains(0)));
        assert!(store.null_rows_for("c").map_or(false, |n| n.contains(0)));
        // Row 1: real values — not in null bitmaps
        store.push_row(&[RawValue::Integer(42), RawValue::Str("DE"), RawValue::Float(1.5)]).unwrap();
        assert!(!store.null_rows_for("a").map_or(false, |n| n.contains(1)));
    }

    #[test]
    fn null_rows_excluded_from_aggregation() {
        // BTreeMap order: amount, flag
        let cfg = mk_cfg(&[("amount", ColumnType::Float), ("flag", ColumnType::Integer)]);
        let mut store = ColumnStore::new(&cfg);
        store.push_row(&[RawValue::Float(10.0), RawValue::Integer(1)]).unwrap();
        store.push_row(&[RawValue::Null, RawValue::Integer(2)]).unwrap(); // null amount
        store.push_row(&[RawValue::Float(30.0), RawValue::Integer(3)]).unwrap();
        assert_eq!(store.row_count(), 3);
        // The null bitmap for "amount" must contain only row 1
        let nulls = store.null_rows_for("amount").unwrap();
        assert!(nulls.contains(1));
        assert!(!nulls.contains(0));
        assert!(!nulls.contains(2));
        // "flag" has no nulls
        assert!(store.null_rows_for("flag").is_none());
    }
}
