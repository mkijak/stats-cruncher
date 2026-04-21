mod column;
mod dictionary;

pub use column::{Column, ColumnName, StringColumn};
pub use dictionary::StringDictionary;

use std::collections::HashMap;

use chrono::DateTime;

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
    row_count: u32,
    chunk_size: usize,
}

impl ColumnStore {
    pub fn new(cfg: &AppConfig) -> Self {
        let mut names = Vec::with_capacity(cfg.searchable.len());
        let mut columns = Vec::with_capacity(cfg.searchable.len());
        let mut lookup = HashMap::with_capacity(cfg.searchable.len());
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
        }
        Self {
            names,
            columns,
            lookup,
            row_count: 0,
            chunk_size: cfg.engine.chunk_size_rows.max(1),
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
            push_value(&mut self.columns[idx], &self.names[idx], value, row_id)?;
        }
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| AppError::Ingestion("row count exceeds u32::MAX".into()))?;
        Ok(())
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

fn push_value(
    column: &mut Column,
    name: &str,
    value: &RawValue<'_>,
    row: u32,
) -> AppResult<()> {
    if matches!(value, RawValue::Null) {
        return Err(AppError::Ingestion(format!(
            "column {name:?}: NULL values are not supported",
        )));
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
    Ok(())
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
        RawValue::Str(s) if s.trim().is_empty() => Err(AppError::Ingestion(format!(
            "column {name:?}: empty string values are not supported"
        ))),
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
            searchable.insert((*n).to_string(), SearchableColumn { column_type: *t });
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
            },
            api: ApiConfig { bind: "0.0.0.0:0".to_string() },
            searchable,
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
    fn push_row_rejects_null() {
        let cfg = mk_cfg(&[("a", ColumnType::Integer)]);
        let mut store = ColumnStore::new(&cfg);
        let err = store.push_row(&[RawValue::Null]).unwrap_err();
        assert!(err.to_string().contains("NULL"));
    }
}
