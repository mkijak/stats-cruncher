use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};

use crate::config::{AppConfig, SourceConfig};
use crate::error::{AppError, AppResult};
use crate::ingestion::Ingestor;
use crate::resource;
use crate::storage::{ColumnStore, RawValue};

/// Streams rows out of a single-file SQLite database into [`ColumnStore`].
pub struct SqliteIngestor {
    cfg: AppConfig,
}

impl SqliteIngestor {
    pub fn new(cfg: AppConfig) -> Self {
        Self { cfg }
    }
}

impl Ingestor for SqliteIngestor {
    fn ingest(&self, store: &mut ColumnStore) -> AppResult<()> {
        let (path, table) = match &self.cfg.source {
            SourceConfig::Sqlite { path, table } => (path.as_path(), table.as_str()),
            _ => unreachable!("SqliteIngestor dispatched on non-sqlite source"),
        };

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| {
                AppError::Ingestion(format!("cannot open SQLite source {}: {e}", path.display()))
            })?;

        let columns: Vec<String> = store.column_names().to_vec();
        let select_cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let sql = format!(
            "SELECT {} FROM {}",
            select_cols.join(", "),
            quote_ident(table)
        );

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            AppError::Ingestion(format!("preparing SQLite SELECT {sql:?}: {e}"))
        })?;
        let mut rows = stmt.query([]).map_err(|e| {
            AppError::Ingestion(format!("executing SQLite SELECT: {e}"))
        })?;

        let limit = self.cfg.engine.memory_limit;
        let check_every = self.cfg.engine.chunk_size_rows as u64;
        let mut ingested: u64 = 0;

        while let Some(row) = rows.next().map_err(|e| {
            AppError::Ingestion(format!("iterating SQLite row {}: {e}", ingested + 1))
        })? {
            let mut row_buf: Vec<RawValue<'_>> = Vec::with_capacity(columns.len());
            for (idx, name) in columns.iter().enumerate() {
                let raw = row.get_ref(idx).map_err(|e| {
                    AppError::Ingestion(format!(
                        "reading SQLite column {name:?} at row {}: {e}",
                        ingested + 1
                    ))
                })?;
                row_buf.push(map_value(name, raw)?);
            }
            store.push_row(&row_buf)?;
            ingested += 1;
            if ingested % check_every == 0 {
                resource::check_memory(limit)?;
            }
        }
        Ok(())
    }
}

fn map_value<'a>(column: &str, value: ValueRef<'a>) -> AppResult<RawValue<'a>> {
    match value {
        ValueRef::Null => Ok(RawValue::Null),
        ValueRef::Integer(i) => Ok(RawValue::Integer(i)),
        ValueRef::Real(f) => Ok(RawValue::Float(f)),
        ValueRef::Text(bytes) => std::str::from_utf8(bytes)
            .map(RawValue::Str)
            .map_err(|e| {
                AppError::Ingestion(format!(
                    "column {column:?}: non-UTF-8 text value: {e}"
                ))
            }),
        ValueRef::Blob(_) => Err(AppError::Ingestion(format!(
            "column {column:?}: BLOB values are not supported"
        ))),
    }
}

/// Quote a SQL identifier using SQLite's double-quote convention and escape any
/// embedded quotes. Keeps the ingestor safe against hostile column/table names
/// in the config.
fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiConfig, ColumnType, EngineConfig, SearchableColumn};
    use crate::storage::Column;
    use rusqlite::Connection;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn make_db(path: &PathBuf) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch("
            CREATE TABLE events (
                user_id     INTEGER NOT NULL,
                amount      REAL,
                country     TEXT,
                event_type  TEXT    NOT NULL,
                occurred_at TEXT    NOT NULL
            );
            INSERT INTO events VALUES (1001, 99.50,  'DE', 'purchase', '2026-01-01T00:00:00Z');
            INSERT INTO events VALUES (1002, NULL,   'PL', 'purchase', '2026-01-02T00:00:00Z');
            INSERT INTO events VALUES (1003, 49.99,  NULL, 'refund',   '2026-01-03T00:00:00Z');
        ").unwrap();
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("stats_cruncher_{name}_{}", std::process::id()))
    }

    fn cfg_for(path: PathBuf, table: &str) -> AppConfig {
        let mut searchable = BTreeMap::new();
        searchable.insert("user_id".into(), SearchableColumn { column_type: ColumnType::Integer, hidden: false });
        searchable.insert("amount".into(), SearchableColumn { column_type: ColumnType::Float, hidden: false });
        searchable.insert("country".into(), SearchableColumn { column_type: ColumnType::String, hidden: false });
        searchable.insert("event_type".into(), SearchableColumn { column_type: ColumnType::String, hidden: false });
        searchable.insert("occurred_at".into(), SearchableColumn { column_type: ColumnType::DateTime, hidden: false });
        AppConfig {
            source: SourceConfig::Sqlite { path, table: table.into() },
            engine: EngineConfig { memory_limit: u64::MAX, chunk_size_rows: 1024, worker_threads: 1, string_value_counts: false },
            api: ApiConfig { bind: "0.0.0.0:0".into() },
            searchable,
            partition_column: None,
        }
    }

    #[test]
    fn ingests_sqlite() {
        let tmp = tmp("events.db");
        make_db(&tmp);
        let cfg = cfg_for(tmp.clone(), "events");
        let mut store = ColumnStore::new(&cfg);
        SqliteIngestor::new(cfg).ingest(&mut store).unwrap();
        assert_eq!(store.row_count(), 3);
        // Row 1 has NULL amount, row 2 has NULL country
        assert!(store.null_rows_for("amount").map_or(false, |n| n.contains(1)));
        assert!(store.null_rows_for("country").map_or(false, |n| n.contains(2)));
        assert!(store.null_rows_for("amount").map_or(false, |n| !n.contains(0)));
        assert!(matches!(store.column("country"), Some(Column::String(_))));
        let Column::DateTime(ts) = store.column("occurred_at").unwrap() else {
            panic!("wrong column kind")
        };
        assert_eq!(ts.len(), 3);
        let Column::Integer(uid) = store.column("user_id").unwrap() else {
            panic!("wrong column kind")
        };
        assert!(uid.iter().all(|&v| (1000..=1030).contains(&v)));
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn errors_on_missing_table() {
        let tmp = tmp("empty.db");
        Connection::open(&tmp).unwrap(); // create empty db
        let cfg = cfg_for(tmp.clone(), "does_not_exist");
        let mut store = ColumnStore::new(&cfg);
        let err = SqliteIngestor::new(cfg).ingest(&mut store).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("does_not_exist"));
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
    }
}
