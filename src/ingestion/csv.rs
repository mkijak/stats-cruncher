use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::SystemTime;

use flate2::read::GzDecoder;

use crate::config::{AppConfig, SourceConfig};
use crate::error::{AppError, AppResult};
use crate::ingestion::{self, Ingestor};
use crate::resource;
use crate::storage::{ColumnStore, RawValue};

/// Streams a CSV file (optionally gzipped) into [`ColumnStore`].
pub struct CsvIngestor {
    cfg: AppConfig,
    last_loaded: Option<SystemTime>,
}

impl CsvIngestor {
    pub fn new(cfg: AppConfig) -> Self {
        Self { cfg, last_loaded: None }
    }
}

impl Ingestor for CsvIngestor {
    fn reload(&mut self) -> AppResult<Option<ColumnStore>> {
        let path = self.cfg.source.file_path()
            .expect("CsvIngestor requires a file-backed source");
        let before = mtime(path)?;
        if Some(before) == self.last_loaded {
            return Ok(None);
        }

        let store = ingest_into_store(&self.cfg, path)?;

        // Re-stat: if the file was rewritten while streamed, the read may
        // be torn — drop the result and let the next tick retry.
        let after = mtime(path)?;
        if after != before {
            tracing::warn!(
                path = %path.display(),
                "source file changed during read; discarding partial reload"
            );
            return Ok(None);
        }

        self.last_loaded = Some(after);
        Ok(Some(ingestion::finalize(store, &self.cfg)?))
    }
}

fn ingest_into_store(cfg: &AppConfig, path: &Path) -> AppResult<ColumnStore> {
    let (delimiter, gzip) = match &cfg.source {
        SourceConfig::Csv { delimiter, gzip, .. } => (*delimiter, *gzip),
        _ => unreachable!("CsvIngestor dispatched on non-csv source"),
    };

    let mut store = ColumnStore::new(cfg);
    let reader = open_reader(path, gzip)?;
    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(resolve_delimiter(delimiter)?)
        .has_headers(true)
        .from_reader(reader);

    let source_indices = map_headers(&mut csv_reader, store.column_names())?;
    let n_cols = source_indices.len();

    let mut record = csv::StringRecord::new();
    let limit = cfg.engine.memory_limit;
    let check_every = cfg.engine.chunk_size_rows as u64;
    let mut ingested: u64 = 0;

    loop {
        let more = csv_reader.read_record(&mut record).map_err(|e| {
            AppError::Ingestion(format!(
                "reading CSV row {} from {}: {e}",
                ingested + 1,
                path.display()
            ))
        })?;
        if !more {
            break;
        }
        let mut row_buf: Vec<RawValue<'_>> = Vec::with_capacity(n_cols);
        for &idx in &source_indices {
            let cell = record.get(idx).ok_or_else(|| {
                AppError::Ingestion(format!(
                    "CSV row {} in {} is missing column index {idx}",
                    ingested + 1,
                    path.display()
                ))
            })?;
            row_buf.push(RawValue::Str(cell));
        }
        store.push_row(&row_buf)?;
        ingested += 1;
        if ingested % check_every == 0 {
            resource::check_memory(limit)?;
        }
    }
    Ok(store)
}

fn mtime(path: &Path) -> AppResult<SystemTime> {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .map_err(|e| AppError::Ingestion(format!(
            "cannot stat source file {}: {e}",
            path.display()
        )))
}

fn open_reader(path: &Path, gzip: bool) -> AppResult<Box<dyn Read>> {
    let file = File::open(path).map_err(|e| {
        AppError::Ingestion(format!("cannot open CSV source {}: {e}", path.display()))
    })?;
    let buffered = BufReader::new(file);
    if gzip {
        Ok(Box::new(BufReader::new(GzDecoder::new(buffered))))
    } else {
        Ok(Box::new(buffered))
    }
}

fn resolve_delimiter(delimiter: Option<char>) -> AppResult<u8> {
    let Some(c) = delimiter else {
        return Ok(b',');
    };
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        Err(AppError::Config(format!(
            "CSV delimiter must be a single ASCII character, got {c:?}"
        )))
    }
}

fn map_headers<R: Read>(
    reader: &mut csv::Reader<R>,
    expected: &[String],
) -> AppResult<Vec<usize>> {
    let headers = reader
        .headers()
        .map_err(|e| AppError::Ingestion(format!("reading CSV header: {e}")))?;
    let mut indices = Vec::with_capacity(expected.len());
    for name in expected {
        let pos = headers.iter().position(|h| h == name).ok_or_else(|| {
            AppError::Ingestion(format!(
                "CSV source is missing declared column {name:?}"
            ))
        })?;
        indices.push(pos);
    }
    Ok(indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiConfig, ColumnType, EngineConfig, SearchableColumn};
    use crate::storage::Column;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::PathBuf;

    const CSV_HEADER: &str = "user_id,amount,country,event_type,occurred_at";
    const CSV_ROWS: &str = "\
1001,99.50,DE,purchase,2026-01-01T00:00:00Z
1002,149.00,PL,purchase,2026-01-02T00:00:00Z
1003,49.99,FR,refund,2026-01-03T00:00:00Z";

    fn cfg_for(path: PathBuf, gzip: bool) -> AppConfig {
        let mut searchable = BTreeMap::new();
        searchable.insert("user_id".into(), SearchableColumn { column_type: ColumnType::Integer, hidden: false });
        searchable.insert("amount".into(), SearchableColumn { column_type: ColumnType::Float, hidden: false });
        searchable.insert("country".into(), SearchableColumn { column_type: ColumnType::String, hidden: false });
        searchable.insert("event_type".into(), SearchableColumn { column_type: ColumnType::String, hidden: false });
        searchable.insert("occurred_at".into(), SearchableColumn { column_type: ColumnType::DateTime, hidden: false });
        AppConfig {
            source: SourceConfig::Csv { path, delimiter: None, gzip },
            engine: EngineConfig { memory_limit: u64::MAX, chunk_size_rows: 1024, worker_threads: 1, string_value_counts: false, reload_interval_mins: 0 },
            api: ApiConfig { bind: "0.0.0.0:0".into() },
            searchable,
            partition_column: None,
        }
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("stats_cruncher_{name}_{}", std::process::id()))
    }

    #[test]
    fn ingests_plain_csv() {
        let tmp = tmp("plain.csv");
        {
            let mut f = File::create(&tmp).unwrap();
            writeln!(f, "{CSV_HEADER}").unwrap();
            write!(f, "{CSV_ROWS}").unwrap();
        }
        let cfg = cfg_for(tmp.clone(), false);
        let store = CsvIngestor::new(cfg).reload().unwrap().unwrap();
        assert_eq!(store.row_count(), 3);
        assert!(matches!(store.column("country"), Some(Column::String(_))));
        assert!(matches!(store.column("occurred_at"), Some(Column::DateTime(_))));
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn ingests_gzipped_csv() {
        let tmp = tmp("gzip.csv.gz");
        {
            let f = File::create(&tmp).unwrap();
            let mut gz = GzEncoder::new(f, Compression::default());
            writeln!(gz, "{CSV_HEADER}").unwrap();
            write!(gz, "{CSV_ROWS}").unwrap();
        }
        let cfg = cfg_for(tmp.clone(), true);
        let store = CsvIngestor::new(cfg).reload().unwrap().unwrap();
        assert_eq!(store.row_count(), 3);
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn empty_cells_treated_as_null() {
        let tmp = tmp("nulls.csv");
        {
            let mut f = File::create(&tmp).unwrap();
            writeln!(f, "{CSV_HEADER}").unwrap();
            writeln!(f, "1001,99.50,DE,purchase,2026-01-01T00:00:00Z").unwrap();
            writeln!(f, "1002,,  ,purchase,2026-01-02T00:00:00Z").unwrap();
        }
        let cfg = cfg_for(tmp.clone(), false);
        let store = CsvIngestor::new(cfg).reload().unwrap().unwrap();
        assert_eq!(store.row_count(), 2);
        assert!(store.null_rows_for("amount").map_or(false, |n| n.contains(1)));
        assert!(store.null_rows_for("country").map_or(false, |n| n.contains(1)));
        assert!(store.null_rows_for("amount").map_or(false, |n| !n.contains(0)));
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn errors_on_missing_column() {
        let tmp = tmp("missing.csv");
        {
            let mut f = File::create(&tmp).unwrap();
            writeln!(f, "user_id,amount,country,event_type").unwrap();
            writeln!(f, "1,1.0,DE,purchase").unwrap();
        }
        let cfg = cfg_for(tmp.clone(), false);
        let err = match CsvIngestor::new(cfg).reload() {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.to_string().contains("occurred_at"));
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn second_reload_returns_none_when_unchanged() {
        let tmp = tmp("repeat.csv");
        {
            let mut f = File::create(&tmp).unwrap();
            writeln!(f, "{CSV_HEADER}").unwrap();
            write!(f, "{CSV_ROWS}").unwrap();
        }
        let cfg = cfg_for(tmp.clone(), false);
        let mut ing = CsvIngestor::new(cfg);
        assert!(ing.reload().unwrap().is_some());
        assert!(ing.reload().unwrap().is_none());
        std::fs::remove_file(tmp).ok();
    }
}
