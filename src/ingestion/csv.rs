use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use flate2::read::GzDecoder;

use crate::config::{AppConfig, SourceConfig};
use crate::error::{AppError, AppResult};
use crate::ingestion::Ingestor;
use crate::resource;
use crate::storage::{ColumnStore, RawValue};

/// Streams a CSV file (optionally gzipped) into [`ColumnStore`].
pub struct CsvIngestor {
    cfg: AppConfig,
}

impl CsvIngestor {
    pub fn new(cfg: AppConfig) -> Self {
        Self { cfg }
    }
}

impl Ingestor for CsvIngestor {
    fn ingest(&self, store: &mut ColumnStore) -> AppResult<()> {
        let (path, delimiter, gzip) = match &self.cfg.source {
            SourceConfig::Csv { path, delimiter, gzip }
                => (path.as_path(), *delimiter, *gzip),
            _ => unreachable!("CsvIngestor dispatched on non-csv source"),
        };

        let reader = open_reader(path, gzip)?;
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(resolve_delimiter(delimiter)?)
            .has_headers(true)
            .from_reader(reader);

        let source_indices = map_headers(&mut csv_reader, store.column_names())?;
        let n_cols = source_indices.len();

        let mut record = csv::StringRecord::new();
        let limit = self.cfg.engine.memory_limit;
        let check_every = self.cfg.engine.chunk_size_rows as u64;
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
        Ok(())
    }
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
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::PathBuf;

    fn cfg_for(path: PathBuf, gzip: bool) -> AppConfig {
        let mut searchable = BTreeMap::new();
        searchable.insert("user_id".into(), SearchableColumn { column_type: ColumnType::Integer });
        searchable.insert("amount".into(), SearchableColumn { column_type: ColumnType::Float });
        searchable.insert("country".into(), SearchableColumn { column_type: ColumnType::String });
        searchable.insert(
            "event_type".into(),
            SearchableColumn { column_type: ColumnType::String },
        );
        searchable.insert(
            "occurred_at".into(),
            SearchableColumn { column_type: ColumnType::DateTime },
        );
        AppConfig {
            source: SourceConfig::Csv { path, delimiter: None, gzip },
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

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data").join(name)
    }

    #[test]
    fn ingests_plain_csv_fixture() {
        let cfg = cfg_for(fixture_path("events.csv"), false);
        let mut store = ColumnStore::new(&cfg);
        CsvIngestor::new(cfg).ingest(&mut store).unwrap();
        assert_eq!(store.row_count(), 100);
        assert!(matches!(store.column("country"), Some(Column::String(_))));
        assert!(matches!(store.column("occurred_at"), Some(Column::DateTime(_))));
    }

    #[test]
    fn ingests_gzipped_csv_fixture() {
        let cfg = cfg_for(fixture_path("events.csv.gz"), true);
        let mut store = ColumnStore::new(&cfg);
        CsvIngestor::new(cfg).ingest(&mut store).unwrap();
        assert_eq!(store.row_count(), 100);
    }

    #[test]
    fn errors_on_missing_column() {
        let tmp = std::env::temp_dir().join("stats_cruncher_csv_missing.csv");
        {
            let mut f = File::create(&tmp).unwrap();
            writeln!(f, "user_id,amount,country,event_type").unwrap();
            writeln!(f, "1,1.0,DE,purchase").unwrap();
        }
        let cfg = cfg_for(tmp.clone(), false);
        let mut store = ColumnStore::new(&cfg);
        let err = CsvIngestor::new(cfg).ingest(&mut store).unwrap_err();
        assert!(err.to_string().contains("occurred_at"));
        std::fs::remove_file(tmp).ok();
    }
}
