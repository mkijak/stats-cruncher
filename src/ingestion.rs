mod csv;
mod sqlite;

pub use csv::CsvIngestor;
pub use sqlite::SqliteIngestor;

use crate::config::{AppConfig, SourceConfig};
use crate::error::AppResult;
use crate::storage::ColumnStore;

/// Implementations should call [`crate::resource::check_memory`] at chunk
/// boundaries (every ~N rows) to fail fast if ingestion is driving RSS past
/// the configured limit.
pub trait Ingestor: Send {
    /// Attempt to produce a fresh store. Returns `None` when there is nothing
    /// to do this tick — either the source is unchanged since the last
    /// successful load, or a transient condition (e.g. concurrent file write)
    /// made the read unsafe and the next tick should retry.
    fn reload(&mut self) -> AppResult<Option<ColumnStore>>;
}

/// Build the right adapter for the configured source.
pub fn select(cfg: &AppConfig) -> Box<dyn Ingestor> {
    match &cfg.source {
        SourceConfig::Csv { .. } => Box::new(CsvIngestor::new(cfg.clone())),
        SourceConfig::Sqlite { .. } => Box::new(SqliteIngestor::new(cfg.clone())),
    }
}

/// Apply post-ingest processing common to all sources (currently: partition sort).
pub(crate) fn finalize(mut store: ColumnStore, cfg: &AppConfig) -> AppResult<ColumnStore> {
    if let Some(col) = &cfg.partition_column {
        tracing::info!(column = col, rows = store.row_count(), "sorting rows by partition column");
        store.sort_by(col)?;
    }
    Ok(store)
}
