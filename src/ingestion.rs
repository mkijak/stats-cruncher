mod csv;
mod sqlite;

pub use csv::CsvIngestor;
pub use sqlite::SqliteIngestor;

use crate::config::{AppConfig, SourceConfig};
use crate::error::AppResult;
use crate::storage::ColumnStore;

/// A data source adapter. Adapters stream rows out of their source and
/// push them into the store; they never hold the full dataset themselves.
///
/// Implementations should call [`crate::resource::check_memory`] at chunk
/// boundaries (every ~N rows) to fail fast if ingestion is driving RSS past
/// the configured limit.
pub trait Ingestor {
    fn ingest(&self, store: &mut ColumnStore) -> AppResult<()>;
}

/// Build the right adapter for the configured source.
pub fn select(cfg: &AppConfig) -> Box<dyn Ingestor> {
    match &cfg.source {
        SourceConfig::Csv { .. } => Box::new(CsvIngestor::new(cfg.clone())),
        SourceConfig::Sqlite { .. } => Box::new(SqliteIngestor::new(cfg.clone())),
    }
}

/// Top-level ingestion entry point: picks an adapter, runs it, returns the
/// populated store. Spawned on a blocking Tokio task because parsing is
/// CPU-heavy and we don't want to starve the async runtime.
pub async fn run(cfg: &AppConfig) -> AppResult<ColumnStore> {
    let cfg = cfg.clone();
    tokio::task::spawn_blocking(move || {
        let mut store = ColumnStore::new(&cfg);
        select(&cfg).ingest(&mut store)?;
        if let Some(col) = &cfg.partition_column {
            tracing::info!(column = col, rows = store.row_count(), "sorting rows by partition column");
            store.sort_by(col)?;
        }
        Ok(store)
    })
    .await
    .expect("ingestion task panicked")
}
