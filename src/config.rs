mod schema;

pub use schema::{AppConfig, ApiConfig, ColumnType, EngineConfig, SearchableColumn, SourceConfig};

use std::path::Path;

use crate::error::{AppError, AppResult};

pub fn load(path: &Path) -> AppResult<AppConfig> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        AppError::Config(format!("cannot read config {}: {e}", path.display()))
    })?;
    parse(&raw)
}

pub fn parse(raw: &str) -> AppResult<AppConfig> {
    toml::from_str(raw).map_err(|e| AppError::Config(format!("parsing config: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config() {
        let cfg = parse(r#"
            partition_column = "occurred_at"

            [source]
            type = "csv"
            path = "/data/events.csv.gz"
            gzip = true

            [engine]
            memory_limit = "8GiB"
            chunk_size_rows = 65_536
            worker_threads = 4

            [api]
            bind = "0.0.0.0:8080"

            [searchable.user_id]
            type = "integer"
            hidden = true

            [searchable.amount]
            type = "float"

            [searchable.country]
            type = "string"

            [searchable.occurred_at]
            type = "date-time"
        "#).unwrap();
        assert_eq!(cfg.engine.memory_limit, 8 * 1024u64.pow(3));
        assert_eq!(cfg.engine.chunk_size_rows, 65_536);
        assert_eq!(cfg.engine.worker_threads, 4);
        assert_eq!(cfg.api.bind, "0.0.0.0:8080");
        assert!(matches!(cfg.source, SourceConfig::Csv { gzip: true, .. }));
        assert!(cfg.searchable["user_id"].hidden);
        assert!(!cfg.searchable["amount"].hidden);
        assert!(matches!(cfg.searchable["occurred_at"].column_type, ColumnType::DateTime));
        assert_eq!(cfg.partition_column.as_deref(), Some("occurred_at"));
    }

    #[test]
    fn hidden_defaults_to_false() {
        let cfg = parse(r#"
            [source]
            type = "csv"
            path = "/data/events.csv"
            gzip = false

            [engine]
            memory_limit = 1024
            chunk_size_rows = 100
            worker_threads = 1

            [api]
            bind = "0.0.0.0:8080"

            [searchable.amount]
            type = "float"
        "#).unwrap();
        assert!(!cfg.searchable["amount"].hidden);
    }

    #[test]
    fn reload_interval_defaults_to_zero() {
        let cfg = parse(r#"
            [source]
            type = "csv"
            path = "/data/events.csv"

            [engine]
            memory_limit = 1024
            chunk_size_rows = 100
            worker_threads = 1

            [api]
            bind = "0.0.0.0:8080"

            [searchable.amount]
            type = "float"
        "#).unwrap();
        assert_eq!(cfg.engine.reload_interval_mins, 0);
    }

    #[test]
    fn reload_interval_parses() {
        let cfg = parse(r#"
            [source]
            type = "csv"
            path = "/data/events.csv"

            [engine]
            memory_limit = 1024
            chunk_size_rows = 100
            worker_threads = 1
            reload_interval_mins = 15

            [api]
            bind = "0.0.0.0:8080"

            [searchable.amount]
            type = "float"
        "#).unwrap();
        assert_eq!(cfg.engine.reload_interval_mins, 15);
    }

    #[test]
    fn missing_file_is_config_error() {
        let err = load(std::path::Path::new("/does/not/exist.toml")).unwrap_err();
        assert!(matches!(err, AppError::Config(_)));
    }

    #[test]
    fn malformed_toml_is_config_error() {
        let err = parse("this is = not [valid toml").unwrap_err();
        assert!(matches!(err, AppError::Config(_)));
    }
}
