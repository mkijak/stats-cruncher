mod schema;

pub use schema::{AppConfig, ApiConfig, ColumnType, EngineConfig, SearchableColumn, SourceConfig};

use std::path::Path;

use crate::error::{AppError, AppResult};

pub fn load(path: &Path) -> AppResult<AppConfig> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        AppError::Config(format!("cannot read config {}: {e}", path.display()))
    })?;
    toml::from_str(&raw).map_err(|e| {
        AppError::Config(format!("parsing config {}: {e}", path.display()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_example_config() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config.example.toml");
        let cfg = load(&path).unwrap();
        assert_eq!(cfg.engine.memory_limit, 8 * 1024u64.pow(3));
        assert_eq!(cfg.engine.chunk_size_rows, 65_536);
        assert_eq!(cfg.engine.worker_threads, 8);
        assert_eq!(cfg.api.bind, "0.0.0.0:8080");
        assert!(matches!(cfg.source, SourceConfig::Csv { gzip: true, .. }));
        assert!(cfg.searchable.contains_key("occurred_at"));
    }

    #[test]
    fn missing_file_is_config_error() {
        let err = load(std::path::Path::new("/does/not/exist.toml")).unwrap_err();
        assert!(matches!(err, AppError::Config(_)));
    }

    #[test]
    fn malformed_toml_is_config_error() {
        let dir = tempdir();
        let path = dir.join("broken.toml");
        std::fs::write(&path, "this is = not [valid toml").unwrap();
        let err = load(&path).unwrap_err();
        assert!(matches!(err, AppError::Config(_)));
    }

    fn tempdir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("stats-cruncher-config-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
