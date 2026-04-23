use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

/// Top-level job configuration loaded from a user-supplied TOML file.
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub source: SourceConfig,
    pub engine: EngineConfig,
    pub api: ApiConfig,
    /// Only columns listed here are pulled off the input source; anything else is dropped
    /// during ingestion to keep the in-memory footprint tight.
    pub searchable: BTreeMap<String, SearchableColumn>,
    /// Optional partition column for chunk-level zone pruning. Must name a numeric or
    /// date-time column in `[searchable]`. When set, all rows are sorted by this column
    /// after ingestion and per-chunk min/max stats are recorded. Queries that include a
    /// range filter on this column will skip chunks whose range doesn't overlap.
    pub partition_column: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Csv { path: PathBuf, delimiter: Option<char>, #[serde(default)] gzip: bool },
    Sqlite { path: PathBuf, table: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// Hard RAM ceiling — exceeding it during ingestion is a fatal error.
    /// Accepts a raw byte count or a string with a binary suffix
    /// (`K`/`KB`/`KiB`, `M`, `G`, `T`)
    #[serde(deserialize_with = "deserialize_byte_size")]
    pub memory_limit: u64,
    /// Number of rows per morsel dispatched to a worker.
    pub chunk_size_rows: usize,
    /// Dedicated CPU-bound worker threads. Does not apply to the Tokio pool.
    pub worker_threads: usize,
    /// When true, non-hidden string columns include a per-value hit count in query responses.
    #[serde(default)]
    pub string_value_counts: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchableColumn {
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// When `true`, the column is indexed and filterable but excluded from query responses.
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Integer,
    Float,
    String,
    #[serde(rename = "date-time")]
    DateTime,
}

fn deserialize_byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct ByteSizeVisitor;

    impl<'de> Visitor<'de> for ByteSizeVisitor {
        type Value = u64;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a byte count as integer or suffixed string (e.g. 1024, \"8G\", \"8GiB\")")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("byte size cannot be negative"))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<u64, E> {
            parse_byte_size(v).map_err(E::custom)
        }
    }

    d.deserialize_any(ByteSizeVisitor)
}

fn parse_byte_size(raw: &str) -> Result<u64, String> {
    let s = raw.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '_')
        .unwrap_or(s.len());
    let (digits, suffix) = s.split_at(split);
    let digits = digits.replace('_', "");
    if digits.is_empty() {
        return Err(format!("byte size is missing a numeric prefix: {raw:?}"));
    }
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("invalid number in byte size: {raw:?}"))?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024u64.pow(2),
        "g" | "gb" | "gib" => 1024u64.pow(3),
        "t" | "tb" | "tib" => 1024u64.pow(4),
        other => return Err(format!("unknown byte size suffix {other:?} in {raw:?}")),
    };
    n.checked_mul(multiplier)
        .ok_or_else(|| format!("byte size overflow: {raw:?}"))
}

#[cfg(test)]
mod tests {
    use super::parse_byte_size;

    #[test]
    fn plain_bytes() {
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
        assert_eq!(parse_byte_size("8_589_934_592").unwrap(), 8 * 1024u64.pow(3));
    }

    #[test]
    fn binary_suffixes() {
        let g = 1024u64.pow(3);
        for form in ["8G", "8g", "8GB", "8gb", "8GiB", "8gib", "8 GiB"] {
            assert_eq!(parse_byte_size(form).unwrap(), 8 * g, "form={form}");
        }
        assert_eq!(parse_byte_size("4K").unwrap(), 4 * 1024);
        assert_eq!(parse_byte_size("16M").unwrap(), 16 * 1024u64.pow(2));
        assert_eq!(parse_byte_size("2T").unwrap(), 2 * 1024u64.pow(4));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("G").is_err());
        assert!(parse_byte_size("8X").is_err());
        assert!(parse_byte_size("8PB").is_err());
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_byte_size("99999999999T").is_err());
    }
}
