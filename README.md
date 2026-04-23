# stats-cruncher

`stats-cruncher` is an in-memory, read-optimized OLAP engine exposed via an HTTP API. Designed for high-throughput analytics, it ingests flat datasets at startup, loads them into memory, and executes fast boolean queries against them.

It is built for scenarios where the working dataset fits within system RAM.

> **Important Note on Text Data:** This engine does not perform full-text search (no `LIKE`, `CONTAINS`, or regex). All `string` columns are effectively treated as **enumerations**. Under the hood, strings are dictionary-encoded and mapped to highly compressed bitsets (Roaring Bitmaps). This allows the engine to resolve queries using fast bitwise operations instead of scanning text. Because of this architecture, string fields are strictly for **exact-hit filtering**. Do not ingest high-cardinality, free-form text (like log messages or unique UUIDs), as it will completely defeat the efficiency of the bitmap indexes.

## Quick start

1. Compile the release binary:

   ```bash
   cargo build --release
   ```

2. Copy the example configuration and map it to your dataset:

   ```bash
   cp config.example.toml config.toml
   ```

3. Initialize the service:

   ```bash
   ./target/release/stats-cruncher --config config.toml
   ```

   **Note on Boot Time:** On startup, the engine loads all source data into memory before accepting any requests. For large datasets this takes a moment. Once loading is complete the HTTP API activates.

## Configuration

All engine parameters and schema definitions are defined in a TOML file. A fully annotated reference is available at [`config.example.toml`](./config.example.toml). The core architectural choices are configured below.

### `[source]`

Defines the ingestion mechanism. The engine currently supports two formats, configured via the `type` field:

* **`csv`**: A flat file on disk. Accepts `path`, an optional `delimiter` (defaults to `,`), and a `gzip = true` flag for streaming decompression of `.gz` archives. The engine assumes the first row is the header.
* **`sqlite`**: A SQLite database file. Accepts `path` and the target `table`.

*Architecture Note:* Data is read strictly once at boot. Restart the service to ingest fresh data, or enable automatic hot reload — see `reload_interval_mins` below.

### `[engine]`

Controls the concurrency model.

* **`chunk_size_rows`**: Dictates the morsel size for parallel execution. This tunes the workload for CPU cache locality. Smaller values ensure fair scheduling across concurrent queries (preventing head-of-line blocking), while larger values reduce thread synchronization overhead. Leave at the default (`65_536`) unless profiling specific hardware.
* **`worker_threads`**: The size of the dedicated compute pool used for query execution. This defines the number of physical CPU cores the application will dedicate to data crunching (note that asynchronous HTTP request handlers may cause total thread usage to slightly exceed this limit). In a heavy load scenarios leave at least 1-2 cores free for other processes.
* **`string_value_counts`** When enabled, query responses include a per-value hit count for every string column (e.g., how many matched rows had country = "DE"). Cost scales with the number of distinct values per column, not with row count — cheap for low-cardinality enums, noticeable for high-cardinality ones.
* **`reload_interval_mins`** *(optional)* When set to a non-zero integer, enables automatic hot reload. Every N minutes the engine checks the modification time of both the config file and the data file. If either has changed, it waits until writes have stopped (by confirming the modification time is stable across two consecutive checks, one minute apart) and then re-ingests the data and atomically swaps the in-memory store — in-flight queries complete against the old store uninterrupted. When absent or set to `0`, reload is disabled and the engine only reads data at startup.

  A few things to be aware of:

  * **No file validation.** The engine loads whatever it finds on a disk. If a write process is interrupted partway through, the resulting file will be read as-is. It is the responsibility of the data pipeline to ensure data are valid.
  * **Transient resource spike.** During reload the new dataset is fully ingested into memory before the old one is released, so RAM usage temporarily doubles. CPU usage also rises as ingestion runs alongside live query processing.

### `[api]`

* **`bind`**: The interface and port for the HTTP listener (e.g., 0.0.0.0:8080). If exposing the engine to the internet, it is highly recommended to place it behind a reverse proxy (like NGINX or Caddy) to handle IP whitelisting, TLS, or authentication.

### `[searchable.<column>]`

Only columns declared here are loaded into memory. Any column not configured as searchable.*, even if it is available in the source data, is dropped during ingestion.

Each entry maps a column to a logical type:

* **`integer`**: 64-bit signed integer. Optimized for range scans.
* **`float`**: 64-bit floating point. Optimized for range scans.
* **`string`**: Text data. Dictionary-encoded into bitsets. Strictly for exact-match inclusion/exclusion filters (`must` / `must_not`).
* **`date-time`**: RFC3339 timestamp strings (e.g., `2026-04-19T00:00:00Z`). Stored internally as UTC epoch-seconds.

You may declare any number of date-time columns (`created_at`, `updated_at`, etc.). Clients specify which column to evaluate at query time.

An optional `hidden = true` flag marks a column as filter-only: it is fully indexed and can appear in any filter clause, but is excluded from the `numeric` block of query responses. Use this for ID or dimension columns where aggregated stats (min/max/sum) are meaningless.

```toml
[searchable.user_id]
type   = "integer"
hidden = true
```

> **NULL and empty values are allowed.** A row is only affected in the context of that specific column: it never matches a `must` or range filter on the null column, always passes a `must_not` filter on it (no value is certainly not the excluded value). It will be still included in results if `must` filters do not mention its empty columns.

## HTTP API

The application automatically serves OpenAPI (Swagger) documentation, allowing you to explore the schema and test endpoints interactively.

* **Swagger UI:** `/docs` (e.g., `http://localhost:8080/docs`)
* **Raw OpenAPI JSON:** `/api-doc/openapi.json`

### `GET /status`

Returns basic runtime stats. Useful for monitoring and confirming the service is up and data is loaded.

```json
{
  "uptime_secs": 3600,
  "rows_loaded": 10000000,
  "queries": { "last_1m": 5, "last_1h": 42, "last_24h": 300 },
  "errors":  { "last_1m": 0, "last_1h": 1,  "last_24h": 3  }
}
```

### `POST /query`

Submits a boolean filter query to the engine. The JSON payload accepts three optional filter clauses: `must`, `must_not`, and `ranges`. An empty query matches the entire dataset.

```json
{
  "must": {
    "country": ["DE", "FR"]
  },
  "must_not": {
    "event_type": ["test"]
  },
  "ranges": {
    "user_id":     [{ "eq": 1001 }, { "eq": 1005 }, { "eq": 1012 }],
    "amount":      { "gte": 10, "lt": 100 },
    "occurred_at": { "gte": "2026-04-19T00:00:00Z", "lt": "2026-04-20T00:00:00Z" }
  }
}
```

* **`must`**: An inclusion filter. For a given column, the row must match *at least one* of the provided exact values (Logical OR within the array). Multiple columns are intersected (Logical AND).
* **`must_not`**: An exclusion filter. A row is dropped if it exactly matches *any* of the provided values across any of the listed columns.
* **`ranges`**: Numeric or temporal boundary scans. Each column entry is either a single range object or an array of range objects — multiple ranges on the same column are OR'd together. Accepted bounds: `gt`, `gte`, `lt`, `lte`, and `eq` (exact match). Dates must be valid RFC3339 strings; numbers are parsed as `f64` or `i64`.

To filter a numeric column by a discrete set of values, pass an array of `eq` objects:

#### Querying a full day

The API requires full RFC3339 timestamps — bare dates like `2026-04-19` are not accepted. To cover an entire day use a half-open interval:

```json
"ranges": {
  "occurred_at": {
    "gte": "2026-04-19T00:00:00Z",
    "lt":  "2026-04-20T00:00:00Z"
  }
}
```

#### Response

```json
{
  "matched_rows": 299682,
  "numeric": {
    "amount":      { "count": 299100, "sum": 52468821.52, "min": 50.0,  "max": 300.0 },
    "occurred_at": { "count": 299682, "oldest": "2025-04-05T06:01:14Z", "newest": "2026-01-01T00:05:51Z" }
  }
}
```

* **`matched_rows`**: Total rows satisfying all filter clauses.
* **`numeric`**: Per-column aggregates for every non-hidden numeric and date-time column.
  * **`count`**: Number of matched rows with a non-null value for this column. May be less than `matched_rows` when the column contains missing values.
  * Numeric columns (`integer`, `float`) additionally report `sum`, `min`, and `max`.
  * Date-time columns additionally report `oldest` and `newest` as RFC3339 strings.
  * Columns declared `hidden = true` in the config are omitted entirely.

## Adding a new data source

The ingestion layer is built around a single trait:

```rust
pub trait Ingestor {
    fn ingest(&self, store: &mut ColumnStore) -> AppResult<()>;
}
```

To add a new source format:

1. **Add a variant to `SourceConfig`** in `src/config/schema.rs` with whatever connection parameters the source needs (path, URL, table name, etc.).
2. **Create `src/ingestion/<format>.rs`** implementing `Ingestor`. Stream rows one at a time and push each via `store.push_row()`. The store coerces values into the declared column type, but the ingestor must provide them in a coercible form — passing everything as strings is fine as long as the strings are valid for the target type: a decimal integer or float string for numeric columns, a non-empty string for string columns, and an RFC3339 timestamp (e.g. `2026-04-19T00:00:00Z`) for date-time columns. Pass `RawValue::Null` or an empty/whitespace string to represent a missing value for any type.
3. **Call** `resource::check_memory(limit)` every `cfg.engine.chunk_size_rows` rows to respect the memory cap and fail fast on runaway ingestion.
3. **Register the variant** in the `select()` match in `src/ingestion.rs`.

The `[searchable]` schema and the query layer require no changes — they are source-agnostic.