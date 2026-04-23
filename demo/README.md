# stats-cruncher demo

Spins up a local instance of stats-cruncher pre-loaded with 5 million synthetic events.

## Requirements

- [Docker](https://docs.docker.com/get-docker/) with Compose plugin

## Build & run

The first run compiles the Rust binary, which can take up to a few minutes. Once sample data is ready and loaded, app starts the HTTP API on `http://localhost:8080`.

```bash
docker compose up --build
```

```bash
docker compose up # subsequent runs, reuses cached image
```

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /status` | Engine status and memory usage |
| `POST /query` | Run a query against the dataset |
| `GET /api-docs` | Swagger UI |

## Example query

Purchases from Germany or the US in Q1 2025, broken down by country:

```json
{
  "must": {
    "event_type": ["purchase"],
    "country": ["DE", "US"]
  },
  "ranges": {
    "occurred_at": { "gte": "2025-01-01T00:00:00Z", "lte": "2025-03-31T23:59:59Z" }
  }
}
```

```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d @- <<'EOF'
{
  "must": {
    "event_type": ["purchase"],
    "country": ["DE", "US"]
  },
  "ranges": {
    "occurred_at": { "gte": "2025-01-01T00:00:00Z", "lte": "2025-03-31T23:59:59Z" }
  }
}
EOF
```

## Test dataset schema

| Column | Type | Notes |
|---|---|---|
| `user_id` | integer | hidden — filterable but not returned |
| `amount` | float | transaction amount (0.01 – 999.99) |
| `country` | string | DE, US, FR, PL, GB, JP, BR, CA, AU, NL |
| `event_type` | string | purchase (~95%), refund (~5%) |
| `occurred_at` | date-time | uniform distribution, 2024-01-01 – 2026-01-01 |

## Stop

```bash
docker compose down
```

Add `-v` to also delete the generated data volume.

## Benchmark

Use attached lua script to benchmark the engine:

```bash
wrk -t2 -c2 -d30s --timeout 5s --latency -s bench.lua http://localhost:8080
```

* **t** - number of threads
* **c** - number of connections
* **d** - benchmark duration
* **timeout** - http timeout for a single request
* **latency** - report latency statistics