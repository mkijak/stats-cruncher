use std::collections::BTreeMap;

use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use chrono::{DateTime, TimeZone, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::query::{
    ColumnKind, Comparison, Must, MustNot, NumericBound, Query, RangeFilter, Response, StringMatch,
};

pub struct AppJson<T>(pub T);

impl<T, S> FromRequest<S> for AppJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<ErrorResponse>);

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(v)) => Ok(AppJson(v)),
            Err(rejection) => {
                let status = match &rejection {
                    JsonRejection::JsonSyntaxError(_) => StatusCode::BAD_REQUEST,
                    JsonRejection::MissingJsonContentType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    _ => StatusCode::UNPROCESSABLE_ENTITY,
                };
                Err((status, axum::Json(ErrorResponse::new(rejection.body_text()))))
            }
        }
    }
}

/// Query request body.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// An inclusion filter. For a given column, the row must match at least one of the provided exact values (Logical OR within the array). Multiple columns are intersected (Logical AND).
    #[serde(default)]
    pub must: BTreeMap<String, Vec<String>>,

    /// An exclusion filter. A row is dropped if it exactly matches any of the provided values across any of the listed columns.
    #[serde(default)]
    pub must_not: BTreeMap<String, Vec<String>>,

    /// Numeric or temporal boundary scans. Each entry is either a single range object or an array of range objects (OR semantics across the array). Each bound is either a JSON number (integer/float columns) or an RFC3339 date-time string (date-time columns).
    #[serde(default)]
    pub ranges: BTreeMap<String, RangeOrList>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RangeDto {
    pub eq: Option<BoundValue>,
    pub gt: Option<BoundValue>,
    pub gte: Option<BoundValue>,
    pub lt: Option<BoundValue>,
    pub lte: Option<BoundValue>,
}

/// Accepts either a single range object or an array of range objects for the same column.
/// Multiple ranges are combined with OR: a row matches if it satisfies any one of them.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RangeOrList {
    One(RangeDto),
    Many(Vec<RangeDto>),
}

/// A single range bound. Either a raw JSON number for numeric columns or an
/// RFC3339 date-time string for date-time columns. At evaluation time both
/// collapse to f64 (date-times become UTC epoch seconds, milliseconds are dropped).
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum BoundValue {
    Number(f64),
    DateTime(String),
}

impl TryFrom<BoundValue> for f64 {
    type Error = String;
    fn try_from(v: BoundValue) -> Result<f64, String> {
        match v {
            BoundValue::Number(n) => Ok(n),
            BoundValue::DateTime(s) => DateTime::parse_from_rfc3339(s.trim())
                .map(|dt| dt.timestamp() as f64)
                .map_err(|e| format!("invalid RFC3339 date-time {s:?}: {e}")),
        }
    }
}

/// Query response body.
#[derive(Debug, Serialize, Default, ToSchema)]
pub struct QueryResponse {
    /// Total number of rows in the store matching the filter.
    pub matched_rows: u64,
    /// Per-column stats. Numeric columns include `sum`/`min`/`max`.
    /// Date-time columns include `oldest`/`newest` as RFC3339 strings instead.
    pub numeric: BTreeMap<String, ColumnStatsDto>,
    /// Per-value hit counts for non-hidden string columns.
    /// Only present when `engine.string_value_counts` is enabled.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub string_counts: BTreeMap<String, BTreeMap<String, u64>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ColumnStatsDto {
    /// Number of matched rows with a non-null value for this column. May be less than
    /// `matched_rows` when the column contains missing values.
    pub count: u64,
    /// Present for numeric columns only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum: Option<f64>,
    /// Present for numeric columns only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Present for numeric columns only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Present for date-time columns only. RFC3339 timestamp of the earliest matched row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest: Option<String>,
    /// Present for date-time columns only. RFC3339 timestamp of the latest matched row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest: Option<String>,
}

/// Response body for GET /status.
#[derive(Debug, Serialize, ToSchema)]
pub struct StatusResponse {
    /// Seconds since the process started.
    pub uptime_secs: u64,
    /// Number of rows loaded into the in-memory store.
    pub rows_loaded: u64,
    pub queries: WindowCounts,
    pub errors: WindowCounts,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WindowCounts {
    pub last_1m: u64,
    pub last_1h: u64,
    pub last_24h: u64,
}

/// Error response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

impl ErrorResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self { error: message.into() }
    }
}

impl TryFrom<QueryRequest> for Query {
    type Error = String;
    fn try_from(req: QueryRequest) -> Result<Self, String> {
        let must = Must(
            req.must
                .into_iter()
                .map(|(k, v)| (k, StringMatch { values: v }))
                .collect(),
        );
        let must_not = MustNot(
            req.must_not
                .into_iter()
                .map(|(k, v)| (k, StringMatch { values: v }))
                .collect(),
        );
        let ranges = req
            .ranges
            .into_iter()
            .map(|(k, r)| {
                let filters: Result<Vec<RangeFilter>, String> = match r {
                    RangeOrList::One(dto) => RangeFilter::try_from(dto).map(|rf| vec![rf]),
                    RangeOrList::Many(dtos) => {
                        dtos.into_iter().map(RangeFilter::try_from).collect()
                    }
                };
                filters.map(|v| (k, v))
            })
            .collect::<Result<_, _>>()?;
        Ok(Query { must, must_not, ranges })
    }
}

impl TryFrom<RangeDto> for RangeFilter {
    type Error = String;
    fn try_from(r: RangeDto) -> Result<Self, String> {
        let to_bound = |v: BoundValue, comparison: Comparison| -> Result<NumericBound, String> {
            Ok(NumericBound { value: f64::try_from(v)?, comparison })
        };
        if let Some(v) = r.eq {
            let val = f64::try_from(v)?;
            return Ok(RangeFilter {
                lower: Some(NumericBound { value: val, comparison: Comparison::Gte }),
                upper: Some(NumericBound { value: val, comparison: Comparison::Lte }),
            });
        }
        let lower = match (r.gte, r.gt) {
            (Some(v), _) => Some(to_bound(v, Comparison::Gte)?),
            (None, Some(v)) => Some(to_bound(v, Comparison::Gt)?),
            (None, None) => None,
        };
        let upper = match (r.lte, r.lt) {
            (Some(v), _) => Some(to_bound(v, Comparison::Lte)?),
            (None, Some(v)) => Some(to_bound(v, Comparison::Lt)?),
            (None, None) => None,
        };
        Ok(RangeFilter { lower, upper })
    }
}

impl From<Response> for QueryResponse {
    fn from(r: Response) -> Self {
        let numeric = r
            .numeric
            .into_iter()
            .map(|(name, stats)| {
                let kind = r.column_types.get(&name).copied().unwrap_or(ColumnKind::Numeric);
                let dto = match kind {
                    ColumnKind::DateTime => ColumnStatsDto {
                        count: stats.count,
                        sum: None,
                        min: None,
                        max: None,
                        oldest: (stats.count > 0).then(|| epoch_to_rfc3339(stats.min)),
                        newest: (stats.count > 0).then(|| epoch_to_rfc3339(stats.max)),
                    },
                    ColumnKind::Numeric => ColumnStatsDto {
                        count: stats.count,
                        sum: Some(stats.sum),
                        min: (stats.count > 0).then_some(stats.min),
                        max: (stats.count > 0).then_some(stats.max),
                        oldest: None,
                        newest: None,
                    },
                };
                (name, dto)
            })
            .collect();
        Self { matched_rows: r.matched_rows, numeric, string_counts: r.string_counts }
    }
}

fn epoch_to_rfc3339(epoch_secs: f64) -> String {
    Utc.timestamp_opt(epoch_secs as i64, 0)
        .single()
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| epoch_secs.to_string())
}
