use std::collections::BTreeMap;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::query::{
    Comparison, Must, MustNot, NumericBound, Query, RangeFilter, Response, StringMatch,
};
use crate::query::NumericStats;

/// Query request body.
#[derive(Debug, Deserialize, ToSchema)]
pub struct QueryRequest {
    /// An inclusion filter. For a given column, the row must match at least one of the provided exact values (Logical OR within the array). Multiple columns are intersected (Logical AND).
    #[serde(default)]
    pub must: BTreeMap<String, Vec<String>>,

    /// An exclusion filter. A row is dropped if it exactly matches any of the provided values across any of the listed columns.
    #[serde(default)]
    pub must_not: BTreeMap<String, Vec<String>>,

    /// Numeric or temporal boundary scans. Accepts any valid combination of gt, gte, lt, and lte. Each bound is either a JSON number (integer/float columns) or an RFC3339 date-time string (date-time columns).
    #[serde(default)]
    pub ranges: BTreeMap<String, RangeDto>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RangeDto {
    pub gt: Option<BoundValue>,
    pub gte: Option<BoundValue>,
    pub lt: Option<BoundValue>,
    pub lte: Option<BoundValue>,
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
    /// Per-column stats. Only numeric and date-time columns are summarised.
    /// Date-time columns report seconds-since-epoch UTC.
    pub numeric: BTreeMap<String, NumericStatsDto>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NumericStatsDto {
    pub count: u64,
    pub sum: f64,
    /// `None` when no rows matched (otherwise the minimum observed value).
    pub min: Option<f64>,
    /// `None` when no rows matched (otherwise the maximum observed value).
    pub max: Option<f64>,
}

impl From<NumericStats> for NumericStatsDto {
    fn from(s: NumericStats) -> Self {
        let (min, max) = if s.count == 0 {
            (None, None)
        } else {
            (Some(s.min), Some(s.max))
        };
        Self { count: s.count, sum: s.sum, min, max }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: &'static str,
}

impl HealthResponse {
    pub fn ok() -> Self {
        Self { status: "ok" }
    }
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
            .map(|(k, r)| RangeFilter::try_from(r).map(|rf| (k, rf)))
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
        Self {
            matched_rows: r.matched_rows,
            numeric: r.numeric.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}
