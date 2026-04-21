use utoipa::OpenApi;

use crate::api::dto::{
    ErrorResponse, NumericStatsDto, QueryRequest, QueryResponse, RangeDto, StatusResponse,
    WindowCounts,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::api::handlers::status,
        crate::api::handlers::query,
    ),
    components(schemas(
        StatusResponse,
        WindowCounts,
        QueryRequest,
        RangeDto,
        QueryResponse,
        NumericStatsDto,
        ErrorResponse,
    )),
    info(
        title = "stats-cruncher",
        description = "In-memory OLAP engine HTTP API."
    )
)]
pub struct ApiDoc;
