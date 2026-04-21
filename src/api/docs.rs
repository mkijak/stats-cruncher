use utoipa::OpenApi;

use crate::api::dto::{
    ErrorResponse, HealthResponse, NumericStatsDto, QueryRequest, QueryResponse, RangeDto,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::api::handlers::health,
        crate::api::handlers::query,
    ),
    components(schemas(
        QueryRequest,
        RangeDto,
        QueryResponse,
        NumericStatsDto,
        HealthResponse,
        ErrorResponse,
    )),
    info(
        title = "stats-cruncher",
        description = "In-memory OLAP engine HTTP API."
    )
)]
pub struct ApiDoc;
