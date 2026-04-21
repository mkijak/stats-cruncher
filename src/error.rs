use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),

    #[error("ingestion error: {0}")]
    Ingestion(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type AppResult<T> = Result<T, AppError>;
