//! Error type shared by every [`Engine`](crate::engine::Engine) backend.
//!
//! One error enum crosses the trait seam so the (future) UI handles failures uniformly
//! whether they come from the local reader, the DataFusion SQL engine, or the remote
//! Lakeleto Cloud engine. `UnsupportedOperation` deliberately carries a `hint` so a missing
//! feature (e.g. SQL without `--features sql`) is a helpful message, not a hard wall.

use thiserror::Error;

use crate::source::Format;

/// Everything an engine can go wrong with.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The file/directory format could not be read by the chosen engine.
    #[error("unsupported format: {detail}")]
    UnsupportedFormat { detail: String },

    /// The engine is real but does not implement this operation in this build.
    #[error("engine `{engine}` cannot {op}: {hint}")]
    UnsupportedOperation {
        engine: String,
        op: String,
        hint: String,
    },

    #[error("arrow error: {0}")]
    Arrow(String),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("query error: {0}")]
    Query(String),

    /// Remote (Lakeleto Cloud) engine failure — network, auth, or not-yet-available.
    #[error("remote engine: {0}")]
    Remote(String),

    /// A request tried to reach a path outside the server's configured `--root` confinement.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// A response (e.g. a `/v1/export` body) exceeded its size cap.
    #[error("too large: {0}")]
    TooLarge(String),

    #[error("{0}")]
    Other(String),
}

impl EngineError {
    pub fn arrow(e: arrow_schema::ArrowError) -> Self {
        EngineError::Arrow(e.to_string())
    }

    pub fn parquet(e: parquet::errors::ParquetError) -> Self {
        EngineError::Parquet(e.to_string())
    }

    pub fn unsupported_format(format: Format, engine: &str) -> Self {
        EngineError::UnsupportedFormat {
            detail: format!(
                "the `{engine}` engine cannot read {} sources yet",
                format.as_str()
            ),
        }
    }

    /// Standard "you didn't compile that backend" message.
    pub fn missing_feature(op: &str, feature: &str) -> Self {
        EngineError::UnsupportedOperation {
            engine: feature.to_string(),
            op: op.to_string(),
            hint: format!(
                "this binary was built without the `{feature}` feature — rebuild with \
                 `cargo build --features {feature}`"
            ),
        }
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;
