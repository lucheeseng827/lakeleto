//! Error type shared by every [`Engine`](crate::engine::Engine) backend.
//!
//! One error enum crosses the trait seam so the (future) UI handles failures uniformly
//! whether they come from the local reader, the DataFusion SQL engine, or the remote
//! Lakeleto Cloud engine. `UnsupportedOperation` deliberately carries a `hint` so a missing
//! feature (e.g. SQL without `--features sql`) is a helpful message, not a hard wall.

use thiserror::Error;

use crate::source::Format;

/// Why a call stopped early. Carried by [`EngineError::Cancelled`].
///
/// The two are kept apart because they need different answers. A deadline means *this budget*
/// was too small — retrying with a bigger one is reasonable. An explicit cancellation means
/// somebody decided the answer was no longer wanted, and retrying it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// The context's deadline passed.
    Deadline,
    /// A holder of the context's `CancelToken` asked the work to stop.
    Requested,
}

impl std::fmt::Display for CancelReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CancelReason::Deadline => {
                f.write_str("cancelled: the deadline for this request passed")
            }
            CancelReason::Requested => f.write_str("cancelled: the request was cancelled"),
        }
    }
}

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

    /// The call stopped before finishing because its [`RequestContext`](crate::RequestContext)
    /// said to — a deadline passed, or someone asked.
    ///
    /// Distinct from every other variant because nothing went *wrong*: the work was correct and
    /// incomplete. A caller that retries an `Io` error should not retry this one without first
    /// changing the budget, and a caller counting failures should not count it as one.
    #[error("{0}")]
    Cancelled(CancelReason),

    #[error("{0}")]
    Other(String),
}

impl EngineError {
    /// An Arrow error. Parquet's error for a page compressed with a codec this build lacks reaches
    /// here too, from the Parquet reader, and becomes the refusal that names the feature.
    pub fn arrow(e: arrow_schema::ArrowError) -> Self {
        let message = e.to_string();
        undecodable_parquet(&message).unwrap_or(EngineError::Arrow(message))
    }

    /// A Parquet error, a page compressed with a codec this build lacks named for the feature that
    /// decodes it.
    pub fn parquet(e: parquet::errors::ParquetError) -> Self {
        let message = e.to_string();
        undecodable_parquet(&message).unwrap_or(EngineError::Parquet(message))
    }

    pub fn unsupported_format(format: Format, engine: &str) -> Self {
        // `Unknown` is not a format this engine "cannot read yet" — it is a source that was
        // never resolved on this machine because a server was supposed to resolve it (see
        // `Source::unresolved`). Saying "cannot read unknown sources yet" would send a reader
        // looking for a missing reader; say what actually went wrong instead.
        if format == Format::Unknown {
            return EngineError::UnsupportedFormat {
                detail: format!(
                    "the `{engine}` engine was given a source whose format was left for a \
                     server to resolve — only a remote engine can read one (pass \
                     `--remote-url`), or name the format explicitly with `--format`"
                ),
            };
        }
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

/// The Parquet codecs a build decompresses with the `compression` feature: the name parquet's error
/// gives the feature it was built without, and the codec's. Every build has Snappy, gzip and LZ4.
const COMPRESSION_PARQUET_CODECS: [(&str, &str); 2] = [("zstd", "zstd"), ("brotli", "Brotli")];

/// The Parquet codecs a build without `compression` decompresses: `iceberg` has zstd, which it
/// links for Avro manifests. (A build with `sql` has all five, through DataFusion.)
const PARQUET_CODECS_WITHOUT_COMPRESSION: &str = if cfg!(feature = "iceberg") {
    "Snappy, gzip, LZ4 and zstd"
} else {
    "Snappy, gzip and LZ4"
};

/// The refusal for a Parquet page compressed with a codec this build lacks, when `message` holds
/// parquet's error for one (`Disabled feature at compile time: zstd`); `None` for any other.
/// Parquet's words name a feature of parquet's, which no Lakeleto user can turn on; this names
/// Lakeleto's. zstd is Polars' default, so it is the one met most.
fn undecodable_parquet(message: &str) -> Option<EngineError> {
    let (_, lacking) = message.split_once("Disabled feature at compile time: ")?;
    let &(_, codec) = COMPRESSION_PARQUET_CODECS
        .iter()
        .find(|(feature, _)| lacking.starts_with(feature))?;
    Some(EngineError::UnsupportedFormat {
        detail: format!(
            "this Parquet file's pages are {codec}-compressed, and this build decompresses \
             {PARQUET_CODECS_WITHOUT_COMPRESSION} only: {codec} needs the `compression` feature \
             (`cargo install lakeleto --features compression`), which the release binaries and \
             the image have ({message})"
        ),
    })
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use arrow_schema::ArrowError;
    use parquet::errors::ParquetError;

    use super::*;

    /// Parquet's error for a page compressed with a codec it was built without, as its reader
    /// returns it; `tests/parquet_codecs.rs` reads such a file to pin the words.
    fn lacking(feature: &str) -> ParquetError {
        ParquetError::General(format!("Disabled feature at compile time: {feature}"))
    }

    /// A Parquet codec this build lacks is refused naming the feature that decodes it, whether
    /// parquet's error arrives as itself or as the Arrow reader hands it on.
    #[test]
    fn a_parquet_codec_this_build_lacks_is_refused_naming_the_feature() {
        for (feature, codec) in [("zstd", "zstd"), ("brotli", "Brotli")] {
            let wrapped = ArrowError::from(lacking(feature));
            for refusal in [
                EngineError::parquet(lacking(feature)),
                EngineError::arrow(wrapped),
            ] {
                let EngineError::UnsupportedFormat { detail } = &refusal else {
                    panic!("{feature}: {refusal}");
                };
                assert!(detail.contains(&format!("{codec}-compressed")), "{detail}");
                assert!(detail.contains("--features compression"), "{detail}");
                assert!(detail.contains(&lacking(feature).to_string()), "{detail}");
            }
        }
    }

    /// Any other error is passed on as it is, a codec every build has included.
    #[test]
    fn other_errors_are_passed_on() {
        let missing_snappy = "Disabled feature at compile time: snap";
        assert!(matches!(
            EngineError::parquet(ParquetError::General(missing_snappy.into())),
            EngineError::Parquet(m) if m.contains(missing_snappy)
        ));
        assert!(matches!(
            EngineError::arrow(ArrowError::ComputeError("no kernel".into())),
            EngineError::Arrow(m) if m.contains("no kernel")
        ));
    }
}
