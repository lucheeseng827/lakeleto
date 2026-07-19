//! # Lakeleto — the Postman of lakehouse tables
//!
//! An instant, local lakehouse-table explorer. Point Lakeleto at a `.parquet`/`.csv` and see its
//! schema, a clean row preview, and per-column profiles near-instantly — no signup, no
//! upload, no server, offline. (The default engine reads Parquet + CSV; Iceberg tables read
//! behind `--features iceberg`; a DuckDB backend is on the roadmap.) This is idea **#25
//! "Lakeleto"** from the idea map:
//! Low-Med MVP, "fastest to lovable," where **the engine is a commodity and the value is
//! the UX**.
//!
//! ## The one idea: an [`Engine`](engine::Engine) trait
//!
//! Because the engine is a commodity, Lakeleto is built around a single trait and the (future)
//! UI binds only to `Box<dyn Engine>`:
//!
//! - [`engine::local::LocalReaderEngine`] — the default, pure-Rust reader (arrow + parquet
//!   + csv). Always compiled; no C++ toolchain; the lean default build.
//! - `engine::sql::DataFusionEngine` (`--features sql`) — a real SQL planner over the same
//!   trait, with a read-only guard.
//! - `engine::remote::RemoteEngine` (`--features remote`) — the **Lakeleto Cloud**
//!   backend, *behind the same trait*. Not a separate product — one more `Engine`.
//!
//! That seam is the answer to "which engine do we build the UI on first?" — the **local**
//! one. Adding the hosted engine later is a new impl + a base-URL swap, not a rewrite. See
//! `the design notes`.

#[cfg(feature = "serve")]
pub mod api;
pub mod cli;
pub mod engine;
pub mod error;
#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "object-store")]
pub mod objstore;
pub mod render;
pub mod source;
pub mod workspace;
#[cfg(feature = "remote")]
pub mod workspace_remote;

pub use engine::local::LocalReaderEngine;
pub use engine::{
    Capabilities, ColumnProfile, ColumnSchema, Engine, NamedSource, RowBatch, TableProfile,
    TableSchema,
};
pub use error::{EngineError, Result};
pub use source::{Format, Source};
