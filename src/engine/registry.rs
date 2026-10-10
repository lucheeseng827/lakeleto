//! [`EngineRegistry`]: which engine answers which request.
//!
//! Every way into Lakeleto (the CLI, `/v1`, and the protocols that come after them) asks the same
//! question before it reads anything: which engine does this? The registry holds the engines a
//! process has, and [`EngineRegistry::resolve`] answers from what is wanted ([`Need`]) and what it
//! is wanted of:
//!
//! - a database table goes to the database engine, the only one that reads a live database;
//! - SQL goes to the SQL engine;
//! - a sorted or filtered grid window goes to the SQL engine, when it reads the table's format;
//! - anything else is a read, for the read engine.

use std::sync::Arc;

use super::{Engine, NamedSource, ScanSpec};
use crate::error::{EngineError, Result};
use crate::source::{Format, Source};

/// What an engine is wanted for.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum Need<'a> {
    /// Read one table: its schema, a preview, a profile.
    Read(&'a Source),
    /// A window of one table, sorted, filtered and projected as the spec says.
    Scan(&'a Source, &'a ScanSpec),
    /// SQL over these tables.
    Query(&'a [NamedSource]),
}

/// The engines one process reads with, and which one answers what. Cheap to clone: the engines
/// are shared.
#[derive(Clone)]
pub struct EngineRegistry {
    read: Arc<dyn Engine>,
    sql: Option<Arc<dyn Engine>>,
    database: Option<Arc<dyn Engine>>,
}

impl EngineRegistry {
    /// A registry that reads with `read`, with no SQL or database engine yet.
    pub fn new(read: Arc<dyn Engine>) -> Self {
        EngineRegistry {
            read,
            sql: None,
            database: None,
        }
    }

    /// This registry, running SQL on `sql`, and the grid's sorted and filtered windows too when it
    /// reads the table's format.
    pub fn with_sql(mut self, sql: Arc<dyn Engine>) -> Self {
        self.sql = Some(sql);
        self
    }

    /// This registry, reading database tables with `database`.
    pub fn with_database(mut self, database: Arc<dyn Engine>) -> Self {
        self.database = Some(database);
        self
    }

    /// This build's engines, reading as the process: the local reader, the SQL engine when built
    /// with `sql`, and the database engine when built with `sqlite`, `postgres` or `mysql`.
    pub fn local() -> Self {
        let registry = EngineRegistry::new(Arc::new(super::local::LocalReaderEngine::default()));
        #[cfg(feature = "sql")]
        let registry = registry.with_sql(Arc::new(super::sql::DataFusionEngine::new()));
        registry.with_local_database()
    }

    /// This registry, reading database tables with this build's database engine, when it was
    /// built with `sqlite`, `postgres` or `mysql`. Unchanged otherwise.
    pub fn with_local_database(self) -> Self {
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
        return self.with_database(Arc::new(super::database::DatabaseEngine::new()));
        #[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
        self
    }

    /// The engine that reads files, prefixes and catalog tables.
    pub fn read_engine(&self) -> &Arc<dyn Engine> {
        &self.read
    }

    /// Is there an SQL engine for files?
    pub fn has_sql(&self) -> bool {
        self.sql.is_some()
    }

    /// Can SQL run at all: over files on the SQL engine, or over a database on its own engine?
    pub fn can_query(&self) -> bool {
        self.sql.is_some() || self.database.is_some()
    }

    /// Every engine, once each, in the order read, SQL, database.
    pub fn engines(&self) -> Vec<Arc<dyn Engine>> {
        let mut all = vec![self.read.clone()];
        for engine in self.sql.iter().chain(&self.database) {
            if !all.iter().any(|known| Arc::ptr_eq(known, engine)) {
                all.push(engine.clone());
            }
        }
        all
    }

    /// The engine for `need`, or why there is none in this build.
    pub fn resolve(&self, need: Need<'_>) -> Result<Arc<dyn Engine>> {
        match need {
            Need::Read(source) if source.format == Format::Database => self.database(),
            Need::Read(_) => Ok(self.read.clone()),
            Need::Scan(source, _) if source.format == Format::Database => self.database(),
            // A plain window is a read, which the read engine answers with offset pushdown and
            // no planner. A sorted or filtered one goes to the SQL engine, for its unbounded sort
            // and filter, when it reads the table's format; otherwise the read engine sorts and
            // filters a bounded working set itself.
            //
            // The format check keeps a window on an engine that can read it: every sorted or
            // filtered window once went to DataFusion, JSON included, which it could not register
            // then, so a JSON grid failed on the first click. Falling back costs the planner, never
            // the feature. A format SQL reads through the local reader is complete over the file
            // either way: JSON and compressed text a pass at a time, Iceberg, Delta and a Parquet
            // directory loaded whole for each such window, at the cost of memory.
            //
            // The SQL engine is asked what it reads only when it is an engine of its own: one that
            // is also the read engine, as a remote is, answers the same request either way.
            Need::Scan(source, spec) => match &self.sql {
                Some(sql)
                    if !spec.is_plain_window()
                        && !Arc::ptr_eq(sql, &self.read)
                        && reads(sql.as_ref(), source.format) =>
                {
                    Ok(sql.clone())
                }
                _ => Ok(self.read.clone()),
            },
            Need::Query(tables) if tables.iter().any(|t| t.source.format == Format::Database) => {
                self.database()
            }
            Need::Query(_) => self
                .sql
                .clone()
                .ok_or_else(|| EngineError::missing_feature("run SQL", "sql")),
        }
    }

    fn database(&self) -> Result<Arc<dyn Engine>> {
        self.database
            .clone()
            .ok_or_else(|| EngineError::missing_feature("query a database", "sqlite"))
    }
}

/// Does `engine` say it reads `format`?
fn reads(engine: &dyn Engine, format: Format) -> bool {
    engine
        .capabilities()
        .formats
        .iter()
        .any(|f| f == format.as_str())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::context::RequestContext;
    use crate::engine::{Capabilities, RowBatch, TableProfile, TableSchema};

    /// An engine that reads `formats`, answers nothing, and counts how often it is asked what it
    /// reads.
    struct Stub {
        name: &'static str,
        formats: Vec<String>,
        asked: AtomicUsize,
    }

    fn stub(name: &'static str, formats: &[&str]) -> Arc<Stub> {
        Arc::new(Stub {
            name,
            formats: formats.iter().map(|f| f.to_string()).collect(),
            asked: AtomicUsize::new(0),
        })
    }

    impl Engine for Stub {
        fn name(&self) -> &str {
            self.name
        }
        fn capabilities(&self) -> Capabilities {
            self.asked.fetch_add(1, Ordering::SeqCst);
            Capabilities {
                engine: self.name.to_string(),
                formats: self.formats.clone(),
                sql: false,
                profile: false,
                remote: false,
                scan: false,
                filtered_stats: false,
            }
        }
        fn schema(&self, _: &RequestContext, _: &Source) -> Result<TableSchema> {
            unimplemented!()
        }
        fn preview(&self, _: &RequestContext, _: &Source, _: usize) -> Result<RowBatch> {
            unimplemented!()
        }
        fn profile(&self, _: &RequestContext, _: &Source, _: usize) -> Result<TableProfile> {
            unimplemented!()
        }
        fn query(&self, _: &RequestContext, _: &str, _: &[NamedSource]) -> Result<RowBatch> {
            unimplemented!()
        }
    }

    fn source(format: Format) -> Source {
        Source::with_format("t", format)
    }

    fn named(format: Format) -> NamedSource {
        NamedSource {
            name: "t".to_string(),
            source: source(format),
        }
    }

    fn name(engine: Result<Arc<dyn Engine>>) -> String {
        match engine {
            Ok(engine) => engine.name().to_string(),
            Err(e) => format!("error: {e}"),
        }
    }

    #[test]
    fn each_need_goes_to_the_engine_that_answers_it() {
        let registry = EngineRegistry::new(stub("read", &["parquet", "csv", "json"]))
            .with_sql(stub("sql", &["parquet", "csv"]))
            .with_database(stub("database", &["sqlite"]));
        let plain = ScanSpec::default();
        let sorted = ScanSpec {
            sort: Some(crate::engine::SortSpec {
                column: "id".to_string(),
                descending: false,
            }),
            ..Default::default()
        };
        for (need, engine) in [
            (Need::Read(&source(Format::Parquet)), "read"),
            (Need::Read(&source(Format::Database)), "database"),
            (Need::Scan(&source(Format::Csv), &plain), "read"),
            (Need::Scan(&source(Format::Csv), &sorted), "sql"),
            (Need::Scan(&source(Format::Json), &sorted), "read"),
            (Need::Scan(&source(Format::Database), &plain), "database"),
            (Need::Query(&[named(Format::Parquet)]), "sql"),
            (
                Need::Query(&[named(Format::Parquet), named(Format::Database)]),
                "database",
            ),
        ] {
            assert_eq!(name(registry.resolve(need)), engine);
        }
        assert!(registry.has_sql() && registry.can_query());
        let names: Vec<String> = registry
            .engines()
            .iter()
            .map(|e| e.name().to_string())
            .collect();
        assert_eq!(names, ["read", "sql", "database"]);
    }

    #[test]
    fn an_engine_this_build_lacks_is_named_with_the_feature_to_build() {
        let registry = EngineRegistry::new(stub("read", &["parquet"]));
        let db = name(registry.resolve(Need::Read(&source(Format::Database))));
        assert!(
            db.contains("query a database") && db.contains("sqlite"),
            "{db}"
        );
        let sql = name(registry.resolve(Need::Query(&[named(Format::Parquet)])));
        assert!(sql.contains("run SQL") && sql.contains("`sql`"), "{sql}");
        assert!(!registry.has_sql() && !registry.can_query());

        let databases_only =
            EngineRegistry::new(stub("read", &["parquet"])).with_database(stub("database", &[]));
        assert!(!databases_only.has_sql() && databases_only.can_query());
    }

    /// One engine registered for every need, as a remote is, answers all of them, and is never
    /// asked what it reads: for a remote, that question is a request to the server.
    #[test]
    fn one_engine_for_every_need_answers_all_of_them_without_being_asked_what_it_reads() {
        let remote = stub("remote", &[]);
        let registry = EngineRegistry::new(remote.clone())
            .with_sql(remote.clone())
            .with_database(remote.clone());
        let sorted = ScanSpec {
            filters: vec![crate::engine::FilterSpec {
                column: "id".to_string(),
                op: crate::engine::FilterOp::Eq,
                value: "1".to_string(),
            }],
            ..Default::default()
        };
        for need in [
            Need::Read(&source(Format::Unknown)),
            Need::Scan(&source(Format::Unknown), &sorted),
            Need::Query(&[named(Format::Unknown)]),
            Need::Read(&source(Format::Database)),
        ] {
            assert_eq!(name(registry.resolve(need)), "remote");
        }
        assert_eq!(registry.engines().len(), 1);
        assert_eq!(remote.asked.load(Ordering::SeqCst), 0);
    }
}
