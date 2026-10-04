//! [`RequestContext`] — what the caller knows that the engine cannot.
//!
//! # Why this exists as a parameter rather than a field
//!
//! Every [`Engine`](crate::engine::Engine) method used to take only its data arguments, so the
//! engine had no way to learn anything about the *call*: how long it may run, whether anyone
//! still wants the answer, who asked, and what that caller may read as. The only channel for
//! caller-specific information was a field on the engine value, and the reasoning for that was
//! written down where it was chosen:
//!
//! > *"A field rather than a per-call argument because [`Engine`] is a stable object-safe trait
//! > shared by every backend and must not grow a cloud-specific parameter."*
//! > — `engine/sql.rs`, on `store_options`
//!
//! That objection is about the *trait signature*, and it still holds: the trait grew exactly one
//! parameter, and it is this type. What may go *inside* this type is a separate question, and the
//! answer is the rule below.
//!
//! # What belongs in here
//!
//! Two kinds of thing, and nothing else.
//!
//! **Advisory** — a deadline, a cancellation flag, a trace id. A backend may ignore all of it and
//! still be correct: the contract is "honour these if you can", not "implement these". An engine
//! that ignores them is uninterruptible, not wrong.
//!
//! **Identity** — who is asking ([`tenant`](RequestContext::tenant)) and what they may read as
//! (`store_options`). A backend may *not* ignore this and stay
//! correct, because reading as the wrong principal is the single failure a credential seam exists
//! to prevent. An engine either honours the identity or refuses the read.
//!
//! Identity is the one place backend-specific material is allowed here, and that deserves a
//! reason, because `objstore::StoreOptions` is unmistakably
//! object-store-specific and the quote above reads like it forbids exactly this. The line is
//! between *configuration* and *identity*. A region, an endpoint, a batch size are configuration:
//! they say how a backend is set up, they are the same for every caller, and they belong on the
//! engine value — an `s3_region` in this struct would still be wrong, and that is what the
//! objection was protecting. An identity is irreducibly per-call, because it is a property of the
//! caller rather than of the engine, and it has no neutral spelling: there is no
//! backend-agnostic way to say "tenant `acme`'s AWS role". Something that is per-call and
//! cannot be said in the abstract has to be carried concretely or not carried at all.
//!
//! # What moving identity here bought
//!
//! While identity lived on the engine value, `ee/src/compute.rs::engine_for` had to build a
//! **fresh** `DataFusionEngine` for every run whose store options differed — not for cost (the
//! struct was one field) but because an engine held ONE identity, and handing it two tenants' work
//! in sequence would read the second as the first. Nothing an engine learns across a request —
//! session, catalog, statistics, a compiled plan — could ever be reused. With identity in the
//! context that function is deleted: one warm engine serves every tenant, and the thing that keeps
//! them apart travels with the call instead of with the engine.
//!
//! It also closed a latent trap. `LocalReaderEngine`
//! held store options but passed them only to the Iceberg mirror; every other remote read called
//! the environment-reading wrapper, so an engine built for one identity read plain remote Parquet,
//! CSV and JSON as whatever principal the process carried. Nothing live reached it — the only
//! caller that set non-ambient options routed Iceberg — which is exactly what made it a trap
//! rather than a bug. Resolving identity per call puts all of those reads on one path.
//!
//! # The fast path is the common path
//!
//! A CLI invocation has no deadline, no canceller and no tenant. [`RequestContext::detached`] is
//! that, and [`RequestContext::check`] short-circuits on it before reading a clock or an atomic —
//! so threading this through a hot loop costs a predictable branch when nobody is watching.
//!
//! # Not here yet, and stated so the absence is not mistaken for a decision
//!
//! - **Per-URI database identity.** [`DbCredentials`] is carried, and it overrides the connection
//!   URI's own userinfo, so a location no longer has to embed a secret. But one context carries one
//!   database identity, the same single-value limitation `store_options` has, and for a query
//!   joining two databases under two accounts it would want a resolver.
//! - **Per-URI identity.** One context carries one object-store identity, which is what the plane
//!   needs today: `ee::creds`' vendor is keyed by tenant, and every table in one query belongs to
//!   one tenant. A query spanning two buckets under two different roles would want a resolver
//!   rather than a value. `store_options` is a method, so that
//!   change would not move a single call site.
//! - **A `Waker`-based canceller.** [`CancelToken`] is a flag that a caller polls, because the
//!   [`Engine`](crate::engine::Engine) trait is synchronous and a synchronous scan loop can only
//!   poll. A future async seam wants a real notifier; the flag is forward-compatible with one
//!   (a notifier can set it), so callers do not need to change.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{CancelReason, EngineError, Result};

/// A shared "stop" flag. Cloning gives another handle to the *same* flag, so the caller keeps
/// one and the engine is handed another inside a [`RequestContext`].
///
/// Deliberately a flag and not a channel: the engine seam is synchronous, so the only thing a
/// scan loop can do is look. `Ordering::Relaxed` is sufficient — this is a single boolean with
/// no other state ordered against it, and a check that observes the flag one batch late is
/// indistinguishable from one that was cancelled one batch later.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask every holder of this token to stop. Idempotent; never blocks.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// What the caller may connect to a **database** as.
///
/// The object-store half of identity is [`StoreOptions`](crate::objstore::StoreOptions), and the
/// asymmetry between the two is worth stating because it is the whole reason this type exists. An
/// object-store URI names a location and its credentials arrive *beside* it, so a `Source` can be
/// passed around, cached and serialized with nothing secret on it. A database URI has no such
/// split: `postgres://alice:hunter2@db/orders` is simultaneously the location and the credential,
/// so the secret rode on the `Source` — the one type `crate::source` documents as "must never be
/// cloned into a cache or a response body", and which was being serialized into
/// `TableSchema.source` and persisted into run history.
///
/// With this on the context, a catalog entry can be `postgres://db/orders?table=t` — a location and
/// nothing else — and the identity is vended per call, which is what a multi-tenant plane needs and
/// what the inline form can never provide.
///
/// Overrides rather than merges with the URI's own userinfo, the same rule
/// [`RequestContext::with_store_options`] follows and for the same reason: combining two
/// credentials produces a third principal nobody asked to connect as.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct DbCredentials {
    username: Option<Arc<str>>,
    password: Option<Arc<str>>,
}

/// Prints the username but never the password — the same line
/// [`StoreOptions`](crate::objstore::StoreOptions)' own `Debug` draws, and for the same reason: a
/// context is the sort of thing that ends up in a tracing span or a panic message.
impl std::fmt::Debug for DbCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbCredentials")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl DbCredentials {
    /// A username and password pair.
    pub fn new(username: impl Into<Arc<str>>, password: impl Into<Arc<str>>) -> Self {
        Self {
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }

    /// A username with no password — Postgres peer/IAM auth, or a MySQL socket login.
    pub fn user_only(username: impl Into<Arc<str>>) -> Self {
        Self {
            username: Some(username.into()),
            password: None,
        }
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// True when there is nothing here to override a URI's own userinfo with.
    pub fn is_empty(&self) -> bool {
        self.username.is_none() && self.password.is_none()
    }
}

/// Per-call execution context. Cheap to clone (four `Arc`s at most) and `Send + Sync`, so it
/// crosses `spawn_blocking` with the rest of a request's arguments.
#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    deadline: Option<Instant>,
    cancel: Option<CancelToken>,
    tenant: Option<Arc<str>>,
    trace_id: Option<Arc<str>>,
    /// What this caller may read object storage as. `Arc` because it is cloned per call and
    /// never mutated, and because a `StoreOptions` holding a live credential provider is not
    /// something to copy per record batch.
    #[cfg(feature = "object-store")]
    store_options: Option<Arc<crate::objstore::StoreOptions>>,
    /// What this caller may connect to a database as. Not feature-gated on any driver: the
    /// *carrier* costs nothing in a build with no database support, and gating it would mean the
    /// plane's context assembly changed shape with the OSS crate's feature flags.
    db_credentials: Option<Arc<DbCredentials>>,
}

impl RequestContext {
    /// No deadline, no canceller, no identity — the honest shape of a one-shot CLI invocation
    /// or a test. Named rather than `new()` so a caller that *should* pass a bounded context has
    /// to type something that admits it is not.
    pub fn detached() -> Self {
        Self::default()
    }

    /// Stop after `after` from now.
    pub fn with_timeout(mut self, after: Duration) -> Self {
        self.deadline = Some(Instant::now() + after);
        self
    }

    /// Stop at an absolute instant. Prefer this when several calls share one budget — computing
    /// the deadline once means the budget is for the whole request, not per call.
    pub fn with_deadline(mut self, at: Instant) -> Self {
        self.deadline = Some(at);
        self
    }

    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Who this work is attributed to. Carried for audit and metering; nothing in the OSS crate
    /// makes an authorization decision from it, and nothing should — the plane authorizes before
    /// it calls an engine, and a second check here would be a second place to get it wrong.
    pub fn with_tenant(mut self, tenant: impl Into<Arc<str>>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<Arc<str>>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Read object storage as `options` says, rather than as the engine's own default says.
    ///
    /// This is the per-caller half of identity — the tenant *label* is
    /// [`with_tenant`](Self::with_tenant), this is the credential it resolves to. Setting it is
    /// how one warm engine serves many tenants without any of them being able to read as another.
    ///
    /// It **overrides**, rather than merges with, the engine's default identity: merging two
    /// credential configurations would produce a third principal that neither the caller nor the
    /// operator asked to read as.
    #[cfg(feature = "object-store")]
    pub fn with_store_options(mut self, options: crate::objstore::StoreOptions) -> Self {
        self.store_options = Some(Arc::new(options));
        self
    }

    /// What this caller may read object storage as, if the caller said.
    ///
    /// `None` does **not** mean "read as the process" — it means the caller did not say, and the
    /// engine decides what that implies. An engine built for a single operator falls back to its
    /// own default (the environment, for a CLI); an engine serving many tenants is built with no
    /// default and refuses instead, so a dropped identity fails the read rather than performing
    /// it as the plane. See `LocalReaderEngine::without_ambient_identity`.
    #[cfg(feature = "object-store")]
    pub fn store_options(&self) -> Option<&crate::objstore::StoreOptions> {
        self.store_options.as_deref()
    }

    /// This caller's object-store identity, falling back to the process environment.
    ///
    /// For the handful of reads that sit **outside** the [`Engine`](crate::engine::Engine) seam and
    /// so have no engine default to fall back to — `Source::detect`'s remote probe, `serve`'s
    /// `HEAD`-for-size. An engine must not use this: an engine resolves against *its own*
    /// configured default, which for a multi-tenant one is deliberately absent so that a missing
    /// identity is a refusal rather than a read as the server.
    ///
    /// Named for what it does, because the fallback is the dangerous half and a caller choosing it
    /// should have to type it. `store_options()` returning `None` is the honest report; this is a
    /// policy applied on top of it.
    #[cfg(feature = "object-store")]
    pub fn store_options_or_env(&self) -> std::borrow::Cow<'_, crate::objstore::StoreOptions> {
        match self.store_options() {
            Some(vended) => std::borrow::Cow::Borrowed(vended),
            None => std::borrow::Cow::Owned(crate::objstore::StoreOptions::from_env()),
        }
    }

    /// Connect to a database as `credentials` says, rather than as the connection URI says.
    ///
    /// The database counterpart of [`with_store_options`](Self::with_store_options), with the same
    /// override-not-merge rule. Setting this is how a location stops needing to carry a secret:
    /// the URI names a host and a database, the identity arrives per call.
    pub fn with_database_credentials(mut self, credentials: DbCredentials) -> Self {
        self.db_credentials = Some(Arc::new(credentials));
        self
    }

    /// What this caller may connect to a database as, if the caller said.
    ///
    /// `None` means the caller did not say, and — exactly as with
    /// [`store_options`](Self::store_options) — the engine decides what that implies. The database
    /// engine falls back to whatever userinfo the URI itself carries, which is the CLI's case and
    /// the only case that worked before this existed.
    pub fn database_credentials(&self) -> Option<&DbCredentials> {
        self.db_credentials.as_deref()
    }

    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Is anything watching this call? `false` means [`check`](Self::check) can never fail, which
    /// lets a caller skip setting up machinery (a poll timer, a select) it would not use.
    pub fn is_bounded(&self) -> bool {
        self.cancel.is_some() || self.deadline.is_some()
    }

    /// Time left, or `None` when there is no deadline. `Some(Duration::ZERO)` once it has passed.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }

    /// Has this call been cancelled, or has its deadline passed?
    pub fn is_cancelled(&self) -> bool {
        self.cancel_reason().is_some()
    }

    /// Why this call should stop, if it should.
    ///
    /// Cancellation is reported before the deadline: if a caller explicitly asked to stop, that
    /// is the more specific fact, and it stays true even on a call that also happened to run out
    /// of time.
    pub fn cancel_reason(&self) -> Option<CancelReason> {
        if !self.is_bounded() {
            return None;
        }
        if self.cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
            return Some(CancelReason::Requested);
        }
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Some(CancelReason::Deadline);
        }
        None
    }

    /// The hot-loop call: `Ok(())` to keep going, [`EngineError::Cancelled`] to stop.
    ///
    /// Call it at boundaries a reader already has — per file, per record batch — and never per
    /// row. On a [`detached`](Self::detached) context it is one branch: no clock read, no atomic
    /// load. On a bounded one it costs an atomic load plus, at most, one `Instant::now()`.
    pub fn check(&self) -> Result<()> {
        match self.cancel_reason() {
            None => Ok(()),
            Some(reason) => Err(EngineError::Cancelled(reason)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_detached_context_never_stops() {
        let ctx = RequestContext::detached();
        assert!(!ctx.is_bounded());
        assert!(!ctx.is_cancelled());
        assert!(ctx.check().is_ok());
        assert_eq!(ctx.remaining(), None);
        assert_eq!(ctx.cancel_reason(), None);
    }

    #[test]
    fn cancelling_the_token_stops_every_holder() {
        let token = CancelToken::new();
        let ctx = RequestContext::detached().with_cancel(token.clone());
        assert!(ctx.check().is_ok());

        token.cancel();

        assert!(ctx.is_cancelled());
        assert_eq!(ctx.cancel_reason(), Some(CancelReason::Requested));
        assert!(matches!(
            ctx.check(),
            Err(EngineError::Cancelled(CancelReason::Requested))
        ));
        // Idempotent.
        token.cancel();
        assert!(ctx.is_cancelled());
    }

    #[test]
    fn a_passed_deadline_stops_the_call() {
        let ctx = RequestContext::detached().with_deadline(Instant::now() - Duration::from_secs(1));
        assert!(ctx.is_bounded());
        assert_eq!(ctx.remaining(), Some(Duration::ZERO));
        assert!(matches!(
            ctx.check(),
            Err(EngineError::Cancelled(CancelReason::Deadline))
        ));
    }

    #[test]
    fn a_future_deadline_does_not() {
        let ctx = RequestContext::detached().with_timeout(Duration::from_secs(60));
        assert!(ctx.check().is_ok());
        assert!(ctx.remaining().expect("has a deadline") > Duration::from_secs(30));
    }

    /// An explicit cancel is the more specific fact, so it wins over a deadline that also passed.
    #[test]
    fn an_explicit_cancel_is_reported_over_an_expired_deadline() {
        let token = CancelToken::new();
        token.cancel();
        let ctx = RequestContext::detached()
            .with_cancel(token)
            .with_deadline(Instant::now() - Duration::from_secs(1));
        assert_eq!(ctx.cancel_reason(), Some(CancelReason::Requested));
    }

    #[test]
    fn attribution_round_trips_and_is_absent_by_default() {
        let ctx = RequestContext::detached();
        assert_eq!(ctx.tenant(), None);
        assert_eq!(ctx.trace_id(), None);

        let ctx = ctx.with_tenant("acme").with_trace_id("run_01");
        assert_eq!(ctx.tenant(), Some("acme"));
        assert_eq!(ctx.trace_id(), Some("run_01"));

        // Cloning shares, and attribution alone does not bound the call.
        let clone = ctx.clone();
        assert_eq!(clone.tenant(), Some("acme"));
        assert!(!clone.is_bounded());
        assert!(clone.check().is_ok());
    }

    /// Identity is absent by default, survives a clone, and does not by itself bound the call —
    /// a context that says who you are still says nothing about how long you may take.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_store_identity_round_trips_and_is_absent_by_default() {
        use crate::objstore::StoreOptions;

        let ctx = RequestContext::detached();
        assert!(ctx.store_options().is_none());

        let ctx = ctx.with_store_options(StoreOptions::empty().with_scope("tenant-a"));
        assert_eq!(ctx.store_options().unwrap().scope_id(), "tenant-a");

        let clone = ctx.clone();
        assert_eq!(clone.store_options().unwrap().scope_id(), "tenant-a");
        assert!(!clone.is_bounded());
        assert!(clone.check().is_ok());
    }

    /// Setting it twice replaces rather than accumulates. The engines document identity as an
    /// override and not a merge; the carrier has to agree, or "last writer wins" would silently
    /// stop being true for a context assembled in two places.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_later_store_identity_replaces_an_earlier_one() {
        use crate::objstore::StoreOptions;

        let ctx = RequestContext::detached()
            .with_store_options(StoreOptions::empty().with_scope("tenant-a"))
            .with_store_options(StoreOptions::empty().with_scope("tenant-b"));
        assert_eq!(ctx.store_options().unwrap().scope_id(), "tenant-b");
    }

    /// The tenant label and the credential are independent halves of identity: carrying one never
    /// implies the other. A context with a tenant but no credential is exactly what an engine with
    /// no ambient default must refuse, so this must not quietly acquire one.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_tenant_label_does_not_imply_a_credential() {
        let ctx = RequestContext::detached().with_tenant("acme");
        assert_eq!(ctx.tenant(), Some("acme"));
        assert!(ctx.store_options().is_none());
    }

    /// A shared absolute deadline means the budget belongs to the request, not to each call.
    #[test]
    fn an_absolute_deadline_is_shared_across_calls() {
        let at = Instant::now() + Duration::from_secs(30);
        let a = RequestContext::detached().with_deadline(at);
        let b = RequestContext::detached().with_deadline(at);
        assert_eq!(a.deadline(), b.deadline());
    }

    /// A context is the sort of value that lands in a tracing span, a panic message or a
    /// `dbg!` — so its `Debug` must not be the thing that publishes the credential the rest of
    /// this change took off the `Source`.
    #[test]
    fn debug_prints_the_account_but_never_the_password() {
        let creds = DbCredentials::new("alice", "hunter2");
        let shown = format!("{creds:?}");
        assert!(!shown.contains("hunter2"), "leaked: {shown}");
        assert!(shown.contains("alice"), "unhelpful: {shown}");
        assert!(
            shown.contains("***"),
            "should say a password is set: {shown}"
        );

        // Through the whole context, which is what actually gets logged.
        let ctx = RequestContext::detached()
            .with_tenant("acme")
            .with_database_credentials(creds);
        let shown = format!("{ctx:?}");
        assert!(
            !shown.contains("hunter2"),
            "leaked via the context: {shown}"
        );
    }

    #[test]
    fn database_credentials_override_rather_than_accumulate() {
        let ctx = RequestContext::detached()
            .with_database_credentials(DbCredentials::new("first", "a"))
            .with_database_credentials(DbCredentials::new("second", "b"));
        assert_eq!(
            ctx.database_credentials().unwrap().username(),
            Some("second")
        );
        assert_eq!(ctx.database_credentials().unwrap().password(), Some("b"));
    }

    /// `None` is "the caller did not say", which the database engine reads as "connect as the URI
    /// says". An empty credential set means the same thing and must not be mistaken for
    /// "connect anonymously" — see `parse_db_uri_as`.
    #[test]
    fn an_absent_database_identity_is_distinguishable_from_an_empty_one() {
        assert!(RequestContext::detached().database_credentials().is_none());
        let empty = RequestContext::detached().with_database_credentials(DbCredentials::default());
        assert!(empty.database_credentials().is_some_and(|c| c.is_empty()));
    }
}
