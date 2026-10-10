//! The version of the HTTP contract `lakeleto serve` speaks, and how a client reads it.
//!
//! The contract is versioned in two parts, `major.minor`:
//!
//! - **The major is the path.** Every route is under `/v1`; a change that breaks a client would
//!   be served under `/v2`, beside `/v1`, rather than change what `/v1` means.
//! - **The minor counts additions.** It goes up when `/v1` gains an endpoint, a field or a
//!   parameter. Existing ones keep their meaning, so a client written against `1.0` works against
//!   any `1.x`, and a client ignores fields it doesn't know.
//!
//! A server says which version it speaks in two places: the `protocol` field of
//! `GET /v1/engines`, and the [`PROTOCOL_HEADER`] on every response, so a client learns it from
//! whatever request it made first. A server that sends neither predates versioning, and speaks a
//! subset of `1.0`.

/// The `/v1` contract this build speaks.
pub const PROTOCOL_VERSION: &str = "1.0";

/// The response header that carries [`PROTOCOL_VERSION`] (`X-Lakeleto-Protocol`). Lowercase,
/// because HTTP field names are case-insensitive and `HeaderName::from_static` takes them so.
pub const PROTOCOL_HEADER: &str = "x-lakeleto-protocol";

/// The major and minor parts of a protocol version, or `None` for one that isn't `major.minor`.
pub fn parse(version: &str) -> Option<(u32, u32)> {
    let (major, minor) = version.trim().split_once('.')?;
    Some((major.parse().ok()?, minor.parse().ok()?))
}

/// Does a server speaking `server` serve this build's major version, so the `/v1` routes mean
/// what this build expects?
pub fn compatible(server: &str) -> bool {
    match (parse(server), parse(PROTOCOL_VERSION)) {
        (Some((theirs, _)), Some((ours, _))) => theirs == ours,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_major_dot_minor_and_compatible_within_a_major() {
        assert_eq!(parse(PROTOCOL_VERSION), Some((1, 0)));
        assert_eq!(parse(" 1.7 "), Some((1, 7)));
        for bad in ["1", "1.x", "v1.0", "", "1.0.0"] {
            assert_eq!(parse(bad), None, "{bad}");
        }
        assert!(compatible("1.0"));
        assert!(compatible("1.9"), "a later minor only adds");
        assert!(!compatible("2.0"));
        assert!(!compatible("garbage"));
    }
}
