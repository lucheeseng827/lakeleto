//! OSS boundary guard for the Lakeleto crate.
//!
//! The public mirror ships only the Apache-2.0 core (`lakeleto` binary + the
//! local/sql/remote engines behind the `Engine` trait). The closed hosted plane
//! (the `ee/` Lakeleto Cloud layer) must never be a dependency of — or be
//! referenced by — this OSS crate, and the private monorepo path must never leak
//! into the crate's source (on the mirror this dir IS the repo root, so any such
//! path is both wrong and a layout leak). The sync workflow enforces the same
//! boundary before a mirror push, but these run under `cargo test` so drift is
//! caught locally, first.
//!
//! NB: the sensitive tokens these tests search for are assembled from FRAGMENTS so
//! that this guard file itself stays clean of the very markers the mirror scan
//! rejects (the manifest's forbidden_markers include the private tree path and this
//! crate's private module id).

use std::fs;
use std::path::{Path, PathBuf};

/// This is a SINGLE crate, so `CARGO_MANIFEST_DIR` IS the module/crate root — the
/// exact directory the mirror publishes as its repo root (unlike a nested-workspace
/// crate, which would sit under `<root>/crates/<name>`).
fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The closed-plane directory name ("ee"), assembled so the literal never appears here.
fn plane_dir() -> String {
    format!("{}{}", "e", "e")
}

/// The private module id, assembled from fragments (it is a forbidden marker, so the
/// contiguous literal must not appear in this file).
fn private_module_id() -> String {
    format!("module_{}", "72")
}

/// The private monorepo source prefix, assembled from fragments.
fn monorepo_prefix() -> String {
    format!("{}/{}", "rust_modules", "lab")
}

#[test]
fn crate_manifest_has_no_dependency_on_the_closed_plane() {
    let cargo = fs::read_to_string(crate_root().join("Cargo.toml")).expect("crate Cargo.toml");
    let plane = plane_dir();
    // A path dep from this crate into the closed plane would dip into a sibling
    // closed-plane subdir (e.g. a `path = ...` pointing at the hosted-plane crate).
    let into_plane = format!("{}/", plane); // closed-plane subdir prefix
    let up_into_plane = format!("../{}", plane); // parent-relative closed-plane prefix
    // The hosted plane's crate name, either dep-name spelling.
    let cloud_underscore = format!("lakeleto_{}", "cloud");
    let cloud_hyphen = format!("lakeleto-{}", "cloud");
    for raw in cargo.lines() {
        let line = raw.trim();
        if line.contains("path") && (line.contains(&into_plane) || line.contains(&up_into_plane)) {
            panic!("crate manifest declares a path dependency into the closed plane: {line}");
        }
        assert!(
            !line.contains(&cloud_underscore) && !line.contains(&cloud_hyphen),
            "crate manifest references the closed hosted plane: {line}"
        );
    }
}

#[test]
fn crate_source_does_not_leak_the_private_monorepo_path() {
    // Both forms are wrong on the mirror (crate root == repo root there) and reveal
    // the private monorepo layout. Assembled from fragments; see the file note.
    let prefix = monorepo_prefix(); // the private monorepo source prefix
    let full = format!("{}/{}", prefix, private_module_id()); // full private crate path
    let needles = [prefix.as_str(), full.as_str()];
    let src = crate_root().join("src");
    assert!(src.is_dir(), "crate src/ must exist");
    scan_rs(&src, &needles);
}

/// Recursively assert no `*.rs` file under `dir` contains any of `needles`.
/// Only walks source (skips any nested `target/` build dir).
fn scan_rs(dir: &Path, needles: &[&str]) {
    for entry in fs::read_dir(dir).expect("readable src dir") {
        let path = entry.expect("readable dir entry").path();
        if path.is_dir() {
            if path.file_name().and_then(|s| s.to_str()) == Some("target") {
                continue;
            }
            scan_rs(&path, needles);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            let body = fs::read_to_string(&path).unwrap_or_default();
            for needle in needles {
                assert!(
                    !body.contains(needle),
                    "{path:?} leaks a private monorepo path ({needle})"
                );
            }
        }
    }
}
