#!/usr/bin/env bash
# Build Lakeleto.app and wrap it in a drag-to-Applications .dmg.
#
#   ./build-dmg.sh 0.1.4 [target-triple]
#
# macOS only: it needs iconutil, hdiutil and codesign. Run it on a macOS runner,
# not in the Linux release container.
#
# Signing and notarization are opt-in via the environment, and skipped with a
# warning when unset, so an unsigned local build still produces a working .dmg:
#
#   MACOS_SIGN_IDENTITY   "Developer ID Application: Name (TEAMID)"
#   APPLE_ID              Apple ID for notarytool
#   APPLE_TEAM_ID         10-character team id
#   APPLE_APP_PASSWORD    app-specific password (NOT the account password)
#
# Notarization matters more here than signing does on Windows: Gatekeeper refuses
# an unnotarized download outright rather than offering a "run anyway", so an
# unsigned .dmg is a dead end for anyone who has not been told about the
# right-click-Open trick.
set -euo pipefail

VERSION="${1:?usage: build-dmg.sh <version> [target-triple]}"
TARGET="${2:-}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
module_root="$(cd "$here/../.." && pwd)"
repo_root="$(cd "$module_root/../../.." && pwd)"

if [[ -n "$TARGET" ]]; then
    release_dir="$repo_root/target/$TARGET/release"
else
    release_dir="$repo_root/target/release"
fi

app="$here/Lakeleto.app"
contents="$app/Contents"
dmg="$here/Lakeleto-$VERSION.dmg"

for exe in lakeleto lakeleto-desktop; do
    if [[ ! -x "$release_dir/$exe" ]]; then
        echo "missing $release_dir/$exe - build the release binaries first" >&2
        exit 1
    fi
done

echo "==> assembling $app"
rm -rf "$app" "$dmg"
mkdir -p "$contents/MacOS" "$contents/Resources"

# Both binaries go inside the bundle: the launcher is what LaunchServices runs,
# and shipping the CLI beside it means `Lakeleto.app/Contents/MacOS/lakeleto` is
# a real path a user can symlink into their PATH. Homebrew remains the supported
# route for the CLI on its own.
cp "$release_dir/lakeleto-desktop" "$contents/MacOS/"
cp "$release_dir/lakeleto" "$contents/MacOS/"
sed "s/@VERSION@/$VERSION/g" "$here/Info.plist" > "$contents/Info.plist"

echo "==> building the icon"
# The .iconset PNGs are generated, not committed - see packaging/.gitignore.
if [[ ! -d "$here/Lakeleto.iconset" ]]; then
    (cd "$repo_root" && cargo run --features serve --example gen_icons)
fi
iconutil -c icns "$here/Lakeleto.iconset" -o "$contents/Resources/Lakeleto.icns"

if [[ -n "${MACOS_SIGN_IDENTITY:-}" ]]; then
    echo "==> signing"
    # --options runtime enables the hardened runtime, which notarization requires.
    # Sign the nested binary before the bundle: codesign seals what it finds, so
    # signing the outside first would be invalidated by the inside changing.
    codesign --force --options runtime --timestamp \
        --sign "$MACOS_SIGN_IDENTITY" "$contents/MacOS/lakeleto"
    codesign --force --options runtime --timestamp \
        --sign "$MACOS_SIGN_IDENTITY" "$contents/MacOS/lakeleto-desktop"
    codesign --force --options runtime --timestamp \
        --sign "$MACOS_SIGN_IDENTITY" "$app"
    codesign --verify --deep --strict --verbose=2 "$app"
else
    echo "!! MACOS_SIGN_IDENTITY unset - building an UNSIGNED app." >&2
    echo "!! Gatekeeper will refuse it on any machine that did not build it." >&2
fi

echo "==> building $dmg"
staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT
cp -R "$app" "$staging/"
# The drag-to-install affordance: the .dmg window shows the app and a shortcut to
# /Applications, so "install" is one drag with nothing to read.
ln -s /Applications "$staging/Applications"
hdiutil create -volname "Lakeleto $VERSION" \
    -srcfolder "$staging" -ov -format UDZO "$dmg"

if [[ -n "${APPLE_ID:-}" && -n "${APPLE_TEAM_ID:-}" && -n "${APPLE_APP_PASSWORD:-}" ]]; then
    echo "==> notarizing (this waits on Apple, typically a few minutes)"
    xcrun notarytool submit "$dmg" \
        --apple-id "$APPLE_ID" \
        --team-id "$APPLE_TEAM_ID" \
        --password "$APPLE_APP_PASSWORD" \
        --wait
    # Stapling attaches the ticket to the .dmg so a first launch works offline;
    # without it Gatekeeper has to reach Apple, and a user on a plane is blocked.
    xcrun stapler staple "$dmg"
    xcrun stapler validate "$dmg"
else
    echo "!! Apple notarization credentials unset - .dmg is NOT notarized." >&2
fi

echo ""
echo "built $dmg"
