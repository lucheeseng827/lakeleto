//! Regenerate the installer icon art from the Strata geometry.
//!
//! ```sh
//! cargo run --features serve --example gen_icons
//! ```
//!
//! Writes into `packaging/`:
//!
//! * `windows/lakeleto.ico` — multi-size, for the Start Menu shortcut and the
//!   Add/Remove Programs entry.
//! * `macos/Lakeleto.iconset/*.png` — the sizes `iconutil` expects; the release
//!   workflow folds them into `Lakeleto.icns` on a macOS runner, since `iconutil`
//!   only exists there.
//!
//! The outputs are **not** committed — see `packaging/.gitignore`. They come to
//! ~7 MB, because the encoder in `src/icon.rs` writes stored (uncompressed)
//! deflate so the crate needs no compression dependency; that is a fine trade for
//! a build artifact and a bad one for git history. The installer jobs run this
//! example themselves, and the geometry in `src/desktop.rs` stays the source of
//! truth for the mark.

use std::path::Path;

use lakeleto::{desktop, icon};

/// What Windows Explorer picks between at various zoom levels.
const ICO_SIZES: [u32; 5] = [16, 32, 48, 128, 256];

/// The `.iconset` contract: `iconutil` matches on these exact filenames.
const ICNS_SIZES: [(u32, &str); 10] = [
    (16, "icon_16x16.png"),
    (32, "icon_16x16@2x.png"),
    (32, "icon_32x32.png"),
    (64, "icon_32x32@2x.png"),
    (128, "icon_128x128.png"),
    (256, "icon_128x128@2x.png"),
    (256, "icon_256x256.png"),
    (512, "icon_256x256@2x.png"),
    (512, "icon_512x512.png"),
    (1024, "icon_512x512@2x.png"),
];

fn main() -> std::io::Result<()> {
    // Run from the module dir regardless of where cargo was invoked.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("packaging");

    let windows = root.join("windows");
    std::fs::create_dir_all(&windows)?;
    let images: Vec<(u32, Vec<u8>)> =
        ICO_SIZES.iter().map(|&s| (s, desktop::strata_icon_at(s))).collect();
    let ico_path = windows.join("lakeleto.ico");
    std::fs::write(&ico_path, icon::ico(&images))?;
    println!("{} ({} sizes)", ico_path.display(), ICO_SIZES.len());

    let iconset = root.join("macos").join("Lakeleto.iconset");
    std::fs::create_dir_all(&iconset)?;
    for (size, name) in ICNS_SIZES {
        let png = icon::png(&desktop::strata_icon_at(size), size, size);
        std::fs::write(iconset.join(name), png)?;
    }
    println!("{} ({} sizes)", iconset.display(), ICNS_SIZES.len());
    Ok(())
}
