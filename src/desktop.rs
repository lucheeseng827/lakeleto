//! The double-click path — what `lakeleto serve` cannot assume.
//!
//! `serve` is a terminal program: the user chose the port, sees the bind error if
//! it is taken, reads the URL off stdout, and stops it with Ctrl-C. A launcher
//! started from a Start Menu entry or an `.app` bundle has none of that. Nobody
//! typed a port, nobody is reading stdout, and there is no terminal to Ctrl-C.
//! So the three decisions `serve` delegates to the operator have to be made here
//! instead:
//!
//! * **Which port.** One the OS says is free, never a well-known number. See
//!   [`free_port`] — a bind error is a stack trace to a double-clicker.
//! * **What if something is already there.** Double-clicking a desktop icon twice
//!   is normal and must not start a second server or fail. [`decide`] finds the
//!   running instance through [`port_file`] and the second launch just re-opens
//!   the browser — the behaviour every desktop app has.
//! * **Where the browser goes.** The SPA root, opened for you, because there is no
//!   stdout to print it to.
//!
//! The port logic is separated from the serving so it can be tested without
//! standing up a server.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How long the "is that a Lakeleto?" probe gets. Loopback, so this is generous;
/// it exists so a port held by something that accepts and never answers cannot
/// hang a double-click.
const PROBE_TIMEOUT: Duration = Duration::from_millis(700);

/// What the launcher should do about the port it found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Launch {
    /// Nothing is listening — bind here and serve.
    Serve(u16),
    /// A Lakeleto is already running here. Show it rather than starting a rival
    /// on another port, which would split the user's workspaces across two
    /// servers without telling them.
    Reuse(u16),
}

impl Launch {
    pub fn port(self) -> u16 {
        match self {
            Launch::Serve(p) | Launch::Reuse(p) => p,
        }
    }
}

/// Where the running launcher publishes its port, so the next double-click can
/// find it. Beside the workspace store, which is already this user's Lakeleto
/// state directory.
pub fn port_file() -> PathBuf {
    crate::workspace::default_home().join("desktop.port")
}

/// Decide where this launch should point.
///
/// Note the deliberate gap between this returning [`Launch::Serve`] and the
/// server actually binding: [`free_port`] closes its listener before the caller
/// opens one. Holding the socket and handing it over would close the race, but
/// [`crate::api::serve`] binds from an address string, and a launcher losing a
/// port race on loopback in that window is not a failure mode worth reshaping
/// the server's signature for. It surfaces as a bind error, which the caller
/// reports.
pub fn decide() -> Launch {
    if let Some(port) = running_instance() {
        return Launch::Reuse(port);
    }
    Launch::Serve(free_port())
}

/// The port of the Lakeleto this user already has running, if any.
///
/// The recorded port is *evidence*, not truth: the launcher can be killed without
/// clearing it, and the OS is free to hand that number to something else later.
/// So the file only says where to look, and the probe decides.
fn running_instance() -> Option<u16> {
    let recorded = std::fs::read_to_string(port_file()).ok()?;
    let port: u16 = recorded.trim().parse().ok()?;
    is_lakeleto(port).then_some(port)
}

/// Ask the OS for an unused port.
///
/// Deliberately *not* a well-known number. 8080 is one of the most contended
/// ports on a developer's machine — a spare Tomcat, a `python -m http.server`, a
/// colleague's dev server — and a launcher that wants it is a launcher that
/// regularly cannot have it. Binding to port 0 asks the kernel for one nothing is
/// using, which is both always available and, by construction, the least
/// contended choice on the machine. The listener is dropped immediately; the
/// number is what we wanted.
///
/// Falls back to 0 if even that fails, which lets [`crate::api::serve`] produce
/// the real bind error rather than this function inventing one.
pub fn free_port() -> u16 {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|a| a.port())
        .unwrap_or(0)
}

/// Publish the port for the next launch. Best-effort: failing to record it costs
/// a duplicate instance later, not this one.
pub fn record_port(port: u16) {
    let path = port_file();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, port.to_string());
}

/// Drop the record on a clean exit, so the next launch does not have to wait out
/// a probe against a port nothing is listening on.
pub fn clear_port() {
    let _ = std::fs::remove_file(port_file());
}

/// Is the thing on this port a Lakeleto? One `GET /v1/engines`, hand-rolled so
/// the launcher needs no HTTP client.
///
/// `/v1/engines` rather than `/healthz`: a bare `ok` is a body half the
/// dev-server processes on a laptop would also return, and a stale port file
/// pointed at a recycled port is exactly the case this has to get right. The
/// engines document is distinctive, and the launcher never sets a token, so its
/// own instance always answers it.
fn is_lakeleto(port: u16) -> bool {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, PROBE_TIMEOUT) else {
        return false;
    };
    let _ = stream.set_write_timeout(Some(PROBE_TIMEOUT));
    let request =
        format!("GET /v1/engines HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    // Bounded read: a hostile or broken peer must not stream into our buffer
    // forever. The signal we need is at the front of the body.
    //
    // One deadline for the whole read, re-applied as a shrinking timeout. A
    // socket read timeout is per-call, so setting it once lets a peer that
    // trickles a byte at a time reset the clock on every iteration and hold this
    // open for up to `buf.len()` timeouts. That matters more since the port comes
    // from a file: the record can be stale and the number recycled to anything,
    // and this runs in the launch path before there is any window to explain a
    // stall.
    let deadline = std::time::Instant::now() + PROBE_TIMEOUT;
    let mut buf = [0u8; 1024];
    let mut filled = 0;
    while filled < buf.len() {
        // A zero-duration timeout means "block forever" to the OS, so treat an
        // exhausted budget as done rather than handing it over.
        let Some(remaining) = deadline
            .checked_duration_since(std::time::Instant::now())
            .filter(|d| !d.is_zero())
        else {
            break;
        };
        if stream.set_read_timeout(Some(remaining)).is_err() {
            break;
        }
        match stream.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(_) => break,
        }
    }
    let response = String::from_utf8_lossy(&buf[..filled]);
    let Some((head, body)) = response.split_once("\r\n\r\n") else {
        return false;
    };
    head.starts_with("HTTP/1.1 200") && body.contains("\"engine\"")
}

/// The SPA URL for a port.
pub fn url(port: u16) -> String {
    format!("http://127.0.0.1:{port}/")
}

/// Serve until the process ends. Blocks.
///
/// `root` confines `/v1/*` the way `serve --root` does. The launcher leaves it
/// `None`: a desktop user browsing their own machine is the whole point, and the
/// server is on loopback with no token, exactly like `lakeleto open`.
pub fn serve_at(port: u16, root: Option<PathBuf>) -> crate::Result<()> {
    let addr = format!("127.0.0.1:{port}");
    let read: std::sync::Arc<dyn crate::Engine> =
        std::sync::Arc::new(crate::LocalReaderEngine::default());
    crate::api::serve(
        &addr,
        read,
        crate::cli::sql_engine_arc(),
        crate::cli::db_engine_arc(),
        crate::cli::DEFAULT_SCAN,
        Some(url(port)),
        None,
        root,
        None,
    )
}

/// Best-effort browser launch, shared with `lakeleto open`.
pub fn open_browser(url: &str) {
    crate::api::open_browser(url);
}

// ---------------------------------------------------------------------------
// "Install command line tool" — the macOS half of `lakeleto` on PATH.
//
// The Windows installer puts the install directory on the user's PATH, so
// `lakeleto schema x.parquet` works in a new terminal and nothing here is
// needed. A macOS `.app` cannot do that: the bundle is dragged into place with
// no install step to run, and its binaries live at
// `Lakeleto.app/Contents/MacOS/`, which is on nobody's PATH.
//
// So the CLI ships inside the bundle and this links it out on request, the way
// VS Code's "Install 'code' command in PATH" does. From a menu item rather than
// on first launch: symlinking into a shared bin directory behind the user's
// back is not something a table viewer should do because it happened to start.
// ---------------------------------------------------------------------------

/// The CLI binary that shipped beside this launcher.
///
/// Pure so it can be tested off-macOS: inside the bundle both binaries sit in
/// `Contents/MacOS/`, so the CLI is always the launcher's sibling — which also
/// means this keeps working if the user moves or renames the `.app`.
pub fn cli_beside(launcher: &Path) -> PathBuf {
    let name = if cfg!(windows) {
        "lakeleto.exe"
    } else {
        "lakeleto"
    };
    launcher.parent().unwrap_or(Path::new(".")).join(name)
}

/// What is already sitting at the link path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkState {
    /// Nothing there.
    Absent,
    /// A symlink already pointing at the binary we would link.
    Ours,
    /// A symlink that resolves to nothing — the `Lakeleto.app` it pointed at was
    /// moved or deleted. Nobody is being served by it, so it is ours to replace.
    StaleLink,
    /// A symlink pointing somewhere that still resolves. Someone else made it and
    /// it is doing its job.
    ///
    /// This is the Homebrew case, and the reason it needs its own state:
    /// `brew install lakeleto` does not put a *file* in `/usr/local/bin`, it puts
    /// a **symlink** to `../Cellar/lakeleto/<version>/bin/lakeleto`. Folding that
    /// in with [`LinkState::StaleLink`] would delete the very install the
    /// real-file check below was written to protect.
    ForeignLink,
    /// A real file — a directly installed `lakeleto`, or anything else wearing
    /// the name.
    RealFile,
}

/// What to do about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkAction {
    Create,
    Replace,
    NothingToDo,
    /// Refuse and say why — never silently replace something we did not create.
    Refuse,
}

/// The rule, separated from the filesystem so it is testable on any platform.
///
/// A stale symlink is replaced because it is ours and it is broken; a *real
/// file* is not, because it is almost certainly the Homebrew install and
/// clobbering someone's package manager is not this menu item's business.
pub fn decide_link(state: &LinkState) -> LinkAction {
    match state {
        LinkState::Absent => LinkAction::Create,
        LinkState::Ours => LinkAction::NothingToDo,
        LinkState::StaleLink => LinkAction::Replace,
        LinkState::ForeignLink | LinkState::RealFile => LinkAction::Refuse,
    }
}

/// Inspect a candidate link path.
pub fn link_state(link: &Path, want: &Path) -> LinkState {
    // `symlink_metadata` does not follow the link, so a *dangling* symlink is
    // still seen — which `Path::exists` would report as absent, and we would
    // then fail to create over it.
    let Ok(meta) = std::fs::symlink_metadata(link) else {
        return LinkState::Absent;
    };
    if !meta.file_type().is_symlink() {
        return LinkState::RealFile;
    }
    match std::fs::read_link(link) {
        Ok(target) if target == want => LinkState::Ours,
        // `exists` follows the link, and the kernel resolves a relative target
        // against the link's own directory — which is the form Homebrew writes.
        // Resolving means someone's working command lives here; only a link that
        // resolves to nothing is safe to take over.
        Ok(_) if link.exists() => LinkState::ForeignLink,
        Ok(_) => LinkState::StaleLink,
        // Unreadable target: ownership cannot be established, so do not claim it.
        Err(_) => LinkState::ForeignLink,
    }
}

/// Where to try linking, best first.
///
/// `/usr/local/bin` is on the default macOS `PATH` (it is in `/etc/paths`), so
/// a link there works in a new shell with no profile editing. It is root-owned
/// unless something like Homebrew has taken it, hence the fallback:
/// `~/.local/bin` always works without privileges but is *not* on the default
/// macOS PATH, so the caller has to say so.
pub fn cli_link_dirs() -> Vec<PathBuf> {
    let mut dirs = vec![PathBuf::from("/usr/local/bin")];
    if let Some(home) = std::env::var_os("HOME") {
        dirs.push(PathBuf::from(home).join(".local").join("bin"));
    }
    dirs
}

/// The outcome, in the words the user needs to see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliInstall {
    Linked {
        link: PathBuf,
        on_default_path: bool,
    },
    AlreadyLinked(PathBuf),
    Occupied(PathBuf),
    Failed(String),
}

impl CliInstall {
    /// A message fit for a dialog: what happened, and what to do next.
    pub fn message(&self) -> String {
        match self {
            CliInstall::Linked {
                link,
                on_default_path: true,
            } => format!(
                "The lakeleto command is now available at {}.\n\n\
                 Open a new terminal and run: lakeleto --help",
                link.display()
            ),
            CliInstall::Linked {
                link,
                on_default_path: false,
            } => format!(
                "The lakeleto command was linked to {}.\n\n\
                 That directory is not on the default PATH, so add this to your shell profile:\n\
                 export PATH=\"$HOME/.local/bin:$PATH\"",
                link.display()
            ),
            CliInstall::AlreadyLinked(link) => {
                format!(
                    "The lakeleto command is already installed at {}.",
                    link.display()
                )
            }
            CliInstall::Occupied(link) => format!(
                "{} already exists and is not a link created by Lakeleto — it is most \
                 likely a Homebrew install.\n\nIt has been left alone. Remove it first if you \
                 want this copy on your PATH instead.",
                link.display()
            ),
            CliInstall::Failed(why) => format!("Could not install the lakeleto command.\n\n{why}"),
        }
    }
}

/// Link the bundled CLI onto the user's PATH.
///
/// Tries each of [`cli_link_dirs`] in turn, so a locked-down `/usr/local/bin`
/// falls through to the home directory rather than failing outright.
pub fn install_cli() -> CliInstall {
    let launcher = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => return CliInstall::Failed(format!("locating this application: {e}")),
    };
    let source = cli_beside(&launcher);
    if !source.is_file() {
        return CliInstall::Failed(format!(
            "the lakeleto command is missing from this application ({} not found)",
            source.display()
        ));
    }

    let mut last_error = None;
    for dir in cli_link_dirs() {
        let on_default_path = dir == Path::new("/usr/local/bin");
        let link = dir.join("lakeleto");

        match decide_link(&link_state(&link, &source)) {
            LinkAction::NothingToDo => return CliInstall::AlreadyLinked(link),
            // Only refuse for the *preferred* directory; a Homebrew binary in
            // /usr/local/bin is already on PATH and doing the job, so there is
            // nothing to fall through for.
            LinkAction::Refuse => return CliInstall::Occupied(link),
            LinkAction::Replace => {
                if let Err(e) = std::fs::remove_file(&link) {
                    last_error = Some(format!("{}: {e}", link.display()));
                    continue;
                }
            }
            LinkAction::Create => {}
        }

        // `~/.local/bin` routinely does not exist yet; `/usr/local/bin` does.
        if !dir.is_dir() && std::fs::create_dir_all(&dir).is_err() {
            last_error = Some(format!(
                "{} does not exist and cannot be created",
                dir.display()
            ));
            continue;
        }

        match make_symlink(&source, &link) {
            Ok(()) => {
                return CliInstall::Linked {
                    link,
                    on_default_path,
                }
            }
            Err(e) => last_error = Some(format!("{}: {e}", link.display())),
        }
    }

    CliInstall::Failed(last_error.unwrap_or_else(|| "no writable directory on PATH".to_string()))
}

/// The one genuinely platform-specific call, kept tiny so everything around it
/// still compiles and is tested on Windows.
fn make_symlink(source: &Path, link: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source, link)
    }
    #[cfg(not(unix))]
    {
        let _ = (source, link);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the CLI symlink is a macOS affordance; on Windows the installer sets PATH",
        ))
    }
}

/// The CLI shipped with this launcher, for showing or copying.
pub fn cli_path() -> Option<PathBuf> {
    let launcher = std::env::current_exe().ok()?;
    let cli = cli_beside(&launcher);
    cli.is_file().then_some(cli)
}

/// Put text on the system clipboard without a clipboard dependency.
///
/// Every desktop platform ships a command that reads the clipboard from stdin,
/// so this is a pipe rather than a crate. Wayland and X11 differ, hence two
/// candidates on Linux — the first that runs wins.
pub fn copy_to_clipboard(text: &str) -> Result<(), String> {
    let candidates: &[(&str, &[&str])] = if cfg!(target_os = "macos") {
        &[("pbcopy", &[])]
    } else if cfg!(windows) {
        &[("clip", &[])]
    } else {
        &[("wl-copy", &[]), ("xclip", &["-selection", "clipboard"])]
    };

    let mut last = String::from("no clipboard command available");
    for (cmd, args) in candidates {
        match write_to_stdin(cmd, args, text) {
            Ok(()) => return Ok(()),
            Err(e) => last = format!("{cmd}: {e}"),
        }
    }
    Err(last)
}

fn write_to_stdin(cmd: &str, args: &[&str], text: &str) -> std::io::Result<()> {
    use std::process::{Command, Stdio};
    let mut child = Command::new(cmd)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    // Take the handle so it drops (closing the pipe) before the wait below;
    // `clip` and `pbcopy` both read until EOF and would otherwise never exit.
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("clipboard command took no stdin"))?;
        stdin.write_all(text.as_bytes())?;
    }
    let status = child.wait()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("exited with {status}")))
    }
}

/// Tell the user what happened, from a process with no window of its own.
///
/// Both branches shell out rather than link a UI toolkit: `osascript` is how a
/// bundled macOS app puts a dialog on screen, and PowerShell can raise a
/// `MessageBox` on Windows. Spawned, never waited on, so a menu click never
/// leaves the tray looking wedged.
pub fn notify(body: &str) {
    #[cfg(target_os = "macos")]
    {
        let escaped = body.replace('\\', "\\\\").replace('"', "\\\"");
        let script = format!(
            "display dialog \"{escaped}\" with title \"Lakeleto\" buttons {{\"OK\"}} \
             default button \"OK\" with icon note"
        );
        let _ = std::process::Command::new("osascript")
            .arg("-e")
            .arg(script)
            .spawn();
    }
    #[cfg(windows)]
    {
        // Single-quoted PowerShell string: only `'` needs escaping, and doubling
        // it is the escape. This keeps a Windows path's backslashes literal.
        let escaped = body.replace('\'', "''");
        let script = format!(
            "Add-Type -AssemblyName PresentationFramework; \
             [System.Windows.MessageBox]::Show('{escaped}', 'Lakeleto') | Out-Null"
        );
        let _ = std::process::Command::new("powershell")
            .args(["-NoProfile", "-WindowStyle", "Hidden", "-Command", &script])
            .spawn();
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        eprintln!("lakeleto-desktop: {body}");
    }
}

/// Where a windowless launcher leaves evidence: `$LAKELETO_HOME/launcher.log`,
/// beside the workspace store it already writes to.
///
/// A console binary reports a bind failure to the terminal that started it. This
/// one has no terminal, so an unwritten error is an error nobody can ever see —
/// "I clicked the icon and nothing happened", with nothing to go on.
pub fn log_path() -> PathBuf {
    crate::workspace::default_home().join("launcher.log")
}

/// Append a fatal error to [`log_path`], and to stderr in case one exists.
///
/// Deliberately infallible: this runs on the path where something has already
/// gone wrong, and a launcher that panics while reporting a panic tells the user
/// even less than one that stays quiet.
pub fn report_fatal(message: &str) {
    eprintln!("lakeleto-desktop: {message}");
    let path = log_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        // No timestamp crate in this dependency set, and pulling one in for a log
        // line is not worth it; the version plus the message is enough to act on.
        let _ = writeln!(
            file,
            "[lakeleto-desktop {}] {message}",
            env!("CARGO_PKG_VERSION")
        );
    }
}

/// Route panics to [`report_fatal`]. In a windowless process the default hook
/// writes to a stderr nobody is attached to, so an unwrap failure looks exactly
/// like the program never starting.
pub fn install_panic_logger() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        report_fatal(&format!("panic: {info}"));
        previous(info);
    }));
}

/// The Strata mark as raw RGBA, rendered rather than embedded.
///
/// Returns `(pixels, size)` for a square `size × size` image. It is drawn in code
/// so the launcher carries no binary asset and no image decoder: the mark is four
/// rectangles on a rounded gradient tile, which is less code than a PNG loader.
/// Coordinates follow `lakeleto-site/assets/favicon.svg` on its 64-unit viewBox,
/// so the tray icon and the site's favicon stay the same drawing.
pub fn strata_icon() -> (Vec<u8>, u32) {
    const SIZE: u32 = 64;
    (strata_icon_at(SIZE), SIZE)
}

/// The mark at an arbitrary size, for the installer icon sets — Windows `.ico`
/// wants 16/32/48/256, macOS `.icns` wants 16 through 1024. Rendering each size
/// from the geometry beats scaling one bitmap, which is what makes the 16px tray
/// and menu-bar versions legible.
pub fn strata_icon_at(size: u32) -> Vec<u8> {
    // 4×4 sub-samples per pixel. The mark is all rounded corners and thin bands;
    // without coverage sampling both alias badly at the 16–24px the OS actually
    // draws a tray icon at.
    const SS: u32 = 4;

    let mut pixels = Vec::with_capacity((size * size * 4) as usize);
    for y in 0..size {
        for x in 0..size {
            let (mut r, mut g, mut b, mut a) = (0f32, 0f32, 0f32, 0f32);
            for sy in 0..SS {
                for sx in 0..SS {
                    let fx = x as f32 + (sx as f32 + 0.5) / SS as f32;
                    let fy = y as f32 + (sy as f32 + 0.5) / SS as f32;
                    let (sr, sg, sb, sa) = sample(fx, fy, size as f32);
                    r += sr;
                    g += sg;
                    b += sb;
                    a += sa;
                }
            }
            let n = (SS * SS) as f32;
            // Colour is averaged over the *covered* sub-samples, not all of them.
            // The buffer is non-premultiplied, so dividing colour by `n` would
            // blend the tile's colour with the transparent black returned outside
            // it — an edge pixel would keep partial alpha but store a darkened
            // colour, and the compositor would draw a dark fringe around every
            // rounded corner. That fringe is worst at the 16–24px the tray and
            // Start Menu use, which is what the sub-sampling is here to protect.
            // `sample` returns alpha 255 inside and 0 outside, so `a / 255` is
            // the number of covered sub-samples.
            let covered = a / 255.0;
            let (r, g, b) = if covered > 0.0 {
                (r / covered, g / covered, b / covered)
            } else {
                (0.0, 0.0, 0.0)
            };
            pixels.push(r.round() as u8);
            pixels.push(g.round() as u8);
            pixels.push(b.round() as u8);
            pixels.push((a / n).round() as u8);
        }
    }
    pixels
}

/// One sub-sample of the mark, in the SVG's 64-unit space.
fn sample(x: f32, y: f32, size: f32) -> (f32, f32, f32, f32) {
    let u = x * 64.0 / size;
    let v = y * 64.0 / size;

    // Tile: rect(6,6,52,52) rx=14.
    if !in_rounded_rect(u, v, 6.0, 6.0, 52.0, 52.0, 14.0) {
        return (0.0, 0.0, 0.0, 0.0);
    }
    // Gradient #2563eb -> #22d3ee along the tile diagonal, as in the SVG.
    let t = (((u - 6.0) + (v - 6.0)) / 104.0).clamp(0.0, 1.0);
    let (mut r, mut g, mut b) = (
        lerp(0x25 as f32, 0x22 as f32, t),
        lerp(0x63 as f32, 0xd3 as f32, t),
        lerp(0xeb as f32, 0xee as f32, t),
    );

    // Strata: bands of varying width — sediment layers, or rows in a table.
    const BANDS: [(f32, f32, f32); 4] = [
        (14.5, 34.0, 1.00),
        (24.5, 22.0, 0.92),
        (34.5, 29.0, 0.84),
        (44.5, 14.0, 0.76),
    ];
    for (band_y, width, alpha) in BANDS {
        if in_rounded_rect(u, v, 15.0, band_y, width, 5.0, 2.5) {
            r = lerp(r, 255.0, alpha);
            g = lerp(g, 255.0, alpha);
            b = lerp(b, 255.0, alpha);
            break;
        }
    }
    (r, g, b, 255.0)
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a + (b - a) * t
}

/// Point-in-rounded-rectangle, corners only tested where they actually curve.
fn in_rounded_rect(x: f32, y: f32, rx: f32, ry: f32, w: f32, h: f32, radius: f32) -> bool {
    if x < rx || y < ry || x > rx + w || y > ry + h {
        return false;
    }
    let radius = radius.min(w / 2.0).min(h / 2.0);
    // Nearest corner centre; inside the straight edges these clamp to the point
    // itself, so the distance test passes trivially.
    let cx = x.clamp(rx + radius, rx + w - radius);
    let cy = y.clamp(ry + radius, ry + h - radius);
    let (dx, dy) = (x - cx, y - cy);
    dx * dx + dy * dy <= radius * radius
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The port file is one path per user, which is right for the product and
    /// wrong for a parallel test runner: two tests recording a port would clobber
    /// each other, and a developer with the launcher actually running would have
    /// its record deleted out from under it. Serialise the tests that touch it,
    /// and put back whatever was there.
    fn with_port_file<T>(body: impl FnOnce() -> T) -> T {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        // A poisoned lock means an earlier test panicked mid-section; the saved
        // state is already restored by then, so carrying on is correct.
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let saved = std::fs::read_to_string(port_file()).ok();
        let out = body();
        match saved {
            Some(previous) => {
                let _ = std::fs::write(port_file(), previous);
            }
            None => clear_port(),
        }
        out
    }

    /// Answers one request the way `/v1/engines` does, then hangs up. Returns the
    /// port it is listening on.
    fn fake_lakeleto() -> (u16, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut scratch = [0u8; 512];
                let _ = sock.read(&mut scratch);
                let body = br#"{"version":"0.1.4","engine":{"engine":"local"},"ee":false}"#;
                let _ = sock.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                );
                let _ = sock.write_all(body);
            }
        });
        (port, handle)
    }

    /// The port must come from the OS, not from a hard-coded favourite: whatever
    /// `free_port` returns has to actually be bindable right now.
    #[test]
    fn the_chosen_port_is_one_nothing_is_using() {
        let port = free_port();
        assert_ne!(port, 0, "OS should have offered a port");
        assert_ne!(port, 8080, "should not be squatting a well-known port");
        // Nothing holds it once free_port's listener drops, so this must succeed.
        TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .expect("the port free_port returned should be free");
    }

    #[test]
    fn successive_choices_do_not_collide() {
        // Two launchers starting at once must not be handed the same port. The
        // kernel guarantees this while each listener is open, which is what the
        // sequencing here reproduces.
        let first = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let taken = first.local_addr().unwrap().port();
        assert_ne!(free_port(), taken, "must not reuse a bound port");
    }

    #[test]
    fn an_unreachable_port_is_not_a_lakeleto() {
        let closed = free_port();
        assert!(!is_lakeleto(closed));
    }

    /// A stale port file pointing at a recycled port is the case the probe exists
    /// for: something is listening, but it is not us.
    #[test]
    fn a_stranger_on_the_recorded_port_is_not_a_lakeleto() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                let mut scratch = [0u8; 512];
                let _ = sock.read(&mut scratch);
                // The `ok` a bare health endpoint would return - deliberately the
                // thing the old /healthz probe would have accepted.
                let _ = sock.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 3\r\n\r\nok\n",
                );
            }
        });
        assert!(!is_lakeleto(port), "a plain `ok` must not pass as Lakeleto");
        handle.join().unwrap();
    }

    /// A peer that answers slowly must not hold the launch open.
    ///
    /// A socket read timeout applies per call, so a byte arriving just inside it
    /// re-arms the clock; without one overall deadline this loop would run up to
    /// `buf.len()` timeouts. The probe sits in front of everything the launcher
    /// does, and the port it dials comes from a file that may be stale and
    /// recycled to any process, so "slow stranger" is a case it has to survive.
    #[test]
    fn a_trickling_peer_cannot_stall_the_probe() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else {
                return;
            };
            let mut scratch = [0u8; 512];
            let _ = sock.read(&mut scratch);
            // A byte at a time, comfortably inside PROBE_TIMEOUT each time, so
            // every read succeeds and a per-call timeout never fires. Bounded so
            // a regression fails the assertion below instead of hanging the suite.
            for _ in 0..64 {
                if sock.write_all(b"x").is_err() {
                    return;
                }
                let _ = sock.flush();
                std::thread::sleep(Duration::from_millis(120));
            }
        });

        let started = std::time::Instant::now();
        assert!(
            !is_lakeleto(port),
            "a trickle of bytes is not an engines document"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed < PROBE_TIMEOUT * 3,
            "probe blocked {elapsed:?}; a per-read timeout re-armed by every byte lets a slow \
             stranger hold the launcher open"
        );
    }

    #[test]
    fn a_running_lakeleto_is_recognised() {
        let (port, handle) = fake_lakeleto();
        assert!(is_lakeleto(port));
        handle.join().unwrap();
    }

    /// The whole double-click-twice path: a recorded port plus something
    /// answering there means reuse, not a second server.
    #[test]
    fn a_recorded_running_instance_is_reused() {
        let (port, handle) = fake_lakeleto();
        let decided = with_port_file(|| {
            record_port(port);
            decide()
        });
        assert_eq!(decided, Launch::Reuse(port));
        handle.join().unwrap();
    }

    /// No record, or a record nobody answers, must yield a fresh port rather than
    /// blocking the launch.
    #[test]
    fn a_stale_record_falls_back_to_a_fresh_port() {
        let decided = with_port_file(|| {
            record_port(free_port()); // nothing is listening there
            decide()
        });
        assert!(matches!(decided, Launch::Serve(_)), "{decided:?}");
    }

    #[test]
    fn the_port_record_round_trips_and_clears() {
        with_port_file(|| {
            record_port(54_321);
            assert_eq!(
                std::fs::read_to_string(port_file()).unwrap().trim(),
                "54321"
            );
            clear_port();
            assert!(!port_file().exists());
        });
    }

    #[test]
    fn url_points_at_loopback() {
        assert_eq!(url(8080), "http://127.0.0.1:8080/");
    }

    /// `Icon::from_rgba` rejects a buffer whose length disagrees with the
    /// dimensions, and it would do so at launch, in a process with no console.
    #[test]
    fn the_icon_buffer_matches_its_declared_size() {
        let (pixels, size) = strata_icon();
        assert_eq!(pixels.len(), (size * size * 4) as usize);
    }

    /// The mark is a rounded tile: corners transparent, centre opaque. A solid or
    /// fully transparent square would satisfy the length check above and still be
    /// obviously wrong in the tray.
    #[test]
    fn the_icon_is_a_rounded_tile_not_a_square() {
        let (pixels, size) = strata_icon();
        let alpha_at = |x: u32, y: u32| pixels[((y * size + x) * 4 + 3) as usize];

        assert_eq!(alpha_at(0, 0), 0, "corner should be outside the tile");
        assert_eq!(alpha_at(size - 1, size - 1), 0, "corner should be outside");
        assert_eq!(alpha_at(size / 2, size / 2), 255, "centre should be opaque");
    }

    /// Partly covered edge pixels must keep the tile's colour and vary only in
    /// alpha. Averaging colour over all sub-samples instead of the covered ones
    /// darkens them toward the transparent black outside the tile, and a
    /// non-premultiplied buffer turns that into a visible dark fringe around every
    /// rounded corner — worst at exactly the small sizes the sub-sampling exists
    /// to protect.
    #[test]
    fn edge_pixels_are_not_darkened_into_a_halo() {
        let (pixels, size) = strata_icon();
        // Only the rounded corners produce partial coverage — the tile's straight
        // edges land on whole pixels — so scan the whole image rather than a row.
        let partial: Vec<_> = (0..size * size)
            .map(|i| {
                let i = (i * 4) as usize;
                (pixels[i], pixels[i + 1], pixels[i + 2], pixels[i + 3])
            })
            .filter(|&(.., a)| a > 0 && a < 255)
            .collect();
        assert!(
            !partial.is_empty(),
            "a rounded tile must have antialiased corner pixels"
        );

        // Every colour the tile can hold is blue-dominant: the gradient runs
        // #2563eb -> #22d3ee, and the bands only lighten it toward white. So any
        // opaque-or-partial pixel should carry a high blue channel. Weighting
        // colour by `n` instead of coverage scales it by alpha, so a corner pixel
        // at alpha 30 would land near blue 28 — the fringe, in one number.
        let worst = partial.iter().map(|p| p.2).min().unwrap();
        assert!(
            worst >= 200,
            "partly covered pixel darkened to blue {worst}; colour is being scaled by coverage"
        );
    }

    /// The strata bands are the mark. Sampling the tile's left edge (inside the
    /// tile, outside the bands) against a band's interior must differ, or we have
    /// rendered a plain gradient tile.
    #[test]
    fn the_strata_bands_are_drawn() {
        let (pixels, size) = strata_icon();
        let px = |x: u32, y: u32| {
            let i = ((y * size + x) * 4) as usize;
            (pixels[i], pixels[i + 1], pixels[i + 2])
        };
        // Band 1 spans x=15..49, y=14.5..19.5 on the 64-unit grid; x=10 is tile
        // background at the same height.
        let background = px(10, 17);
        let band = px(30, 17);
        assert_ne!(background, band, "the first strata band should be visible");
        assert!(
            band.0 > background.0 && band.1 > background.1 && band.2 > background.2,
            "bands are white over the gradient, so every channel should lighten: \
             background={background:?} band={band:?}"
        );
    }

    #[test]
    fn the_cli_is_the_launchers_sibling() {
        // Inside the bundle both binaries live in Contents/MacOS/, so resolving
        // relative to the running launcher survives the user moving the .app.
        let launcher = Path::new("/Applications/Lakeleto.app/Contents/MacOS/lakeleto-desktop");
        let cli = cli_beside(launcher);
        assert_eq!(cli.parent(), launcher.parent());
        assert_eq!(
            cli.file_name().unwrap(),
            if cfg!(windows) {
                "lakeleto.exe"
            } else {
                "lakeleto"
            }
        );
    }

    /// The rule that keeps this from clobbering a Homebrew install. A real file
    /// is someone else's; a symlink — even a broken one pointing at an .app that
    /// moved — is ours to replace.
    #[test]
    fn only_our_own_symlinks_are_replaced() {
        assert_eq!(decide_link(&LinkState::Absent), LinkAction::Create);
        assert_eq!(decide_link(&LinkState::Ours), LinkAction::NothingToDo);
        assert_eq!(
            decide_link(&LinkState::StaleLink),
            LinkAction::Replace,
            "a link resolving to nothing serves nobody - taking it over costs no one"
        );
        assert_eq!(
            decide_link(&LinkState::ForeignLink),
            LinkAction::Refuse,
            "a link that still resolves is somebody's working command - never delete it"
        );
        assert_eq!(
            decide_link(&LinkState::RealFile),
            LinkAction::Refuse,
            "a real binary is somebody's install - never overwrite it"
        );
    }

    /// The regression this state exists for. `brew install lakeleto` does not put
    /// a *file* in `/usr/local/bin` — it puts a symlink into `../Cellar`. Reading
    /// that as a stale link of ours would delete a user's working Homebrew CLI,
    /// which is the one outcome the install menu item promises never to cause.
    #[test]
    fn a_live_homebrew_style_symlink_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        // The Homebrew shape: bin/lakeleto -> ../Cellar/lakeleto/0.1.4/bin/lakeleto
        let cellar = dir.path().join("Cellar/lakeleto/0.1.4/bin");
        std::fs::create_dir_all(&cellar).unwrap();
        std::fs::write(cellar.join("lakeleto"), b"#!/bin/sh\n").unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let link = bin.join("lakeleto");
        std::os::unix::fs::symlink("../Cellar/lakeleto/0.1.4/bin/lakeleto", &link).unwrap();

        let ours = dir.path().join("Lakeleto.app/Contents/MacOS/lakeleto");
        let state = link_state(&link, &ours);

        assert_eq!(
            state,
            LinkState::ForeignLink,
            "a resolving link is not ours to reclaim"
        );
        assert_eq!(decide_link(&state), LinkAction::Refuse);
        assert!(link.exists(), "and it is still there");
    }

    /// The case that *is* ours: the `Lakeleto.app` a previous install linked has
    /// been moved or deleted, so the link resolves to nothing.
    #[test]
    fn a_dangling_link_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("lakeleto");
        std::os::unix::fs::symlink(dir.path().join("gone/Lakeleto.app/lakeleto"), &link).unwrap();

        let state = link_state(&link, &dir.path().join("here/lakeleto"));

        assert_eq!(state, LinkState::StaleLink);
        assert_eq!(decide_link(&state), LinkAction::Replace);
    }

    #[test]
    fn a_missing_link_path_reads_as_absent() {
        // A directory of our own, so a parallel test process — or a real file
        // that happens to share the name — is never touched.
        let dir = tempfile::tempdir().unwrap();
        let nowhere = dir.path().join("no-such-link-at-all");
        assert_eq!(
            link_state(&nowhere, Path::new("/whatever")),
            LinkState::Absent
        );
    }

    /// A regular file must be recognised as such, not mistaken for our link.
    #[test]
    fn a_regular_file_is_not_mistaken_for_our_link() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("lakeleto");
        std::fs::write(&file, b"#!/bin/sh\n").unwrap();

        assert_eq!(
            link_state(&file, Path::new("/somewhere/else")),
            LinkState::RealFile
        );
    }

    #[test]
    fn the_preferred_link_dir_is_on_the_default_macos_path() {
        let dirs = cli_link_dirs();
        assert_eq!(
            dirs.first().unwrap(),
            Path::new("/usr/local/bin"),
            "/usr/local/bin is in /etc/paths, so a link there needs no profile edit"
        );
    }

    /// Every outcome has to say something actionable — an empty or truncated
    /// dialog is the failure mode of a process with no other UI.
    #[test]
    fn every_outcome_explains_itself() {
        let link = PathBuf::from("/usr/local/bin/lakeleto");
        let cases = [
            CliInstall::Linked {
                link: link.clone(),
                on_default_path: true,
            },
            CliInstall::Linked {
                link: link.clone(),
                on_default_path: false,
            },
            CliInstall::AlreadyLinked(link.clone()),
            CliInstall::Occupied(link),
            CliInstall::Failed("disk on fire".into()),
        ];
        for case in cases {
            let msg = case.message();
            assert!(msg.len() > 20, "{case:?} -> {msg:?}");
        }
        // The fallback directory is not on the default PATH, so the message has
        // to tell the user what to add; without it the link is invisible.
        let off_path = CliInstall::Linked {
            link: PathBuf::from("/home/x/.local/bin/lakeleto"),
            on_default_path: false,
        };
        assert!(
            off_path.message().contains("export PATH"),
            "{}",
            off_path.message()
        );
    }

    #[test]
    fn the_log_lands_beside_the_workspace_store() {
        let path = log_path();
        assert_eq!(path.file_name().unwrap(), "launcher.log");
        assert!(path.parent().is_some());
    }
}
