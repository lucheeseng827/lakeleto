// No console window. Without this the launcher is a console subsystem binary, so
// double-clicking it — the entire point of shipping an installer — flashes a black
// window that never goes away, and the Start Menu entry looks broken. On macOS and
// Linux the attribute does not apply; the `.app` bundle plays this role there.
#![cfg_attr(windows, windows_subsystem = "windows")]

//! `lakeleto-desktop` — what the Start Menu entry and the `Lakeleto.app` icon run.
//!
//! The README used to have to say *"double-clicking the file won't work"*. This is
//! the binary that makes that sentence false. It is deliberately thin: every
//! decision it makes is one a terminal user would have made by typing, and the
//! actual work is still [`lakeleto::api::serve`].
//!
//! 1. [`desktop::decide`] finds the Lakeleto already running, or takes a port the
//!    OS says nothing is using.
//! 2. The server runs on a worker thread; the browser opens itself.
//! 3. A tray icon holds the main thread, because a process with no console and no
//!    window is a process the user cannot quit without the task manager.
//!
//! Failures are the interesting part: with no console, a `panic!` or a bind error
//! is *invisible*. Anything fatal is written to [`lakeleto::desktop::log_path`]
//! before exiting, so "I clicked it and nothing happened" has an answer on disk.

use std::sync::mpsc;

use lakeleto::desktop::{self, Launch};
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{TrayIconBuilder, TrayIconEvent};
use winit::application::ApplicationHandler;
use winit::event::WindowEvent;
use winit::event_loop::{ActiveEventLoop, ControlFlow, EventLoop};
use winit::window::WindowId;

fn main() {
    // A panic anywhere in a windowless process is silent. Route it to the log
    // first, so the failure leaves evidence rather than just not starting.
    desktop::install_panic_logger();

    match desktop::decide() {
        // Already running: this is the user double-clicking the icon a second
        // time. Show them the window they already have and get out of the way —
        // starting a rival server on another port would silently split their
        // workspaces across two stores.
        Launch::Reuse(port) => {
            desktop::open_browser(&desktop::url(port));
        }
        Launch::Serve(port) => {
            // Publish before serving: the window between here and the browser
            // opening is when an impatient user double-clicks a second time.
            desktop::record_port(port);
            let outcome = run_server(port);
            // Leaving a record behind costs the next launch a pointless probe
            // against a dead port, so clear it however this exits.
            desktop::clear_port();
            if let Err(e) = outcome {
                desktop::report_fatal(&e);
                std::process::exit(1);
            }
        }
    }
}

/// Serve on `port` and hold the process open behind a tray icon.
fn run_server(port: u16) -> Result<(), String> {
    // `serve` blocks for the life of the process and owns its own Tokio runtime,
    // so it gets a thread and the main thread is left for the event loop — which
    // macOS requires be the main thread anyway.
    let (fatal_tx, fatal_rx) = mpsc::channel::<String>();
    std::thread::Builder::new()
        .name("lakeleto-serve".into())
        .spawn(move || {
            if let Err(e) = desktop::serve_at(port, None) {
                // Bind races, an unreadable workspace home — the launcher has no
                // stdout, so hand it back to the main thread to log and surface.
                let _ = fatal_tx.send(format!("server stopped: {e}"));
            }
        })
        .map_err(|e| format!("spawning the server thread: {e}"))?;

    let event_loop = EventLoop::new().map_err(|e| format!("creating the event loop: {e}"))?;
    // `Wait` rather than `Poll`: this loop exists to notice menu clicks, not to
    // render. Polling would spin a core forever for a program that draws nothing.
    event_loop.set_control_flow(ControlFlow::Wait);

    let mut app = Launcher {
        port,
        tray: None,
        items: None,
        fatal: fatal_rx,
    };
    event_loop
        .run_app(&mut app)
        .map_err(|e| format!("running the event loop: {e}"))
}

/// The menu items, kept alive alongside the tray: dropping a `MenuItem` unregisters
/// its id, and then clicks arrive for an id nothing matches.
struct Items {
    open: MenuItem,
    copy_cli: MenuItem,
    /// macOS only. The Windows installer puts the CLI on the user's PATH, so
    /// there is nothing for this to do there; a menu item that reports "already
    /// installed" forever is worse than no menu item.
    #[cfg(target_os = "macos")]
    install_cli: MenuItem,
    quit: MenuItem,
}

struct Launcher {
    port: u16,
    tray: Option<tray_icon::TrayIcon>,
    items: Option<Items>,
    fatal: mpsc::Receiver<String>,
}

impl ApplicationHandler for Launcher {
    /// The tray is built here rather than in `main` because macOS will not accept
    /// a status item before the `NSApplication` the event loop sets up exists.
    fn resumed(&mut self, _event_loop: &ActiveEventLoop) {
        if self.tray.is_some() {
            return; // `resumed` can fire more than once; the tray is built once.
        }
        match build_tray() {
            Ok((tray, items)) => {
                self.tray = Some(tray);
                self.items = Some(items);
            }
            // No tray means no way to quit, so this is fatal rather than cosmetic.
            Err(e) => {
                desktop::report_fatal(&e);
                std::process::exit(1);
            }
        }
    }

    /// The launcher owns no windows — the UI is a browser tab. Required by the
    /// trait; nothing can arrive here.
    fn window_event(&mut self, _: &ActiveEventLoop, _: WindowId, _: WindowEvent) {}

    /// Tray and menu events do not arrive as winit events; they come over their
    /// own channels, so they are drained on each turn of the loop.
    fn new_events(&mut self, event_loop: &ActiveEventLoop, _: winit::event::StartCause) {
        if let Ok(message) = self.fatal.try_recv() {
            desktop::report_fatal(&message);
            event_loop.exit();
            return;
        }
        while let Ok(event) = MenuEvent::receiver().try_recv() {
            let Some(items) = &self.items else { continue };
            if event.id == items.copy_cli.id() {
                match desktop::cli_path() {
                    Some(path) => {
                        let shown = path.display().to_string();
                        let body = match desktop::copy_to_clipboard(&shown) {
                            Ok(()) => format!("Copied to the clipboard:\n\n{shown}"),
                            // Still show it: the user can select and copy by
                            // hand, which beats a menu item that silently fails.
                            Err(e) => format!("The lakeleto command is at:\n\n{shown}\n\n(Could not reach the clipboard: {e})"),
                        };
                        desktop::notify(&body);
                    }
                    None => desktop::notify(
                        "The lakeleto command was not found beside this application.",
                    ),
                }
                continue;
            }
            #[cfg(target_os = "macos")]
            if event.id == items.install_cli.id() {
                // Runs on the event-loop thread: it is a handful of filesystem
                // calls, and `notify` only spawns osascript rather than waiting
                // on it, so the menu cannot appear wedged.
                let outcome = desktop::install_cli();
                desktop::notify(&outcome.message());
                continue;
            }
            if event.id == items.open.id() {
                desktop::open_browser(&desktop::url(self.port));
            } else if event.id == items.quit.id() {
                event_loop.exit();
            }
        }
        // Clicking the icon itself (rather than the menu) reopens the tab, which
        // is what a tray icon for a browser-backed app should do.
        while let Ok(event) = TrayIconEvent::receiver().try_recv() {
            if let TrayIconEvent::DoubleClick { .. } = event {
                desktop::open_browser(&desktop::url(self.port));
            }
        }
    }
}

fn build_tray() -> Result<(tray_icon::TrayIcon, Items), String> {
    let menu = Menu::new();
    let open = MenuItem::new("Open Lakeleto", true, None);
    // The CLI ships with the app on both platforms and nothing in the installed
    // experience mentions it — the Start Menu entry and the .app both open the
    // browser UI. This is the smallest thing that makes it discoverable.
    let copy_cli = MenuItem::new("Copy CLI path", true, None);
    #[cfg(target_os = "macos")]
    let install_cli = MenuItem::new("Install command line tool…", true, None);
    let quit = MenuItem::new("Quit Lakeleto", true, None);

    menu.append(&open).map_err(|e| format!("tray menu: {e}"))?;
    menu.append(&PredefinedMenuItem::separator())
        .map_err(|e| format!("tray menu: {e}"))?;
    menu.append(&copy_cli)
        .map_err(|e| format!("tray menu: {e}"))?;
    #[cfg(target_os = "macos")]
    menu.append(&install_cli)
        .map_err(|e| format!("tray menu: {e}"))?;
    menu.append(&PredefinedMenuItem::separator())
        .map_err(|e| format!("tray menu: {e}"))?;
    menu.append(&quit).map_err(|e| format!("tray menu: {e}"))?;

    let (rgba, size) = desktop::strata_icon();
    let icon =
        tray_icon::Icon::from_rgba(rgba, size, size).map_err(|e| format!("tray icon: {e}"))?;

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip(format!("Lakeleto {}", env!("CARGO_PKG_VERSION")))
        .with_icon(icon)
        .build()
        .map_err(|e| format!("building the tray icon: {e}"))?;

    Ok((
        tray,
        Items {
            open,
            copy_cli,
            #[cfg(target_os = "macos")]
            install_cli,
            quit,
        },
    ))
}
