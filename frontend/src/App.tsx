// Lakeleto — the multi-tab workspace shell. Around the per-tab TableView it wires the workspace
// data plane (/v1/workspaces/*): a workspace switcher, saved connections + queries (grouped into
// folders), environment variables ({{var}}), a ⌘K command palette, keyboard shortcuts, the run
// history store, cached-result re-open, a two-run compare, a run-across-connections runner, and
// export/import. Open tabs + grid state persist through the store, so a reload restores the
// workbench. Falls back to an offline localStorage store + sample tables when no server answers.
import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { connectLakeleto, type Backend, type Conn, type Row, type RunRecord, type Workspace, type WorkspaceBundle, type WsConnection, type WsMeta, type WsSavedQuery } from "./api";
import { Banner, Button, Chip } from "./components";
import { TableView } from "./TableView";
import { buildDoc, docToTabs, HistoryPanel, newDataTab, newLauncherTab, newTabId, resolveVars, ResultView, Sidebar, TabStrip, WorkspaceBar, basename, type OpenTab } from "./workspace";
import { CommandPalette, CompareView, LauncherView, RowDetail, RunnerModal, type PaletteItem, type RunJob } from "./extras";

const qs = new URLSearchParams(location.search);
const dirOf = (p: string) => { const i = Math.max(p.lastIndexOf("/"), p.lastIndexOf("\\")); return i > 0 ? p.slice(0, i) : "/data/warehouse"; };

interface RunnerCfg { open: boolean; title: string; sharedSql: string | null; jobs: RunJob[]; }

export function App() {
  const [conn, setConn] = useState<Conn | null>(null);
  const [connErr, setConnErr] = useState<string | null>(null);

  const [workspaces, setWorkspaces] = useState<WsMeta[]>([]);
  const [wsId, setWsId] = useState<string | null>(null);
  const [ws, setWs] = useState<Workspace | null>(null);
  const [tabs, setTabs] = useState<OpenTab[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [closedTabs, setClosedTabs] = useState<OpenTab[]>([]);
  const [history, setHistory] = useState<RunRecord[]>([]);
  const [listing, setListing] = useState<import("./api").Listing | null>(null);
  const [showSidebar, setShowSidebar] = useState(true);
  const [showHistory, setShowHistory] = useState(true);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savedKey, setSavedKey] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [detailRow, setDetailRow] = useState<Row | null>(null);
  const [runner, setRunner] = useState<RunnerCfg | null>(null);
  const [compareSel, setCompareSel] = useState<RunRecord | null>(null);

  const backend: Backend | null = conn ? conn.backend : null;
  const active = tabs.find((t) => t.id === activeId) || null;
  const resolve = (s: string) => resolveVars(s, ws?.variables);
  const mutate = (fn: (w: Workspace) => Workspace) => setWs((w) => (w ? fn(w) : w));

  // Content-only fingerprint (excludes timestamps/id) — the autosave trigger. Memoized so the
  // full-workbench stringify runs only when ws/tabs actually change, not on every render.
  const contentKey = useMemo(
    () => (ws ? JSON.stringify({ name: ws.name, connections: ws.connections, saved_queries: ws.saved_queries, variables: ws.variables || [], tabs: buildDoc(ws, tabs).tabs }) : null),
    [ws, tabs],
  );

  const patchTab = (id: string, p: Partial<OpenTab>) => setTabs((ts) => ts.map((t) => (t.id === id ? { ...t, ...p } : t)));
  const addTab = (t: OpenTab) => { setTabs((ts) => [...ts, t]); setActiveId(t.id); };
  const closeTab = (id: string) => setTabs((ts) => {
    const closed = ts.find((t) => t.id === id);
    if (closed && closed.kind === "data") setClosedTabs((c) => [...c.slice(-9), closed]);
    const idx = ts.findIndex((t) => t.id === id);
    const next = ts.filter((t) => t.id !== id);
    if (activeId === id) { const na = next[idx] || next[idx - 1] || next[0] || null; setActiveId(na ? na.id : null); }
    return next;
  });
  const reopenClosed = () => setClosedTabs((c) => { if (!c.length) return c; const t = c[c.length - 1]; addTab({ ...t, id: newTabId(), sqlOut: null, sqlErr: null, sqlBusy: false }); return c.slice(0, -1); });
  const renameTab = (id: string) => { const t = tabs.find((x) => x.id === id); if (!t) return; const n = window.prompt("Rename tab", t.title); if (n && n.trim()) patchTab(id, { title: n.trim() }); };
  const duplicateTab = () => { if (active && active.kind === "data") addTab({ ...active, id: newTabId(), title: active.title + " copy", sqlOut: null, sqlErr: null }); };
  const switchTab = (dir: number) => { if (!tabs.length) return; const i = Math.max(0, tabs.findIndex((t) => t.id === activeId)); setActiveId(tabs[(i + dir + tabs.length) % tabs.length].id); };
  const reorderTabs = (fromId: string, toId: string) => setTabs((ts) => {
    if (fromId === toId) return ts;
    const from = ts.find((t) => t.id === fromId);
    if (!from) return ts;
    const rest = ts.filter((t) => t.id !== fromId);
    const i = rest.findIndex((t) => t.id === toId);
    rest.splice(i < 0 ? rest.length : i, 0, from);
    return rest;
  });

  const loadListingFor = (path: string) => backend && backend.list(dirOf(path)).then(setListing).catch(() => { /* ignore */ });
  const openDir = (d: string) => backend && backend.list(d).then((l) => { setListing(l); setErr(null); }).catch((e) => setErr((e as Error).message));

  const openPath = (path: string, opts: { connId?: string | null; title?: string } = {}) => {
    setErr(null);
    const existing = tabs.find((t) => t.kind === "data" && t.path === path);
    if (existing) { setActiveId(existing.id); if (opts.connId) patchTab(existing.id, { connId: opts.connId }); }
    else addTab(newDataTab(path, opts));
    loadListingFor(path);
  };
  // `+` opens a launcher tab (a start page), not an arbitrary source.
  const openLauncher = () => addTab(newLauncherTab());
  // Launcher actions: create the chosen tab, then drop the launcher tab.
  const launcherOpenPath = (launcherId: string, path: string) => {
    openPath(path);
    setTabs((ts) => ts.filter((t) => t.id !== launcherId));
  };
  const launcherNewQuery = (launcherId: string, c: WsConnection) => {
    addTab(newDataTab(c.path, { sub: "SQL", connId: c.id, title: c.label }));
    setTabs((ts) => ts.filter((t) => t.id !== launcherId));
    loadListingFor(c.path);
  };

  const loadHistory = async (id: string) => {
    if (!backend) return;
    setHistoryBusy(true);
    try { setHistory(await backend.wsHistory(id)); } catch { /* ignore */ } finally { setHistoryBusy(false); }
  };

  const selectWorkspace = async (id: string) => {
    if (!backend) return;
    try {
      const w = await backend.wsGet(id);
      setWs(w); setWsId(id); setSavedKey(null); setCompareSel(null);
      let t = docToTabs(w);
      if (t.length === 0 && w.connections[0]) t = [newDataTab(w.connections[0].path, { connId: w.connections[0].id })];
      setTabs(t); setActiveId(t[0]?.id ?? null);
      if (t[0]) loadListingFor(t[0].path);
      else {
        // Fresh workspace: show the server's default dir and auto-open its first data file so the
        // grid isn't empty on first load (no hardcoded sample path — works against any root).
        try {
          const l = await backend.list("");
          setListing(l);
          const first = l.entries.find((e) => e.kind === "file");
          if (first) addTab(newDataTab(first.path));
        } catch { /* no default listing */ }
      }
      loadHistory(id);
    } catch (e) { setErr((e as Error).message); }
  };

  const loadWorkspaces = async (be: Backend) => {
    try {
      let list = await be.wsList();
      if (list.length === 0) { const w = await be.wsCreate("My workspace"); list = await be.wsList(); setWorkspaces(list); await selectWorkspace(w.id); return; }
      setWorkspaces(list);
      await selectWorkspace(list[0].id);
    } catch (e) { setErr("Workspaces unavailable: " + (e as Error).message); }
  };

  useEffect(() => {
    connectLakeleto({ base: qs.get("api") || "", token: qs.get("token") || "" }).then(setConn).catch((e) => setConnErr((e as Error).message));
  }, []);
  useEffect(() => { if (conn) loadWorkspaces(conn.backend); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [conn]);

  // Debounced autosave: persist the workbench whenever its content (not timestamps) changes.
  useEffect(() => {
    if (!backend || !ws || !wsId || contentKey == null) return;
    if (savedKey === null) { setSavedKey(contentKey); return; }
    if (savedKey === contentKey) return;
    const h = setTimeout(() => {
      setSaving(true);
      backend.wsSave(wsId, buildDoc(ws, tabs)).then((saved) => { setWs(saved); setSavedKey(contentKey); })
        .catch((e) => setErr("Save failed: " + (e as Error).message)).finally(() => setSaving(false));
    }, 700);
    return () => clearTimeout(h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [contentKey]);

  const runSql = async (tabId: string, sql: string) => {
    const t = tabs.find((x) => x.id === tabId);
    if (!backend || !wsId || !t) return;
    patchTab(tabId, { sqlBusy: true, sqlErr: null });
    try {
      const resp = await backend.wsRun(wsId, { sql: resolve(sql), path: resolve(t.path), preview: 200 });
      patchTab(tabId, { sqlOut: resp, sqlErr: null, sqlBusy: false });
    } catch (e) { patchTab(tabId, { sqlErr: (e as Error).message, sqlOut: null, sqlBusy: false }); }
    loadHistory(wsId);
  };

  const openRunResultTab = (r: RunRecord) => {
    if (!backend || !wsId) return;
    if (!r.cached) { if (r.sql) addTab(newDataTab(r.source_path, { sub: "SQL", sql: r.sql, title: basename(r.source_path) })); else openPath(r.source_path); return; }
    const t: OpenTab = { id: newTabId(), kind: "result", title: "result · " + basename(r.source_path), path: r.source_path, sub: "Grid", sort: null, filters: {}, sql: "", connId: null, runId: r.id, run: r, result: null };
    addTab(t);
    backend.wsRunResult(wsId, r.id, 0, 500).then((res) => patchTab(t.id, { result: res })).catch((e) => patchTab(t.id, { resultErr: (e as Error).message }));
  };

  const onCompare = (r: RunRecord) => {
    if (!r.cached) { setErr("Only cached results can be compared."); return; }
    if (!compareSel) { setCompareSel(r); return; }
    if (compareSel.id === r.id) { setCompareSel(null); return; }
    openCompare(compareSel, r); setCompareSel(null);
  };
  const openCompare = (a: RunRecord, b: RunRecord) => {
    if (!backend || !wsId) return;
    const t: OpenTab = { id: newTabId(), kind: "compare", title: "compare A/B", path: "", sub: "Grid", sort: null, filters: {}, sql: "", connId: null, runA: a, runB: b, resultA: null, resultB: null };
    addTab(t);
    backend.wsRunResult(wsId, a.id, 0, 1000).then((res) => patchTab(t.id, { resultA: res })).catch((e) => patchTab(t.id, { resultErr: (e as Error).message }));
    backend.wsRunResult(wsId, b.id, 0, 1000).then((res) => patchTab(t.id, { resultB: res })).catch((e) => patchTab(t.id, { resultErr: (e as Error).message }));
  };

  // ---- runner (run across connections / run a folder) ----
  const execRun = async (sql: string, path: string) => {
    if (!backend || !wsId) throw new Error("no workspace");
    const resp = await backend.wsRun(wsId, { sql: resolve(sql), path: resolve(path), preview: 200 });
    loadHistory(wsId);
    return resp;
  };
  const runAcross = () => {
    if (!active || active.kind !== "data" || !ws) return;
    if (!ws.connections.length) { setErr("Add a connection first to run across sources."); return; }
    setRunner({ open: true, title: "Run across connections", sharedSql: active.sql, jobs: ws.connections.map((c) => ({ id: c.id, label: c.label, path: c.path, sql: active.sql })) });
  };
  const runFolder = (folder: string) => {
    if (!ws) return;
    const jobs: RunJob[] = ws.saved_queries.filter((q) => q.folder === folder).map((q) => {
      const c = q.connection_id ? ws.connections.find((x) => x.id === q.connection_id) : null;
      return { id: q.id, label: q.name, path: c?.path || active?.path || ws.connections[0]?.path || "", sql: q.sql };
    }).filter((j) => j.path);
    if (!jobs.length) { setErr("No runnable queries in this folder — bind them to a connection."); return; }
    setRunner({ open: true, title: `Run folder: ${folder}`, sharedSql: null, jobs });
  };

  // ---- connection / saved-query actions ----
  const openConnection = (c: WsConnection) => openPath(c.path, { connId: c.id, title: c.label });
  const saveConnection = () => {
    if (!active || active.kind !== "data" || !ws) return;
    const exists = ws.connections.find((c) => c.path === active.path);
    if (exists) { patchTab(active.id, { connId: exists.id }); return; }
    const c: WsConnection = { id: "conn-" + newTabId(), label: basename(active.path), path: active.path };
    setWs({ ...ws, connections: [...ws.connections, c] });
    patchTab(active.id, { connId: c.id });
  };
  const openQuery = (q: WsSavedQuery) => {
    const c = q.connection_id && ws ? ws.connections.find((x) => x.id === q.connection_id) : null;
    const path = c?.path || active?.path || ws?.connections[0]?.path;
    if (!path) { setErr("This query has no source — open a source first."); return; }
    addTab(newDataTab(path, { sub: "SQL", sql: q.sql, title: q.name, connId: c?.id ?? null }));
  };
  const saveQuery = () => {
    if (!active || active.kind !== "data" || !ws) return;
    const raw = window.prompt("Save query as (use folder/name to group it)", active.title);
    if (raw == null || !raw.trim()) return;
    const s = raw.trim(); const slash = s.lastIndexOf("/");
    const folder = slash > 0 ? s.slice(0, slash).trim() : null;
    const name = slash > 0 ? s.slice(slash + 1).trim() || s : s;
    const q: WsSavedQuery = { id: "q-" + newTabId(), name, sql: active.sql, connection_id: active.connId, folder };
    setWs({ ...ws, saved_queries: [...ws.saved_queries, q] });
  };

  // ---- workspace-level actions ----
  const newWorkspace = async () => {
    if (!backend) return;
    const name = window.prompt("New workspace name", "Workspace"); if (name == null) return;
    const w = await backend.wsCreate(name.trim() || "Workspace");
    setWorkspaces(await backend.wsList()); await selectWorkspace(w.id);
  };
  const renameWorkspace = () => {
    if (!ws) return;
    const name = window.prompt("Rename workspace", ws.name); if (name == null || !name.trim()) return;
    setWs({ ...ws, name: name.trim() });
    setWorkspaces((wss) => wss.map((w) => (w.id === ws.id ? { ...w, name: name.trim() } : w)));
  };
  const deleteWorkspace = async () => {
    if (!backend || !wsId || !ws) return;
    if (!window.confirm(`Delete workspace "${ws.name}"? This removes its history and cached results.`)) return;
    await backend.wsDelete(wsId);
    const list = await backend.wsList(); setWorkspaces(list);
    if (list.length) await selectWorkspace(list[0].id);
    else { const w = await backend.wsCreate("My workspace"); setWorkspaces(await backend.wsList()); await selectWorkspace(w.id); }
  };
  const exportWorkspace = async () => {
    if (!backend || !wsId) return;
    try {
      const bundle = await backend.wsExport(wsId);
      const url = URL.createObjectURL(new Blob([JSON.stringify(bundle, null, 2)], { type: "application/json" }));
      const a = document.createElement("a"); a.href = url;
      a.download = (bundle.workspace.name || "workspace").replace(/[^\w.-]+/g, "_") + ".lakeleto-workspace.json"; a.click(); URL.revokeObjectURL(url);
    } catch (e) { setErr((e as Error).message); }
  };
  const importWorkspace = async (file: File) => {
    if (!backend) return;
    // "Import" loads a *workspace bundle* (the .json from Export), NOT a data file. Guard the
    // common mistake — picking a .csv/.parquet here — with a clear message instead of a raw
    // "Unexpected token … is not valid JSON" from JSON.parse.
    const looksLikeData = /\.(csv|tsv|parquet|json?l|ndjson|arrow)$/i.test(file.name);
    let bundle: WorkspaceBundle;
    try {
      bundle = JSON.parse(await file.text()) as WorkspaceBundle;
    } catch {
      setErr(looksLikeData
        ? `"${file.name}" is a data file, not a workspace. To view data, open it from the file browser (or the address box) — "Import" only loads a workspace bundle exported via "Export".`
        : `"${file.name}" isn't valid JSON. "Import" expects a Lakeleto workspace bundle (the .json from "Export").`);
      return;
    }
    if (!bundle || typeof bundle !== "object" || !bundle.workspace || bundle.bundle_version == null) {
      setErr(`"${file.name}" is JSON but not a Lakeleto workspace bundle (no workspace / bundle_version). Use a file exported via "Export".`);
      return;
    }
    try {
      const w = await backend.wsImport(bundle);
      setWorkspaces(await backend.wsList()); await selectWorkspace(w.id);
    } catch (e) { setErr("Import failed: " + (e as Error).message); }
  };

  // ---- keyboard shortcuts (via a ref so the listener always sees fresh state) ----
  const kb = useRef<Record<string, () => void>>({});
  kb.current = {
    palette: () => setPaletteOpen((v) => !v),
    run: () => { if (active && active.kind === "data" && active.sub === "SQL") runSql(active.id, active.sql); },
    newTab: openLauncher, close: () => { if (activeId) closeTab(activeId); },
    prev: () => switchTab(-1), next: () => switchTab(1), reopen: reopenClosed,
    sidebar: () => setShowSidebar((v) => !v),
    esc: () => { setPaletteOpen(false); setDetailRow(null); setRunner(null); },
  };
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const mod = e.metaKey || e.ctrlKey;
      const tag = (e.target as HTMLElement | null)?.tagName;
      const typing = tag === "INPUT" || tag === "TEXTAREA";
      const k = e.key.toLowerCase();
      if (mod && k === "k") { e.preventDefault(); kb.current.palette(); }
      else if (mod && e.key === "Enter") { e.preventDefault(); kb.current.run(); }
      else if (mod && k === "b" && !typing) { e.preventDefault(); kb.current.sidebar(); }
      else if (e.altKey && k === "t" && !typing) { e.preventDefault(); kb.current.newTab(); }
      else if (e.altKey && k === "w" && !typing) { e.preventDefault(); kb.current.close(); }
      else if (e.altKey && k === "r" && !typing) { e.preventDefault(); kb.current.reopen(); }
      else if (e.altKey && e.key === "ArrowLeft" && !typing) { e.preventDefault(); kb.current.prev(); }
      else if (e.altKey && e.key === "ArrowRight" && !typing) { e.preventDefault(); kb.current.next(); }
      else if (e.key === "Escape") { kb.current.esc(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  if (connErr) return <div style={{ padding: 24 }}><Banner tone="err">Could not connect to the Lakeleto server: {connErr}</Banner></div>;
  if (!conn) return <div style={{ padding: 24, color: "var(--muted)", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)" }}>connecting to engine…</div>;

  const sqlAvailable = !!(conn.caps && conn.caps.sql_available);
  const engineName = conn.caps.engine.engine;
  const shellBar: CSSProperties = { display: "flex", gap: "var(--space-4)", alignItems: "center", padding: "var(--pad-toolbar)", borderBottom: "var(--border-hairline)", flex: "0 0 auto", flexWrap: "wrap" };

  // ---- command palette items ----
  const paletteItems: PaletteItem[] = [
    { icon: "＋", label: "New tab", hint: "Alt+T", run: openLauncher },
    { icon: "⧉", label: "Duplicate current tab", run: duplicateTab },
    { icon: "✎", label: "Rename current tab", run: () => activeId && renameTab(activeId) },
    ...(active?.kind === "data" && active.sub === "SQL" ? [{ icon: "▶", label: "Run across connections…", hint: "current SQL", run: runAcross }] : []),
    ...(closedTabs.length ? [{ icon: "↺", label: "Reopen closed tab", hint: "Alt+R", run: reopenClosed }] : []),
    { icon: "⤓", label: "Export workspace", run: exportWorkspace },
    ...(ws?.connections || []).map((c) => ({ icon: "▤", label: c.label, hint: c.path, run: () => openConnection(c) })),
    ...(ws?.saved_queries || []).map((q) => ({ icon: "λ", label: (q.folder ? q.folder + "/" : "") + q.name, hint: q.sql.replace(/\s+/g, " "), run: () => openQuery(q) })),
    ...(listing?.entries || []).map((e) => ({ icon: e.kind === "dir" ? "▸" : "▦", label: e.name, hint: e.path, run: () => (e.kind === "dir" ? openDir(e.path) : openPath(e.path)) })),
    ...tabs.map((t) => ({ icon: "▦", label: "Tab: " + t.title, hint: t.path, run: () => setActiveId(t.id) })),
  ];

  return (
    <>
      <WorkspaceBar conn={conn} engineName={engineName} sqlAvailable={sqlAvailable}
        workspaces={workspaces} wsId={wsId} onSelect={selectWorkspace} onNew={newWorkspace} onRename={renameWorkspace}
        onDelete={deleteWorkspace} onExport={exportWorkspace} onImport={importWorkspace} busy={saving} />

      <TabStrip tabs={tabs} activeId={activeId} onSelect={setActiveId} onClose={closeTab} onNew={openLauncher} onRename={renameTab} onReorder={reorderTabs} />

      <div style={shellBar}>
        <Button size="sm" onClick={() => setShowSidebar((v) => !v)} title="toggle the sidebar (⌘B)">☰ Sidebar</Button>
        <Button size="sm" onClick={saveConnection} disabled={!active || active.kind !== "data"} title="save this source as a connection">⭑ Save source</Button>
        <Button size="sm" onClick={saveQuery} disabled={!active || active.kind !== "data" || active.sub !== "SQL"} title="save the current SQL as a saved query">Save query</Button>
        <Button size="sm" onClick={runAcross} disabled={!active || active.kind !== "data" || active.sub !== "SQL"} title="run this SQL across several connections">▶ Run across…</Button>
        <span style={{ flex: 1 }} />
        <Button size="sm" onClick={() => setPaletteOpen(true)} title="command palette (⌘K)">⌘K</Button>
        <Button size="sm" onClick={() => setShowHistory((v) => !v)} title="toggle run history">◷ History{history.length ? ` (${history.length})` : ""}</Button>
      </div>

      {err && <div style={{ padding: "var(--space-4) var(--gutter)" }}><Banner tone="err">{err}</Banner></div>}

      <div style={{ flex: "1 1 auto", display: "flex", minHeight: 0, overflow: "hidden" }}>
        {showSidebar && (
          <Sidebar ws={ws} listing={listing} mutate={mutate}
            onOpenConnection={openConnection} onOpenQuery={openQuery} onRunFolder={runFolder}
            onOpenFile={(p) => openPath(p)} onOpenDir={openDir} />
        )}

        {active ? (
          active.kind === "launcher" ? (
            <LauncherView connections={ws?.connections || []}
              onOpenPath={(p) => launcherOpenPath(active.id, p)}
              onNewQuery={(c) => launcherNewQuery(active.id, c)}
              onBrowse={() => setPaletteOpen(true)} />
          ) : active.kind === "compare" ? <CompareView tab={active} />
            : active.kind === "result" ? <ResultView tab={active} onOpenRow={setDetailRow} />
              : <TableView backend={conn.backend} conn={conn} tab={active} onPatch={(p) => patchTab(active.id, p)} onRunSql={(sql) => runSql(active.id, sql)} sqlAvailable={sqlAvailable} resolve={resolve} onOpenRow={setDetailRow} />
        ) : (
          <main style={{ flex: "1 1 auto", display: "flex", alignItems: "center", justifyContent: "center", color: "var(--muted)", flexDirection: "column", gap: 10 }}>
            <div>No open tabs.</div>
            <Button variant="primary" onClick={openLauncher}>Open a source</Button>
            <Chip tone="neutral">or press ⌘K</Chip>
          </main>
        )}

        {detailRow && <RowDetail row={detailRow} onClose={() => setDetailRow(null)} />}

        {showHistory && (
          <HistoryPanel history={history} compareId={compareSel?.id ?? null} onOpenRun={openRunResultTab} onCompare={onCompare}
            onRefresh={() => wsId && loadHistory(wsId)} onClose={() => setShowHistory(false)} busy={historyBusy} />
        )}
      </div>

      <CommandPalette open={paletteOpen} items={paletteItems} onClose={() => setPaletteOpen(false)} />
      <RunnerModal open={!!runner?.open} title={runner?.title || ""} sharedSql={runner ? runner.sharedSql : null} jobs={runner?.jobs || []}
        exec={execRun} onOpenResult={openRunResultTab} onClose={() => setRunner(null)} />
    </>
  );
}
