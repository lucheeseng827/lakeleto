// Workspace shell — multi-tab state model + the Postman-style chrome around the per-tab
// TableView: a workspace switcher, the open-tab strip, the sidebar (saved connections + queries +
// file tree), and the run-history panel. The client tab model round-trips through the backend
// `Tab.view` (opaque grid state), so open tabs + their sort/filter/sql survive a reload.
import { useEffect, useState, type CSSProperties, type DragEvent as ReactDragEvent, type ReactNode, type SyntheticEvent } from "react";
import type { Conn, Filters, Row, RunRecord, RunResponse, Sort, Workspace, WsConnection, WsMeta, WsSavedQuery, WsVariable, QueryResp } from "./api";
import { Button, Chip, filterRows, Select, StatTable, TextInput, ThemeToggle } from "./components";

const VAR_RE = /\{\{\s*([\w.-]+)\s*\}\}/g;
/** Substitute `{{key}}` with the workspace variable's value (client-side, Postman-style). Unknown
 *  keys are left verbatim so a missing variable is visible rather than silently blanked. */
export function resolveVars(text: string, vars?: WsVariable[]): string {
  if (!text || !vars || !vars.length) return text;
  const map: Record<string, string> = {};
  for (const v of vars) if (v.key) map[v.key] = v.value;
  return text.replace(VAR_RE, (m, k) => (k in map ? map[k] : m));
}
/** The `{{keys}}` referenced by a string (for highlighting unresolved ones). */
export function usedVars(text: string): string[] {
  const out = new Set<string>(); let m: RegExpExecArray | null;
  const re = new RegExp(VAR_RE);
  while ((m = re.exec(text || ""))) out.add(m[1]);
  return [...out];
}

export type SubView = "Grid" | "Schema" | "Profile" | "SQL";

export interface OpenTab {
  id: string;
  kind: "data" | "result" | "compare" | "launcher";
  title: string;
  path: string;
  sub: SubView;
  sort: Sort | null;
  filters: Filters;
  sql: string;
  connId: string | null;
  // transient (never persisted)
  sqlOut?: RunResponse | null;
  sqlErr?: string | null;
  sqlBusy?: boolean;
  // result tab (opened from history)
  runId?: string;
  run?: RunRecord;
  result?: QueryResp | null;
  resultErr?: string | null;
  // compare tab (two cached runs, diffed)
  runA?: RunRecord;
  runB?: RunRecord;
  resultA?: QueryResp | null;
  resultB?: QueryResp | null;
}

let seq = 0;
export const newTabId = () => `tab-${Date.now().toString(36)}-${(seq++).toString(36)}`;
export const basename = (p: string) => { const i = Math.max(p.lastIndexOf("/"), p.lastIndexOf("\\")); return i >= 0 ? p.slice(i + 1) : p; };
const fmtInt = (n?: number | null) => (n == null ? "?" : Number(n).toLocaleString());

export function agoStr(ms: number): string {
  const s = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  if (s < 60) return s + "s ago";
  const m = Math.floor(s / 60); if (m < 60) return m + "m ago";
  const h = Math.floor(m / 60); if (h < 24) return h + "h ago";
  return Math.floor(h / 24) + "d ago";
}

const DEFAULT_SQL = "SELECT * FROM t LIMIT 20";
export function newDataTab(path: string, o: { connId?: string | null; sub?: SubView; sql?: string; title?: string } = {}): OpenTab {
  return {
    id: newTabId(), kind: "data", title: o.title || basename(path), path,
    sub: o.sub || "Grid", sort: null, filters: {}, sql: o.sql || DEFAULT_SQL, connId: o.connId ?? null,
  };
}

/** A "launcher" tab — the start page shown by `+`: open a source or start a query on a saved
 * connection. Transient (never persisted; buildDoc keeps only `data` tabs). */
export function newLauncherTab(): OpenTab {
  return {
    id: newTabId(), kind: "launcher", title: "New tab", path: "",
    sub: "Grid", sort: null, filters: {}, sql: "", connId: null,
  };
}

// ---- persistence: open tabs <-> backend Workspace.tabs (opaque view state) ----
interface TabView { v: 1; path: string; sub: SubView; sort: Sort | null; filters: Filters; sql: string; title: string; }

export function buildDoc(ws: Workspace, tabs: OpenTab[]): Workspace {
  const persistable = tabs.filter((t) => t.kind === "data");
  return {
    ...ws,
    tabs: persistable.map((t) => ({
      id: t.id, kind: "connection", ref_id: t.connId || "",
      view: { v: 1, path: t.path, sub: t.sub, sort: t.sort, filters: t.filters, sql: t.sql, title: t.title } satisfies TabView,
    })),
  };
}

export function docToTabs(ws: Workspace): OpenTab[] {
  const out: OpenTab[] = [];
  for (const t of ws.tabs || []) {
    const v = t.view as Partial<TabView> | null;
    if (!v || typeof v.path !== "string") continue;
    out.push({
      id: t.id || newTabId(), kind: "data", title: v.title || basename(v.path), path: v.path,
      sub: v.sub || "Grid", sort: v.sort ?? null, filters: v.filters || {}, sql: v.sql || DEFAULT_SQL,
      connId: t.ref_id || null,
    });
  }
  return out;
}

// ======================================================================
// Workspace bar — title, switcher, and workspace-level actions
// ======================================================================
export function WorkspaceBar({ conn, engineName, sqlAvailable, workspaces, wsId, onSelect, onNew, onRename, onDelete, onExport, onImport, busy }: {
  conn: Conn; engineName: string; sqlAvailable: boolean;
  workspaces: WsMeta[]; wsId: string | null;
  onSelect: (id: string) => void; onNew: () => void; onRename: () => void; onDelete: () => void;
  onExport: () => void; onImport: (file: File) => void; busy?: boolean;
}) {
  const header: CSSProperties = { display: "flex", gap: "10px", alignItems: "center", flexWrap: "wrap", padding: "var(--pad-chrome)", borderBottom: "var(--border-hairline)", background: "var(--panel)", flex: "0 0 auto" };
  return (
    <header style={header}>
      <div style={{ display: "flex", flexDirection: "column", lineHeight: 1.15 }}>
        <h1 style={{ fontSize: "var(--text-ui)", margin: 0, fontWeight: "var(--weight-h1)", whiteSpace: "nowrap" }}>
          Lakeleto <span style={{ color: "var(--muted)", fontWeight: "var(--weight-normal)" }}>· the Postman of lakehouse tables</span>
        </h1>
        {conn.caps.version && (
          <span style={{ color: "var(--muted)", fontSize: "var(--text-xs)", fontFamily: "var(--font-mono)" }}>v{conn.caps.version}</span>
        )}
      </div>
      <span style={{ color: "var(--muted)", fontSize: "var(--text-12)" }}>workspace</span>
      <Select value={wsId || ""} onChange={onSelect} options={workspaces.map((w) => ({ value: w.id, label: w.name }))} title="switch workspace" style={{ minWidth: 160 }} />
      <Button size="sm" onClick={onNew} title="new workspace">＋ New</Button>
      <Button size="sm" onClick={onRename} disabled={!wsId} title="rename this workspace">Rename</Button>
      <Button size="sm" onClick={onExport} disabled={!wsId} title="download this workspace as a portable bundle">Export</Button>
      <label style={{ display: "inline-flex" }} title="import a workspace bundle">
        <span style={{ font: "inherit", fontSize: "var(--text-12)", padding: "3px 8px", border: "var(--border-hairline)", borderColor: "var(--line)", borderRadius: "var(--radius-sm)", cursor: "pointer", background: "var(--bg)", color: "var(--fg)", whiteSpace: "nowrap" }}>Import</span>
        <input type="file" accept=".json,application/json" style={{ display: "none" }}
          onChange={(e) => { const f = e.target.files && e.target.files[0]; if (f) onImport(f); e.currentTarget.value = ""; }} />
      </label>
      <Button size="sm" onClick={onDelete} disabled={!wsId} title="delete this workspace">Delete</Button>
      <span style={{ flex: 1 }} />
      {busy && <span style={{ color: "var(--muted)", fontSize: "var(--text-12)" }}>saving…</span>}
      <ThemeToggle />
      <Chip>{engineName}{sqlAvailable ? " · SQL" : ""}</Chip>
      <Chip tone={conn.mode === "live" ? "indigo" : "neutral"}
        title={conn.mode === "live" ? "connected to a running lakeleto serve" : "no server reached — in-memory sample tables + a localStorage workspace store"}>
        {conn.mode === "live" ? "● live" : "○ sample data"}
      </Chip>
    </header>
  );
}

// ======================================================================
// Tab strip — the open-tab bar
// ======================================================================
export function TabStrip({ tabs, activeId, onSelect, onClose, onNew, onRename, onReorder }: {
  tabs: OpenTab[]; activeId: string | null; onSelect: (id: string) => void; onClose: (id: string) => void; onNew: () => void; onRename?: (id: string) => void;
  onReorder?: (fromId: string, toId: string) => void;
}) {
  const bar: CSSProperties = { display: "flex", alignItems: "stretch", gap: 0, borderBottom: "var(--border-hairline)", background: "var(--panel)", overflowX: "auto", flex: "0 0 auto" };
  const [dragId, setDragId] = useState<string | null>(null);
  const [overId, setOverId] = useState<string | null>(null);
  return (
    <div style={bar}>
      {tabs.map((t) => {
        const active = t.id === activeId;
        const isOver = onReorder && overId === t.id && dragId != null && dragId !== t.id;
        return (
          <div key={t.id} onClick={() => onSelect(t.id)} title={t.path}
            draggable={!!onReorder}
            onDragStart={() => onReorder && setDragId(t.id)}
            onDragEnd={() => { setDragId(null); setOverId(null); }}
            onDragOver={(e) => { if (onReorder && dragId && dragId !== t.id) { e.preventDefault(); if (overId !== t.id) setOverId(t.id); } }}
            onDrop={() => { if (onReorder && dragId) onReorder(dragId, t.id); setDragId(null); setOverId(null); }}
            style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 8px 6px 12px", cursor: onReorder ? "grab" : "pointer", whiteSpace: "nowrap",
              borderRight: "var(--border-hairline)", borderBottom: `var(--accent-underline) solid ${active ? "var(--accent)" : "transparent"}`,
              boxShadow: isOver ? "inset 2px 0 0 0 var(--accent)" : undefined,
              opacity: dragId === t.id ? 0.5 : 1,
              background: active ? "var(--bg)" : "transparent", color: active ? "var(--fg)" : "var(--muted)", fontSize: "var(--text-12)", maxWidth: 220 }}>
            <span style={{ opacity: 0.7 }}>{t.kind === "compare" ? "⇄" : t.kind === "result" ? "▤" : t.sub === "SQL" ? "λ" : "▦"}</span>
            <span style={{ overflow: "hidden", textOverflow: "ellipsis" }} onDoubleClick={(e) => { e.stopPropagation(); onRename && onRename(t.id); }} title="double-click to rename">{t.title}</span>
            <span role="button" tabIndex={0} aria-label="close tab"
              onClick={(e) => { e.stopPropagation(); onClose(t.id); }}
              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); e.stopPropagation(); onClose(t.id); } }}
              style={{ marginLeft: 2, padding: "0 4px", borderRadius: 4, color: "var(--muted)" }}>×</span>
          </div>
        );
      })}
      <button onClick={onNew} title="new tab" style={{ border: "none", background: "transparent", cursor: "pointer", color: "var(--muted)", padding: "0 12px", fontSize: "var(--text-ui)" }}>＋</button>
    </div>
  );
}

// ---- a hoverable sidebar row with optional pin / edit / delete actions ----
function SideRow({ icon, label, sub, onClick, onDelete, onPin, pinned, onEdit, title, indent }: {
  icon: string; label: string; sub?: string; onClick: () => void; onDelete?: () => void;
  onPin?: () => void; pinned?: boolean; onEdit?: () => void; title?: string; indent?: boolean;
}) {
  const [hover, setHover] = useState(false);
  const act = (fn?: () => void) => (e: SyntheticEvent) => { e.stopPropagation(); fn && fn(); };
  const IconBtn = ({ on, glyph, lbl, active }: { on?: () => void; glyph: string; lbl: string; active?: boolean }) =>
    on ? <span role="button" tabIndex={0} aria-label={lbl} title={lbl} onClick={act(on)}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); act(on)(e); } }}
      style={{ color: active ? "var(--accent)" : "var(--muted)", padding: "0 3px" }}>{glyph}</span> : null;
  return (
    <div role="button" tabIndex={0} onClick={onClick} title={title}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(); } }}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}
      style={{ display: "flex", gap: "var(--space-4)", alignItems: "center", padding: "3px 6px", paddingLeft: indent ? 18 : 6, borderRadius: 6, cursor: "pointer", fontSize: "var(--text-base)", background: hover ? "var(--hover)" : "transparent" }}>
      <span style={{ width: 14, textAlign: "center", color: "var(--muted)", flex: "0 0 auto" }}>{icon}</span>
      <span style={{ display: "flex", flexDirection: "column", overflow: "hidden", flex: 1 }}>
        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{label}</span>
        {sub && <span style={{ color: "var(--muted)", fontSize: "var(--text-xs)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{sub}</span>}
      </span>
      {(pinned || hover) && <IconBtn on={onPin} glyph={pinned ? "★" : "☆"} lbl={pinned ? "unpin" : "pin"} active={pinned} />}
      {hover && <IconBtn on={onEdit} glyph="✎" lbl="edit description" />}
      {hover && <IconBtn on={onDelete} glyph="×" lbl="remove" />}
    </div>
  );
}

/** A sidebar section: a collapsible header (click to toggle) with an optional drag handle so the
 * user can reorder sections. `drag` is omitted for non-reorderable uses. */
function Section({ title, action, children, collapsed, onToggle, drag }: {
  title: string; action?: ReactNode; children: ReactNode;
  collapsed?: boolean; onToggle?: () => void;
  drag?: {
    onDragStart: () => void; onDragEnd: () => void;
    onDragOver: (e: ReactDragEvent) => void; onDrop: () => void; isTarget?: boolean;
  };
}) {
  return (
    <div style={{ marginBottom: "var(--space-6)", borderRadius: 6, ...(drag?.isTarget ? { outline: "2px dashed var(--accent)", outlineOffset: 2 } : {}) }}
      onDragOver={drag?.onDragOver} onDrop={drag?.onDrop}>
      <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 4 }}>
        {drag && (
          <span draggable onDragStart={drag.onDragStart} onDragEnd={drag.onDragEnd} title="drag to reorder"
            style={{ cursor: "grab", color: "var(--muted)", fontSize: "var(--text-xs)", userSelect: "none", padding: "0 1px" }}>⠿</span>
        )}
        <span role="button" tabIndex={0} onClick={onToggle}
          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onToggle?.(); } }}
          title={collapsed ? "expand" : "collapse"}
          style={{ display: "flex", alignItems: "center", gap: 4, cursor: onToggle ? "pointer" : "default", fontSize: "var(--text-xs)", textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--muted)", fontWeight: "var(--weight-semibold)" }}>
          <span style={{ fontSize: 9, width: 8, display: "inline-block" }}>{collapsed ? "▸" : "▾"}</span>{title}
        </span>
        <span style={{ flex: 1 }} />
        {action}
      </div>
      {!collapsed && children}
    </div>
  );
}

const hint: CSSProperties = { color: "var(--muted)", fontSize: "var(--text-xs)", padding: "2px 6px" };
const byPinned = <T extends { pinned?: boolean }>(a: T, b: T) => (b.pinned ? 1 : 0) - (a.pinned ? 1 : 0);

// ---- variables editor (Postman-style {{key}} environment) ----
function VariablesEditor({ variables, mutate }: { variables: WsVariable[]; mutate: (fn: (ws: Workspace) => Workspace) => void }) {
  const set = (i: number, patch: Partial<WsVariable>) => mutate((w) => ({ ...w, variables: (w.variables || []).map((v, j) => (j === i ? { ...v, ...patch } : v)) }));
  const del = (i: number) => mutate((w) => ({ ...w, variables: (w.variables || []).filter((_, j) => j !== i) }));
  const add = () => mutate((w) => ({ ...w, variables: [...(w.variables || []), { key: "", value: "" }] }));
  return (
    <div>
      {variables.length === 0 && <div style={hint}>Add a variable, then use <code>{"{{key}}"}</code> in any SQL or path.</div>}
      {variables.map((v, i) => (
        <div key={i} style={{ display: "flex", gap: 4, alignItems: "center", marginBottom: 3 }}>
          <TextInput value={v.key} onChange={(x) => set(i, { key: x })} placeholder="key" size="sm" style={{ flex: "0 0 40%" }} />
          <span style={{ color: "var(--muted)" }}>=</span>
          <TextInput value={v.value} onChange={(x) => set(i, { value: x })} placeholder="value" size="sm" />
          <span role="button" tabIndex={0} aria-label="remove variable" onClick={() => del(i)}
            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); del(i); } }}
            style={{ color: "var(--muted)", padding: "0 3px", cursor: "pointer" }}>×</span>
        </div>
      ))}
      <Button size="sm" onClick={add} style={{ marginTop: 2 }}>＋ Variable</Button>
    </div>
  );
}

type ConnKind = "sqlite" | "postgres" | "mysql" | "file";
// Split a stored connection back into editable form fields (kind / path / table / label / desc).
function connToForm(c: WsConnection): { kind: ConnKind; path: string; table: string } {
  const m = /^(sqlite|postgres|postgresql|mysql):/i.exec(c.path);
  if (m) {
    const [base, qs] = c.path.split("?");
    const table = new URLSearchParams(qs || "").get("table") || "";
    const scheme = m[1].toLowerCase();
    if (scheme === "sqlite") return { kind: "sqlite", path: base.replace(/^sqlite:\/\/\/?/i, ""), table };
    return { kind: scheme === "mysql" ? "mysql" : "postgres", path: base, table };
  }
  return { kind: "file", path: c.path, table: "" };
}

// Add/edit connection form: a local file/URI, or a SQLite database (built into a
// `sqlite:///…?table=` URI). Shown inline in the Connections section via ＋ (add) or the ✎ on a
// row (edit its properties). `initial` prefills for editing.
function AddConnectionForm({ initial, onSubmit, onCancel }: {
  initial?: WsConnection | null;
  onSubmit: (c: { label: string; path: string; format?: string | null; description?: string | null }) => void;
  onCancel: () => void;
}) {
  const pre = initial ? connToForm(initial) : null;
  const [kind, setKind] = useState<ConnKind>(pre?.kind ?? "sqlite");
  const [path, setPath] = useState(pre?.path ?? "");
  const [table, setTable] = useState(pre?.table ?? "");
  const [label, setLabel] = useState(initial?.label ?? "");
  const [desc, setDesc] = useState(initial?.description ?? "");
  const isDb = kind !== "file";
  const submit = () => {
    const p = path.trim();
    if (!p) return;
    const description = desc.trim() || null;
    const withTable = (uri: string) => {
      const t = table.trim();
      return t && !/[?&]table=/.test(uri) ? uri + (uri.includes("?") ? "&" : "?") + "table=" + encodeURIComponent(t) : uri;
    };
    if (kind === "sqlite") {
      const file = p.replace(/\\/g, "/");
      // A non-sqlite URI pasted into the SQLite tab (e.g. s3:// / gs:// / az://) is a plain source,
      // not a SQLite file — don't wrap it in sqlite:///. (Handles the common mistake.)
      if (/^[a-z][a-z0-9+.-]*:\/\//i.test(file) && !/^sqlite:/i.test(file)) {
        onSubmit({ label: label.trim() || basename(file.split("?")[0]), path: file, format: null, description });
        return;
      }
      const uri = withTable(/^sqlite:/i.test(file) ? file : `sqlite:///${file.replace(/^\/+/, "")}`);
      onSubmit({ label: label.trim() || table.trim() || basename(file.split("?")[0]), path: uri, format: "database", description });
    } else if (kind === "postgres" || kind === "mysql") {
      const scheme = kind === "mysql" ? "mysql" : "postgres";
      const uri = withTable(/^(postgres|postgresql|mysql):\/\//i.test(p) ? p : `${scheme}://${p}`);
      onSubmit({ label: label.trim() || table.trim() || kind, path: uri, format: "database", description });
    } else {
      onSubmit({ label: label.trim() || basename(p), path: p, format: null, description });
    }
  };
  const lbl: CSSProperties = { fontSize: "var(--text-xs)", color: "var(--muted)", display: "block", marginBottom: 2 };
  const field: CSSProperties = { marginBottom: 6 };
  const pathLabel = kind === "sqlite" ? "Database file path" : kind === "file" ? "File path or URI" : "Connection URI";
  const pathPlaceholder = kind === "sqlite" ? "C:\\data\\app.db"
    : kind === "postgres" ? "postgres://user:{{PGPASS}}@host:5432/db"
    : kind === "mysql" ? "mysql://user:{{MYSQLPASS}}@host:3306/db"
    : "C:\\data\\file.parquet  ·  s3://bucket/x.parquet";
  return (
    <div style={{ border: "var(--border-hairline)", borderRadius: "var(--radius-sm)", padding: "var(--space-5)", marginBottom: 8, background: "var(--bg)" }}>
      <div style={{ display: "flex", gap: 3, marginBottom: 8 }}>
        {(["sqlite", "postgres", "mysql", "file"] as const).map((k) => (
          <button key={k} onClick={() => setKind(k)}
            style={{ flex: 1, padding: "4px 2px", fontSize: "var(--text-xs)", border: "var(--border-hairline)", borderRadius: "var(--radius-sm)", cursor: "pointer",
              background: kind === k ? "var(--accent)" : "var(--panel)", color: kind === k ? "var(--accent-fg)" : "var(--fg)" }}>
            {k === "sqlite" ? "SQLite" : k === "postgres" ? "Postgres" : k === "mysql" ? "MySQL" : "File"}
          </button>
        ))}
      </div>
      <div style={field}>
        <label style={lbl}>{pathLabel}</label>
        <TextInput value={path} onChange={setPath} placeholder={pathPlaceholder}
          onKeyDown={(e) => { if (e.key === "Enter") submit(); }} />
      </div>
      {(kind === "postgres" || kind === "mysql") && (
        <div style={{ ...lbl, marginTop: -2, marginBottom: 6 }}>
          Tip: put the password in a <code>{"{{VAR}}"}</code> and define it under Variables, so it isn't stored in the workspace file.
        </div>
      )}
      {isDb && (
        <div style={field}>
          <label style={lbl}>Table (optional — leave blank to browse tables)</label>
          <TextInput value={table} onChange={setTable} placeholder="orders" onKeyDown={(e) => { if (e.key === "Enter") submit(); }} />
        </div>
      )}
      <div style={field}>
        <label style={lbl}>Label (optional)</label>
        <TextInput value={label} onChange={setLabel} placeholder="my connection" onKeyDown={(e) => { if (e.key === "Enter") submit(); }} />
      </div>
      <div style={field}>
        <label style={lbl}>Description (optional)</label>
        <TextInput value={desc} onChange={setDesc} mono={false} placeholder="notes" onKeyDown={(e) => { if (e.key === "Enter") submit(); }} />
      </div>
      <div style={{ display: "flex", gap: 6, justifyContent: "flex-end", marginTop: 2 }}>
        <Button size="sm" onClick={onCancel}>Cancel</Button>
        <Button size="sm" variant="primary" disabled={!path.trim()} onClick={submit}>{initial ? "Save" : "Add"}</Button>
      </div>
    </div>
  );
}

// ======================================================================
// Sidebar — connections, saved queries (grouped into folders), variables, files
// ======================================================================
export function Sidebar({ ws, listing, mutate, onOpenConnection, onOpenQuery, onRunFolder, onOpenFile, onOpenDir, ee }: {
  ws: Workspace | null;
  listing: { dir: string; parent?: string | null; entries: { name: string; path: string; kind: "dir" | "file"; size?: number | null }[] } | null;
  mutate: (fn: (ws: Workspace) => Workspace) => void;
  onOpenConnection: (c: WsConnection) => void; onOpenQuery: (q: WsSavedQuery) => void;
  onRunFolder: (folder: string) => void;
  onOpenFile: (p: string) => void; onOpenDir: (d: string) => void;
  ee?: boolean;   // Lakeleto Cloud edition — lifts the open-source DB-connection cap
}) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({}); // saved-query FOLDER collapse (keyed by folder name)
  const aside: CSSProperties = { flex: "0 0 260px", borderRight: "var(--border-hairline)", overflow: "auto", padding: "var(--space-5)", background: "var(--panel)", display: "flex", flexDirection: "column" };

  // --- reorderable + collapsible sidebar sections (persisted) ---
  // Files is first by default — it's the "browse to your data" entry point.
  const DEFAULT_ORDER = ["files", "connections", "queries", "variables"];
  const [order, setOrder] = useState<string[]>(() => {
    try {
      const s = JSON.parse(localStorage.getItem("lakeleto-sidebar-order") || "null");
      if (Array.isArray(s) && s.length === DEFAULT_ORDER.length && DEFAULT_ORDER.every((x) => s.includes(x))) return s;
    } catch { /* ignore */ }
    return DEFAULT_ORDER;
  });
  const [secCollapsed, setSecCollapsed] = useState<Record<string, boolean>>(() => {
    try { return JSON.parse(localStorage.getItem("lakeleto-sidebar-collapsed") || "{}"); } catch { return {}; }
  });
  const [dragId, setDragId] = useState<string | null>(null);
  const [overId, setOverId] = useState<string | null>(null);
  useEffect(() => { try { localStorage.setItem("lakeleto-sidebar-order", JSON.stringify(order)); } catch { /* ignore */ } }, [order]);
  useEffect(() => { try { localStorage.setItem("lakeleto-sidebar-collapsed", JSON.stringify(secCollapsed)); } catch { /* ignore */ } }, [secCollapsed]);
  const toggleSec = (id: string) => setSecCollapsed((c) => ({ ...c, [id]: !c[id] }));
  const reorder = (from: string, to: string) => {
    if (from === to) return;
    setOrder((o) => { const a = o.filter((x) => x !== from); const i = a.indexOf(to); a.splice(i < 0 ? a.length : i, 0, from); return a; });
  };
  const connections = ws?.connections || [];
  const queries = ws?.saved_queries || [];
  const variables = ws?.variables || [];

  const editDesc = (kind: "conn" | "query", id: string, cur?: string | null) => {
    const d = window.prompt("Description", cur || ""); if (d == null) return;
    const val = d.trim() || null;
    if (kind === "conn") mutate((w) => ({ ...w, connections: w.connections.map((c) => (c.id === id ? { ...c, description: val } : c)) }));
    else mutate((w) => ({ ...w, saved_queries: w.saved_queries.map((q) => (q.id === id ? { ...q, description: val } : q)) }));
  };
  const pinConn = (id: string) => mutate((w) => ({ ...w, connections: w.connections.map((c) => (c.id === id ? { ...c, pinned: !c.pinned } : c)) }));
  const delConn = (id: string) => mutate((w) => ({ ...w, connections: w.connections.filter((c) => c.id !== id) }));
  const pinQuery = (id: string) => mutate((w) => ({ ...w, saved_queries: w.saved_queries.map((q) => (q.id === id ? { ...q, pinned: !q.pinned } : q)) }));
  const delQuery = (id: string) => mutate((w) => ({ ...w, saved_queries: w.saved_queries.filter((q) => q.id !== id) }));

  // Add/edit-connection form (a file path or a database URI) shown inline in the Connections section.
  const [addingConn, setAddingConn] = useState(false);
  const [editingConn, setEditingConn] = useState<WsConnection | null>(null);
  const [capMsg, setCapMsg] = useState<string | null>(null);
  type ConnForm = { label: string; path: string; format?: string | null; description?: string | null };
  // Open-source edition connects up to 2 databases at once (any mix of SQLite/Postgres/MySQL);
  // Lakeleto Cloud (ee) lifts the cap. File/object connections are uncapped.
  const OSS_DB_LIMIT = 2;
  const addConn = (c: ConnForm) => {
    if (c.format === "database" && !ee) {
      const have = (ws?.connections || []).filter((x) => x.format === "database").length;
      if (have >= OSS_DB_LIMIT) {
        setCapMsg(`Open-source Lakeleto connects up to ${OSS_DB_LIMIT} databases at once. Lakeleto Cloud unlocks unlimited connections + more databases.`);
        return;
      }
    }
    setCapMsg(null);
    const conn: WsConnection = { id: "conn-" + newTabId(), label: c.label, path: c.path, format: c.format ?? null, description: c.description ?? null };
    mutate((w) => ({ ...w, connections: [...w.connections, conn] }));
    setAddingConn(false);
    onOpenConnection(conn);
  };
  const updateConn = (id: string, c: ConnForm) => {
    mutate((w) => ({ ...w, connections: w.connections.map((x) => (x.id === id ? { ...x, label: c.label, path: c.path, format: c.format ?? null, description: c.description ?? null } : x)) }));
    setEditingConn(null);
  };

  // group queries by folder (undefined → ungrouped, rendered first)
  const folders = new Map<string, WsSavedQuery[]>();
  const ungrouped: WsSavedQuery[] = [];
  for (const q of [...queries].sort(byPinned)) {
    if (q.folder) { if (!folders.has(q.folder)) folders.set(q.folder, []); folders.get(q.folder)!.push(q); }
    else ungrouped.push(q);
  }
  const queryRow = (q: WsSavedQuery, indent?: boolean) => (
    <SideRow key={q.id} icon="λ" label={q.name} sub={q.description || undefined} indent={indent}
      onClick={() => onOpenQuery(q)} onDelete={() => delQuery(q.id)} onPin={() => pinQuery(q.id)} pinned={q.pinned}
      onEdit={() => editDesc("query", q.id, q.description)} title={q.sql} />
  );

  // Each section rendered by id, so the order can be reordered + persisted.
  const sections: Record<string, { title: string; body: ReactNode; action?: ReactNode }> = {
    files: {
      title: `Files${listing ? ` · ${listing.entries.filter((e) => e.kind === "dir").length} dirs / ${listing.entries.filter((e) => e.kind === "file").length} files` : ""}`,
      body: listing ? (
        <div>
          {listing.parent != null && (
            <SideRow icon="↑" label=".. (up a folder)" onClick={() => onOpenDir(listing.parent!)} title={listing.parent} />
          )}
          {listing.entries.map((e) => (
            <SideRow key={e.path} icon={e.kind === "dir" ? "▸" : "▦"} label={e.name}
              sub={e.kind === "dir" ? "folder — click to open" : undefined}
              onClick={() => (e.kind === "dir" ? onOpenDir(e.path) : onOpenFile(e.path))} title={e.path} />
          ))}
          {listing.entries.length === 0 && listing.parent == null && <div style={hint}>Empty folder.</div>}
        </div>
      ) : <div style={hint}>Start the server with a folder — <code>serve --root your-folder</code> — to browse files here.</div>,
    },
    connections: {
      title: `Connections (${connections.length})`,
      action: (
        <span role="button" tabIndex={0} title="add a connection (file or database)"
          onClick={(e) => { e.stopPropagation(); setEditingConn(null); setAddingConn((v) => !v); }}
          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setEditingConn(null); setAddingConn((v) => !v); } }}
          style={{ cursor: "pointer", color: "var(--muted)", fontSize: "var(--text-ui)", lineHeight: 1, padding: "0 2px" }}>＋</span>
      ),
      body: (
        <>
          {(addingConn || editingConn) && (
            <AddConnectionForm initial={editingConn}
              onSubmit={editingConn ? (c) => updateConn(editingConn.id, c) : addConn}
              onCancel={() => { setAddingConn(false); setEditingConn(null); setCapMsg(null); }} />
          )}
          {capMsg && (
            <div style={{ border: "var(--border-hairline)", borderColor: "var(--warn-fg)", background: "var(--warn-bg)", color: "var(--warn-fg)", borderRadius: "var(--radius-sm)", padding: "6px 8px", marginBottom: 6, fontSize: "var(--text-12)" }}>
              {capMsg} <span role="button" tabIndex={0} onClick={() => setCapMsg(null)} style={{ cursor: "pointer", textDecoration: "underline" }}>dismiss</span>
            </div>
          )}
          {connections.length === 0 && !addingConn && <div style={hint}>Add a source with ＋, or save one with ⭑ in a tab.</div>}
          {[...connections].sort(byPinned).map((c) => (
            <SideRow key={c.id} icon={c.format === "database" ? "🗄" : "▤"} label={c.label}
              sub={c.description || (c.format === "database" ? "database" : undefined)}
              onClick={() => onOpenConnection(c)} onDelete={() => delConn(c.id)} onPin={() => pinConn(c.id)} pinned={c.pinned}
              onEdit={() => { setAddingConn(false); setEditingConn(c); }} title={c.path} />
          ))}
        </>
      ),
    },
    queries: {
      title: `Saved queries (${queries.length})`,
      body: (
        <>
          {queries.length === 0 && <div style={hint}>Save a query from the SQL tab (name it <code>folder/name</code> to group it).</div>}
          {ungrouped.map((q) => queryRow(q))}
          {[...folders.entries()].map(([folder, qs]) => {
            const open = !collapsed[folder];
            return (
              <div key={folder}>
                <div style={{ display: "flex", alignItems: "center", gap: 4, padding: "3px 6px", cursor: "pointer", borderRadius: 6 }}>
                  <span role="button" tabIndex={0} onClick={() => setCollapsed((c) => ({ ...c, [folder]: open }))}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setCollapsed((c) => ({ ...c, [folder]: open })); } }}
                    style={{ flex: 1, display: "flex", gap: 4, alignItems: "center", color: "var(--muted)", fontSize: "var(--text-12)" }}>
                    <span>{open ? "▾" : "▸"}</span><span>▦ {folder}</span><span>({qs.length})</span>
                  </span>
                  <span role="button" tabIndex={0} aria-label="run all in folder" title="run every query in this folder"
                    onClick={() => onRunFolder(folder)} onKeyDown={(e) => { if (e.key === "Enter") onRunFolder(folder); }}
                    style={{ color: "var(--muted)", padding: "0 3px", cursor: "pointer" }}>▶</span>
                </div>
                {open && qs.map((q) => queryRow(q, true))}
              </div>
            );
          })}
          {queries.length > 0 && <div style={{ ...hint, marginTop: 2 }}>Right-click a query row's ▶… use “Run across” from ⌘K.</div>}
        </>
      ),
    },
    variables: {
      title: `Variables (${variables.length})`,
      body: <VariablesEditor variables={variables} mutate={mutate} />,
    },
  };

  return (
    <aside style={aside}>
      {order.filter((id) => sections[id]).map((id) => (
        <Section key={id} title={sections[id].title} action={sections[id].action}
          collapsed={!!secCollapsed[id]} onToggle={() => toggleSec(id)}
          drag={{
            onDragStart: () => setDragId(id),
            onDragEnd: () => { setDragId(null); setOverId(null); },
            onDragOver: (e) => { e.preventDefault(); if (dragId && dragId !== id && overId !== id) setOverId(id); },
            onDrop: () => { if (dragId) reorder(dragId, id); setDragId(null); setOverId(null); },
            isTarget: overId === id && dragId != null && dragId !== id,
          }}>
          {sections[id].body}
        </Section>
      ))}
    </aside>
  );
}

// ======================================================================
// History panel — the workspace run store
// ======================================================================
export function HistoryPanel({ history, onOpenRun, onCompare, compareId, onRefresh, onClose, busy }: {
  history: RunRecord[]; onOpenRun: (r: RunRecord) => void; onCompare: (r: RunRecord) => void; compareId?: string | null;
  onRefresh: () => void; onClose: () => void; busy?: boolean;
}) {
  const panel: CSSProperties = { flex: "0 0 300px", borderLeft: "var(--border-hairline)", overflow: "auto", background: "var(--panel)", display: "flex", flexDirection: "column" };
  return (
    <aside style={panel}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, padding: "var(--pad-toolbar)", borderBottom: "var(--border-hairline)" }}>
        <span style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--muted)", fontWeight: "var(--weight-semibold)" }}>History</span>
        <span style={{ flex: 1 }} />
        <Button size="sm" onClick={onRefresh} title="refresh history">↻</Button>
        <Button size="sm" onClick={onClose} title="hide history">×</Button>
      </div>
      <div style={{ padding: "var(--space-4)" }}>
        {busy && <div style={{ color: "var(--muted)", fontSize: "var(--text-xs)", padding: "4px 6px" }}>loading…</div>}
        {!busy && history.length === 0 && <div style={{ color: "var(--muted)", fontSize: "var(--text-xs)", padding: "4px 6px" }}>No runs yet. Run a query in a SQL tab.</div>}
        {compareId && <div style={{ ...hint, color: "var(--accent)" }}>compare: baseline A picked — click ⇄ on another cached run.</div>}
        {history.map((r) => {
          const ok = r.status === "ok";
          const isBase = compareId === r.id;
          return (
            <div key={r.id} style={{ padding: "6px 8px", borderRadius: 6, marginBottom: 4, border: "var(--border-hairline)", borderColor: isBase ? "var(--accent)" : "var(--line)" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: "var(--text-12)" }}>
                <span style={{ color: ok ? "var(--accent)" : "var(--err-fg)" }}>{ok ? "●" : "▲"}</span>
                <span role="button" tabIndex={0} onClick={() => onOpenRun(r)}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onOpenRun(r); } }}
                  title={r.cached ? "open the cached result" : ok ? "re-run" : r.error || "failed run"}
                  style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontFamily: "var(--font-mono)", cursor: "pointer" }}>
                  {r.sql ? r.sql.replace(/\s+/g, " ").trim() : "scan " + basename(r.source_path)}
                </span>
                {r.cached && <span role="button" tabIndex={0} aria-label="compare" title="pick for compare (A, then B)"
                  onClick={() => onCompare(r)} onKeyDown={(e) => { if (e.key === "Enter") onCompare(r); }}
                  style={{ color: isBase ? "var(--accent)" : "var(--muted)", cursor: "pointer", padding: "0 2px" }}>⇄</span>}
                {r.cached && <Chip style={{ fontSize: "var(--text-xs)", padding: "1px 6px" }}>cached</Chip>}
              </div>
              <div style={{ display: "flex", gap: 8, color: "var(--muted)", fontSize: "var(--text-xs)", marginTop: 3 }}>
                <span>{basename(r.source_path)}</span>
                <span style={{ flex: 1 }} />
                {ok ? <span>{fmtInt(r.row_count)} rows</span> : <span>error</span>}
                <span>{r.duration_ms}ms</span>
                <span>{agoStr(r.at_ms)}</span>
              </div>
            </div>
          );
        })}
      </div>
    </aside>
  );
}

// ======================================================================
// ResultView — a read-only grid over a cached run result (opened from history)
// ======================================================================
export function ResultView({ tab, onOpenRow }: { tab: OpenTab; onOpenRow: (r: Row) => void }) {
  const [search, setSearch] = useState("");
  const pane: CSSProperties = { flex: "1 1 auto", minHeight: 0, display: "flex", flexDirection: "column" };
  const meta: CSSProperties = { padding: "var(--pad-toolbar)", borderBottom: "var(--border-hairline)", color: "var(--muted)", fontSize: "var(--text-12)", display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" };
  const r = tab.run;
  const rows = tab.result ? filterRows(tab.result.rows, search) : [];
  return (
    <main style={pane}>
      <div style={meta}>
        <Chip tone="neutral">cached result</Chip>
        <span style={{ fontFamily: "var(--font-mono)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 420 }}>
          {r?.sql ? r.sql.replace(/\s+/g, " ").trim() : "scan " + (r ? basename(r.source_path) : "")}
        </span>
        <span style={{ flex: 1 }} />
        {tab.result && <div style={{ width: 180 }}><TextInput value={search} onChange={setSearch} placeholder="search…" size="sm" /></div>}
        {r && <span>{fmtInt(r.row_count)} rows · {r.duration_ms}ms · {agoStr(r.at_ms)}</span>}
      </div>
      {tab.resultErr && <div style={{ padding: "var(--gutter)", color: "var(--err-fg)", fontSize: "var(--text-base)" }}>{tab.resultErr}</div>}
      {!tab.resultErr && tab.result && (
        <div style={{ overflow: "auto", minHeight: 0, padding: "var(--gutter)" }}>
          <StatTable columns={tab.result.columns.map((c) => ({ key: c.name, label: c.name }))} rows={rows} onRowClick={onOpenRow} />
          {search.trim() !== "" && <div style={{ color: "var(--muted)", fontSize: "var(--text-xs)", marginTop: 6 }}>{fmtInt(rows.length)} of {fmtInt(tab.result.rows.length)} rows match “{search}”</div>}
        </div>
      )}
      {!tab.resultErr && !tab.result && <div style={{ padding: "var(--gutter)", color: "var(--muted)" }}>loading cached result…</div>}
    </main>
  );
}
