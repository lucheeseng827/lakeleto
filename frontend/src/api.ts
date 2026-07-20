// Lakeleto API client — speaks the exact `/v1/*` contract from `src/api.rs`.
// Ported from the design system's `lakeleto-api.js` (production-liftable).
//
// Two interchangeable backends behind one async interface:
//   • LakeletoHttpClient — real fetch() against a running `lakeleto serve` (or Lakeleto Cloud;
//     same contract). Bearer token via Authorization header.
//   • LakeletoMockBackend — in-memory sample tables implementing the identical wire shapes,
//     so the UI is fully functional offline.
//
// connectLakeleto({base, token}) probes GET /v1/engines and returns whichever is live.
// Point the UI at a real server by opening it with ?api=http://127.0.0.1:8080&token=<TOKEN>.

// Thrown by LakeletoHttpClient when the server actually answered (with a non-2xx
// status) — as opposed to a network-level failure (no `status`), which is the
// only case connectLakeleto should treat as "no server, fall back to sample".
export class ApiError extends Error {
  status?: number;
  constructor(message: string, status?: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export interface Column { name: string; data_type: string; nullable: boolean; }
export interface Sort { col: string; desc: boolean; }
export type Filters = Record<string, string>;
export interface Row { [k: string]: unknown; }

export interface Engines {
  version?: string;   // the running server's version (CARGO_PKG_VERSION); absent in sample mode
  engine: { engine: string; formats: string[]; sql: boolean; profile: boolean; remote: boolean };
  sql_available: boolean;
  endpoints: string[];
}
export interface Entry { name: string; path: string; kind: "dir" | "file"; size?: number | null; }
export interface Listing { dir: string; parent?: string | null; entries: Entry[]; }
export interface SchemaResp { source: string; format: string; engine: string; row_count?: number | null; columns: Column[]; }
export interface InfoResp { path: string; format: string; engine: string; size_bytes?: number | null; row_count?: number | null; columns: number; }
export interface RowsResp {
  columns: Column[]; offset: number; num_rows: number; matched_rows: number;
  total_known: boolean; scanned_rows: number; bounded: boolean; rows: Row[];
}
export interface ProfileColumn {
  name: string; data_type: string; null_count: number; null_fraction: number;
  distinct: number; distinct_capped: boolean; min: string | null; max: string | null; sample: string[];
}
export interface Profile { source?: string; engine: string; row_count?: number | null; scanned_rows: number; columns: ProfileColumn[]; }
export interface QueryResp { columns: Column[]; num_rows: number; rows: Row[]; }

// ---- workspace data plane (mirrors src/workspace.rs over /v1/workspaces/*) ----
export interface WsMeta { id: string; name: string; created_at_ms: number; updated_at_ms: number; connection_count: number; query_count: number; }
export interface WsConnection { id: string; label: string; path: string; format?: string | null; description?: string | null; pinned?: boolean; }
export interface WsSavedQuery { id: string; name: string; sql: string; connection_id?: string | null; description?: string | null; folder?: string | null; pinned?: boolean; }
export interface WsTab { id: string; kind: string; ref_id: string; view: unknown; }
export interface WsVariable { key: string; value: string; }
export interface Workspace {
  id: string; name: string; created_at_ms: number; updated_at_ms: number;
  connections: WsConnection[]; saved_queries: WsSavedQuery[]; tabs: WsTab[]; variables?: WsVariable[];
}
export type RunStatus = "ok" | "error";
export interface RunRecord {
  id: string; at_ms: number; sql?: string | null; source_path: string; format?: string | null;
  status: RunStatus; error?: string | null; row_count?: number | null; duration_ms: number; cached: boolean;
}
export interface WorkspaceBundle { bundle_version: number; workspace: Workspace; history: RunRecord[]; }
export interface RunResponse { run: RunRecord; columns: Column[]; num_rows: number; rows: Row[]; }
export interface RunReq { sql?: string | null; path: string; format?: string | null; limit?: number; preview?: number; }
export const BUNDLE_VERSION = 1;

export interface Backend {
  engines(): Promise<Engines>;
  list(dir: string): Promise<Listing>;
  schema(path: string): Promise<SchemaResp>;
  info(path: string): Promise<InfoResp>;
  profile(path: string, scan?: number): Promise<Profile>;
  rows(o: { path: string; offset?: number; limit?: number; sort?: Sort | null; filters?: Filters; cols?: string[] | null }): Promise<RowsResp>;
  stats(o: { path: string; filters?: Filters }): Promise<Profile>;
  query(o: { sql: string; file?: string | null; tables?: string[] }): Promise<QueryResp>;
  exportUrl(o: { path: string; fmt?: string; sort?: Sort | null; filters?: Filters; cols?: string[] | null }): string | null;
  // workspace data plane
  wsList(): Promise<WsMeta[]>;
  wsCreate(name: string): Promise<Workspace>;
  wsGet(id: string): Promise<Workspace>;
  wsSave(id: string, ws: Workspace): Promise<Workspace>;
  wsDelete(id: string): Promise<void>;
  wsHistory(id: string): Promise<RunRecord[]>;
  wsRun(id: string, req: RunReq): Promise<RunResponse>;
  wsRunResult(id: string, runId: string, offset?: number, limit?: number): Promise<QueryResp>;
  wsExport(id: string): Promise<WorkspaceBundle>;
  wsImport(bundle: WorkspaceBundle): Promise<Workspace>;
}

export interface Conn { mode: "live" | "sample"; base: string | null; caps: Engines; backend: Backend; }

export const OP_SYMBOL: Record<string, string> = { ge: ">=", le: "<=", ne: "!=", eq: "=", gt: ">", lt: "<", contains: "~" };

// ---- shared: filter-text → {op,value} (matches the SPA's parseFilter) ----
export function parseFilter(raw: string): { op: string; value: string } | null {
  raw = (raw || "").trim();
  if (raw === "") return null;
  const m = raw.match(/^(>=|<=|!=|=|>|<|~)?\s*([\s\S]*)$/)!;
  const map: Record<string, string> = { ">=": "ge", "<=": "le", "!=": "ne", "=": "eq", ">": "gt", "<": "lt", "~": "contains" };
  return { op: map[m[1]] || "contains", value: m[2] };
}
function filterSpecs(filters: Filters): string[] {
  const out: string[] = [];
  for (const [col, raw] of Object.entries(filters || {})) {
    const f = parseFilter(raw);
    if (f && f.value !== "") out.push(`${col}:${f.op}:${f.value}`);
  }
  return out;
}

// ======================================================================
// HTTP client — the real backend
// ======================================================================
export class LakeletoHttpClient implements Backend {
  base: string;
  token: string;
  constructor({ base = "", token = "" }: { base?: string; token?: string } = {}) {
    this.base = base.replace(/\/$/, "");
    this.token = token;
  }
  private headers(extra?: Record<string, string>): Record<string, string> {
    const h = Object.assign({}, extra || {});
    if (this.token) h["Authorization"] = "Bearer " + this.token;
    return h;
  }
  private async get<T>(path: string): Promise<T> {
    const r = await fetch(this.base + path, { headers: this.headers(), signal: AbortSignal.timeout(30_000) });
    const b = await r.json().catch(() => ({ error: `HTTP ${r.status}` }));
    if (!r.ok) throw new ApiError((b as { error?: string }).error || `HTTP ${r.status}`, r.status);
    return b as T;
  }
  private async post<T>(path: string, payload: unknown): Promise<T> {
    return this.body<T>("POST", path, payload);
  }
  private async body<T>(method: string, path: string, payload: unknown): Promise<T> {
    const r = await fetch(this.base + path, {
      method, headers: this.headers({ "content-type": "application/json" }),
      body: JSON.stringify(payload), signal: AbortSignal.timeout(30_000),
    });
    const b = await r.json().catch(() => ({ error: `HTTP ${r.status}` }));
    if (!r.ok) throw new ApiError((b as { error?: string }).error || `HTTP ${r.status}`, r.status);
    return b as T;
  }
  engines() { return this.get<Engines>("/v1/engines"); }
  // Omit `dir` entirely when empty so the server browses its default root — sending `?dir=`
  // (empty value) makes the server confine against an empty path and 403 under `--root`.
  list(dir: string) { return this.get<Listing>("/v1/list" + (dir ? "?dir=" + encodeURIComponent(dir) : "")); }
  schema(path: string) { return this.get<SchemaResp>("/v1/schema?path=" + encodeURIComponent(path)); }
  info(path: string) { return this.get<InfoResp>("/v1/info?path=" + encodeURIComponent(path)); }
  profile(path: string, scan?: number) {
    const p = new URLSearchParams({ path });
    if (scan != null) p.set("scan", String(scan));
    return this.get<Profile>("/v1/profile?" + p);
  }
  rows({ path, offset = 0, limit = 100, sort = null, filters = {}, cols = null }: Parameters<Backend["rows"]>[0]) {
    const p = new URLSearchParams({ path, offset: String(offset), limit: String(limit) });
    if (sort) { p.set("sort", sort.col); p.set("desc", sort.desc ? "1" : "0"); }
    for (const f of filterSpecs(filters)) p.append("filter", f);
    if (cols && cols.length) p.set("cols", cols.join(","));
    return this.get<RowsResp>("/v1/rows?" + p);
  }
  stats({ path, filters = {} }: Parameters<Backend["stats"]>[0]) {
    const p = new URLSearchParams({ path });
    for (const f of filterSpecs(filters)) p.append("filter", f);
    return this.get<Profile>("/v1/stats?" + p);
  }
  query({ sql, file = null, tables = [] }: Parameters<Backend["query"]>[0]) {
    return this.post<QueryResp>("/v1/query", { sql, file, tables });
  }
  exportUrl({ path, fmt = "csv", sort = null, filters = {}, cols = null }: Parameters<Backend["exportUrl"]>[0]) {
    const p = new URLSearchParams({ path, fmt });
    if (sort) { p.set("sort", sort.col); p.set("desc", sort.desc ? "1" : "0"); }
    for (const f of filterSpecs(filters)) p.append("filter", f);
    if (cols && cols.length) p.set("cols", cols.join(","));
    if (this.token) p.set("token", this.token);
    return this.base + "/v1/export?" + p;
  }
  // ---- workspace data plane ----
  async wsList() { return (await this.get<{ workspaces: WsMeta[] }>("/v1/workspaces")).workspaces; }
  wsCreate(name: string) { return this.post<Workspace>("/v1/workspaces", { name }); }
  wsGet(id: string) { return this.get<Workspace>("/v1/workspaces/" + encodeURIComponent(id)); }
  wsSave(id: string, ws: Workspace) { return this.body<Workspace>("PUT", "/v1/workspaces/" + encodeURIComponent(id), ws); }
  async wsDelete(id: string) { await this.body<unknown>("DELETE", "/v1/workspaces/" + encodeURIComponent(id), {}); }
  async wsHistory(id: string) { return (await this.get<{ history: RunRecord[] }>("/v1/workspaces/" + encodeURIComponent(id) + "/history")).history; }
  wsRun(id: string, req: RunReq) { return this.post<RunResponse>("/v1/workspaces/" + encodeURIComponent(id) + "/runs", req); }
  wsRunResult(id: string, runId: string, offset = 0, limit = 200) {
    const p = new URLSearchParams({ offset: String(offset), limit: String(limit) });
    return this.get<QueryResp>("/v1/workspaces/" + encodeURIComponent(id) + "/runs/" + encodeURIComponent(runId) + "?" + p);
  }
  wsExport(id: string) { return this.get<WorkspaceBundle>("/v1/workspaces/" + encodeURIComponent(id) + "/export"); }
  wsImport(bundle: WorkspaceBundle) { return this.post<Workspace>("/v1/workspaces/import", bundle); }
}

// ======================================================================
// In-memory backend — same contract, sample tables, no server
// ======================================================================
const NUMERIC = (v: unknown): v is number => typeof v === "number";
const CAP = 50000;

interface SampleTable { format: string; engine: string; size: number; rowCount?: number; cols: [string, string][]; rows: Row[]; }

const TABLES: Record<string, SampleTable> = {
  "/data/warehouse/people.csv": {
    format: "csv", engine: "local", size: 236,
    cols: [["id", "Int64"], ["name", "Utf8"], ["city", "Utf8"], ["score", "Float64"], ["active", "Boolean"]],
    rows: [
      { id: 1, name: "Ada", city: "London", score: 91.5, active: true },
      { id: 2, name: "Grace", city: "New York", score: 88.0, active: false },
      { id: 3, name: "Linus", city: "Helsinki", score: null, active: true },
      { id: 4, name: "Alan", city: "London", score: 79.25, active: true },
      { id: 5, name: "Katherine", city: "Hampton", score: 95.0, active: null },
      { id: 6, name: "Edsger", city: "Rotterdam", score: 84.0, active: false },
      { id: 7, name: "Barbara", city: "New York", score: null, active: true },
      { id: 8, name: "Donald", city: "Pittsburgh", score: 90.5, active: true },
    ],
  },
  "/data/warehouse/events.parquet": {
    format: "parquet", engine: "local", size: 184320000, rowCount: 4200000,
    cols: [["event_id", "Int64"], ["ts", "Timestamp(us)"], ["user", "Utf8"], ["event", "Utf8"], ["latency_ms", "Int32"], ["ok", "Boolean"]],
    rows: [
      { event_id: 90001, ts: "2026-07-13 09:12:04", user: "ada", event: "open", latency_ms: 12, ok: true },
      { event_id: 90002, ts: "2026-07-13 09:12:07", user: "grace", event: "query", latency_ms: 143, ok: true },
      { event_id: 90003, ts: "2026-07-13 09:12:09", user: "linus", event: "export", latency_ms: null, ok: false },
      { event_id: 90004, ts: "2026-07-13 09:12:15", user: "ada", event: "sort", latency_ms: 8, ok: true },
      { event_id: 90005, ts: "2026-07-13 09:12:22", user: "alan", event: "query", latency_ms: 311, ok: true },
      { event_id: 90006, ts: "2026-07-13 09:12:31", user: "barbara", event: "filter", latency_ms: 19, ok: true },
      { event_id: 90007, ts: "2026-07-13 09:12:44", user: "donald", event: "open", latency_ms: 10, ok: true },
      { event_id: 90008, ts: "2026-07-13 09:12:59", user: "edsger", event: "export", latency_ms: 520, ok: false },
    ],
  },
  "/data/warehouse/year=2024/sales.parquet": {
    format: "parquet", engine: "local", size: 52428800, rowCount: 1200000,
    cols: [["region", "Utf8"], ["product", "Utf8"], ["qty", "Int64"], ["revenue", "Float64"]],
    rows: [
      { region: "EMEA", product: "Pro", qty: 120, revenue: 35880.0 },
      { region: "AMER", product: "Team", qty: 340, revenue: 20400.0 },
      { region: "APAC", product: "Pro", qty: 92, revenue: 27508.0 },
      { region: "EMEA", product: "Team", qty: 210, revenue: 12600.0 },
      { region: "AMER", product: "Enterprise", qty: 18, revenue: 90000.0 },
      { region: "APAC", product: "Team", qty: 265, revenue: 15900.0 },
    ],
  },
};
TABLES["/data/warehouse/year=2025/sales.parquet"] = Object.assign({}, TABLES["/data/warehouse/year=2024/sales.parquet"], { size: 61341184 });

const FS: Record<string, Listing> = {
  "/data": { dir: "/data", parent: "/", entries: [{ name: "warehouse", path: "/data/warehouse", kind: "dir" }] },
  "/data/warehouse": {
    dir: "/data/warehouse", parent: "/data", entries: [
      { name: "year=2024", path: "/data/warehouse/year=2024", kind: "dir" },
      { name: "year=2025", path: "/data/warehouse/year=2025", kind: "dir" },
      { name: "events.parquet", path: "/data/warehouse/events.parquet", kind: "file", size: 184320000 },
      { name: "people.csv", path: "/data/warehouse/people.csv", kind: "file", size: 236 },
    ],
  },
  "/data/warehouse/year=2024": { dir: "/data/warehouse/year=2024", parent: "/data/warehouse", entries: [{ name: "sales.parquet", path: "/data/warehouse/year=2024/sales.parquet", kind: "file", size: 52428800 }] },
  "/data/warehouse/year=2025": { dir: "/data/warehouse/year=2025", parent: "/data/warehouse", entries: [{ name: "sales.parquet", path: "/data/warehouse/year=2025/sales.parquet", kind: "file", size: 61341184 }] },
};

const columnsOf = (t: SampleTable): Column[] => t.cols.map(([name, data_type]) => ({ name, data_type, nullable: t.rows.some((r) => r[name] == null) }));
const cmp = (a: unknown, b: unknown): number => { if (a == null) return 1; if (b == null) return -1; return (a as number) < (b as number) ? -1 : (a as number) > (b as number) ? 1 : 0; };

function applyFilters(rows: Row[], filters: Filters): Row[] {
  for (const spec of filterSpecs(filters)) {
    const i = spec.indexOf(":"), j = spec.indexOf(":", i + 1);
    const col = spec.slice(0, i), op = spec.slice(i + 1, j), val = spec.slice(j + 1);
    rows = rows.filter((r) => {
      const cell = r[col];
      if (cell == null) return false;
      const numeric = NUMERIC(cell) && !isNaN(parseFloat(val));
      if (numeric) {
        const c = cell as number, n = parseFloat(val);
        switch (op) { case "gt": return c > n; case "lt": return c < n; case "ge": return c >= n; case "le": return c <= n; case "eq": return c === n; case "ne": return c !== n; default: return String(c).includes(val); }
      }
      const s = String(cell);
      switch (op) { case "eq": return s === val; case "ne": return s !== val; case "gt": return s > val; case "lt": return s < val; case "ge": return s >= val; case "le": return s <= val; default: return s.includes(val); }
    });
  }
  return rows;
}

function profileColumns(t: SampleTable, rows: Row[]): ProfileColumn[] {
  return t.cols.map(([name, data_type]) => {
    const vals = rows.map((r) => r[name]);
    const present = vals.filter((v) => v != null);
    const nulls = vals.length - present.length;
    const numeric = present.length > 0 && present.every(NUMERIC);
    const distinct = new Set(present.map(String));
    let min: string | null = null, max: string | null = null;
    if (present.length) {
      if (numeric) { min = String(Math.min(...(present as number[]))); max = String(Math.max(...(present as number[]))); }
      else { const s = present.map(String).sort(); min = s[0]; max = s[s.length - 1]; }
    }
    return {
      name, data_type, null_count: nulls, null_fraction: vals.length ? nulls / vals.length : 0,
      distinct: Math.min(distinct.size, CAP), distinct_capped: distinct.size > CAP,
      min, max, sample: present.slice(0, 5).map(String),
    };
  });
}

const delay = <T>(v: T, ms = 120): Promise<T> => new Promise((res) => setTimeout(() => res(v), ms));

// ---- mock workspace store (same contract as LocalStore; persists to localStorage) ----
// The offline demo keeps its own workspaces/history so the multi-tab UI is fully usable with no
// server. Result bytes are cached in-memory (ephemeral) like the on-disk store's Parquet cache.
const LS_KEY = "lakeleto.workspaces";
let mockSeq = 0;
const mockId = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${(mockSeq++).toString(36)}`;
interface MockEntry { ws: Workspace; history: RunRecord[]; }
type MockDb = Record<string, MockEntry>;
const MOCK_RESULTS = new Map<string, QueryResp>();
function loadDb(): MockDb {
  try { const s = localStorage.getItem(LS_KEY); if (s) return JSON.parse(s) as MockDb; } catch { /* no storage */ }
  return {};
}
function saveDb(db: MockDb) { try { localStorage.setItem(LS_KEY, JSON.stringify(db)); } catch { /* no storage */ } }
function seedDb(): MockDb {
  const db = loadDb();
  if (Object.keys(db).length) return db;
  const now = Date.now();
  const ws: Workspace = {
    id: "ws-demo", name: "Demo workspace", created_at_ms: now, updated_at_ms: now,
    connections: [
      { id: "conn-people", label: "people.csv", path: "/data/warehouse/people.csv", description: "sample people table", pinned: true },
      { id: "conn-events", label: "events.parquet", path: "/data/warehouse/events.parquet" },
    ],
    saved_queries: [
      { id: "q-bycity", name: "count by city", sql: "SELECT city, count(*) n\nFROM t GROUP BY city ORDER BY n DESC", connection_id: "conn-people", folder: "dashboards", pinned: true },
      { id: "q-active", name: "active only", sql: "SELECT * FROM t WHERE active = {{active}}", connection_id: "conn-people", folder: "dashboards" },
    ],
    tabs: [],
    variables: [{ key: "active", value: "true" }],
  };
  db[ws.id] = { ws, history: [] };
  saveDb(db);
  return db;
}
const metaOf = (ws: Workspace): WsMeta => ({
  id: ws.id, name: ws.name, created_at_ms: ws.created_at_ms, updated_at_ms: ws.updated_at_ms,
  connection_count: ws.connections.length, query_count: ws.saved_queries.length,
});

export class LakeletoMockBackend implements Backend {
  engines() {
    return delay<Engines>({
      engine: { engine: "local", formats: ["parquet", "csv"], sql: true, profile: true, remote: false },
      sql_available: true,
      endpoints: ["GET /v1/engines", "GET /v1/schema", "GET /v1/rows", "GET /v1/stats", "POST /v1/query"],
    });
  }
  list(dir: string) {
    const d = FS[dir] || FS["/data/warehouse"];
    const key = FS[dir] ? dir : "/data/warehouse";
    return delay<Listing>({ dir: key, parent: d.parent, entries: d.entries });
  }
  private table(path: string): SampleTable { const t = TABLES[path]; if (!t) throw new Error("no such file: " + path); return t; }
  async schema(path: string) {
    const t = this.table(path);
    return delay<SchemaResp>({ source: path, format: t.format, engine: t.engine, row_count: t.rowCount ?? t.rows.length, columns: columnsOf(t) });
  }
  async info(path: string) {
    const t = this.table(path);
    return delay<InfoResp>({ path, format: t.format, engine: t.engine, size_bytes: t.size, row_count: t.rowCount ?? t.rows.length, columns: t.cols.length });
  }
  profile(path: string) { return this.stats({ path }); }
  async rows({ path, offset = 0, limit = 100, sort = null, filters = {}, cols = null }: Parameters<Backend["rows"]>[0]) {
    const t = this.table(path);
    let rows = applyFilters(t.rows.slice(), filters);
    const matched = rows.length;
    if (sort) rows = rows.sort((a, b) => cmp(a[sort.col], b[sort.col]) * (sort.desc ? -1 : 1));
    const window = rows.slice(offset, offset + limit);
    let columns = columnsOf(t);
    if (cols && cols.length) columns = cols.map((n) => columns.find((c) => c.name === n)).filter(Boolean) as Column[];
    return delay<RowsResp>({
      columns, offset, num_rows: window.length, matched_rows: matched, total_known: true,
      scanned_rows: t.rows.length, bounded: false, rows: window,
    });
  }
  async stats({ path, filters = {} }: Parameters<Backend["stats"]>[0]) {
    const t = this.table(path);
    const rows = applyFilters(t.rows.slice(), filters);
    return delay<Profile>({ source: path, engine: t.engine, row_count: t.rowCount ?? t.rows.length, scanned_rows: t.rows.length, columns: profileColumns(t, rows) });
  }
  async query({ sql, file }: Parameters<Backend["query"]>[0]) {
    const t = this.table(file || "");
    const s = (sql || "").trim();
    const gb = s.match(/group\s+by\s+([a-z_][a-z0-9_]*)/i);
    if (gb && /count\s*\(\s*\*\s*\)/i.test(s)) {
      const col = gb[1], counts: Record<string, number> = {};
      t.rows.forEach((r) => { const k = String(r[col]); counts[k] = (counts[k] || 0) + 1; });
      const rows: Row[] = Object.entries(counts).map(([k, n]) => ({ [col]: k, n }));
      if (/order\s+by/i.test(s)) rows.sort((a, b) => (b.n as number) - (a.n as number));
      return delay<QueryResp>({ columns: [{ name: col, data_type: "Utf8", nullable: false }, { name: "n", data_type: "Int64", nullable: false }], num_rows: rows.length, rows });
    }
    const lim = (s.match(/limit\s+(\d+)/i) || [])[1];
    const rows = t.rows.slice(0, lim ? +lim : 20);
    return delay<QueryResp>({ columns: columnsOf(t), num_rows: rows.length, rows });
  }
  exportUrl() { return null; } // sample mode downloads client-side instead

  // ---- workspace data plane (in-memory, same shapes as LocalStore) ----
  private entry(db: MockDb, id: string): MockEntry {
    const e = db[id];
    if (!e) throw new ApiError("no such workspace: " + id, 404);
    return e;
  }
  async wsList() { const db = seedDb(); return delay(Object.values(db).map((e) => metaOf(e.ws))); }
  async wsCreate(name: string) {
    const db = seedDb(); const now = Date.now();
    const ws: Workspace = { id: mockId("ws"), name: name || "untitled", created_at_ms: now, updated_at_ms: now, connections: [], saved_queries: [], tabs: [] };
    db[ws.id] = { ws, history: [] }; saveDb(db);
    return delay(ws);
  }
  async wsGet(id: string) { const db = seedDb(); return delay(this.entry(db, id).ws); }
  async wsSave(id: string, ws: Workspace) {
    const db = seedDb(); const cur = this.entry(db, id).ws;
    const merged: Workspace = { ...ws, id, created_at_ms: cur.created_at_ms, updated_at_ms: Date.now() };
    db[id].ws = merged; saveDb(db);
    return delay(merged);
  }
  async wsDelete(id: string) { const db = seedDb(); this.entry(db, id); delete db[id]; saveDb(db); await delay(null); }
  async wsHistory(id: string) { const db = seedDb(); return delay(this.entry(db, id).history.slice()); }
  async wsRun(id: string, req: RunReq) {
    const db = seedDb(); const entry = this.entry(db, id);
    const cap = Math.min(Math.max(req.limit ?? 10000, 1), 100000);
    const previewN = Math.min(Math.max(req.preview ?? 200, 1), cap);
    const sql = (req.sql || "").trim();
    const started = Date.now();
    const format = req.format ?? (/\.parquet$/i.test(req.path) ? "parquet" : "csv");
    let full: QueryResp | null = null, error: string | null = null;
    try {
      if (sql) {
        // Cap the SQL result BEFORE recording/caching — same bounded-execution contract as the
        // server's plan-level limit (`Engine::query_capped`).
        const q = await this.query({ sql, file: req.path });
        full = { columns: q.columns, num_rows: Math.min(q.num_rows, cap), rows: q.rows.slice(0, cap) };
      } else { const g = await this.rows({ path: req.path, offset: 0, limit: cap }); full = { columns: g.columns, num_rows: g.num_rows, rows: g.rows }; }
    } catch (e) { error = (e as Error).message; }
    const rec: RunRecord = {
      id: mockId("run"), at_ms: Date.now(), sql: sql || null, source_path: req.path, format,
      status: error ? "error" : "ok", error, row_count: error ? null : full!.num_rows,
      duration_ms: Date.now() - started, cached: !error,
    };
    entry.history.unshift(rec); saveDb(db);
    if (error || !full) throw new ApiError(error || "run failed", 400);
    MOCK_RESULTS.set(id + ":" + rec.id, full);
    return delay<RunResponse>({ run: rec, columns: full.columns, num_rows: Math.min(previewN, full.num_rows), rows: full.rows.slice(0, previewN) });
  }
  async wsRunResult(id: string, runId: string, offset = 0, limit = 200) {
    const full = MOCK_RESULTS.get(id + ":" + runId);
    if (!full) throw new ApiError("no cached result for run " + runId, 404);
    return delay<QueryResp>({ columns: full.columns, num_rows: Math.min(limit, Math.max(full.rows.length - offset, 0)), rows: full.rows.slice(offset, offset + limit) });
  }
  async wsExport(id: string) { const db = seedDb(); const e = this.entry(db, id); return delay<WorkspaceBundle>({ bundle_version: BUNDLE_VERSION, workspace: e.ws, history: e.history.slice() }); }
  async wsImport(bundle: WorkspaceBundle) {
    const db = seedDb(); const now = Date.now(); const id = mockId("ws");
    const src = bundle.workspace || ({} as Workspace);
    const ws: Workspace = {
      id, name: (src.name || "imported") + " (imported)", created_at_ms: now, updated_at_ms: now,
      connections: src.connections || [], saved_queries: src.saved_queries || [], tabs: src.tabs || [],
      variables: src.variables || [], // parity with LocalStore::import — offline imports keep {{vars}}
    };
    const history = (bundle.history || []).map((h) => ({ ...h, cached: false }));
    db[id] = { ws, history }; saveDb(db);
    return delay(ws);
  }
}

// ======================================================================
// connect: probe a live server, else fall back to the in-memory backend
// ======================================================================
export async function connectLakeleto({ base = "", token = "" }: { base?: string; token?: string } = {}): Promise<Conn> {
  // Always probe live first: `base` empty means same-origin, which is exactly the case when
  // the `lakeleto` binary serves this bundle at `/` (embedded). A `?api=` override points at a
  // remote server / Lakeleto Cloud. Only fall back to the in-memory sample tables when no
  // `/v1/engines` answers (e.g. the design-system preview opened as a file:// page).
  try {
    const client = new LakeletoHttpClient({ base, token });
    const caps = await client.engines();
    return { mode: "live", base, caps, backend: client };
  } catch (e) {
    // A `status` means the server answered (401/403/5xx/etc.) — a real problem the
    // caller should see, not something to mask behind sample data. Only a genuine
    // network failure (no response at all, e.g. no server / file:// preview) falls
    // through to the in-memory backend.
    if (e instanceof ApiError && e.status != null) throw e;
  }
  const mock = new LakeletoMockBackend();
  const caps = await mock.engines();
  return { mode: "sample", base: null, caps, backend: mock };
}
