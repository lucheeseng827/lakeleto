// TableView — the per-tab explorer (Grid / Schema / Profile / SQL) for a single source.
// One of these renders inside each open workspace tab; it owns its own browse-fetch state
// (grid/schema/profile) keyed on the tab's path + sort + filters, while sort/filters/sql live in
// the tab object so they round-trip through the workspace store. SQL "Run" goes up to the shell
// (onRunSql) so the run is recorded in the workspace history and its result is cached.
import { useEffect, useState, type CSSProperties } from "react";
import { isJsonSource, normalizeJsonPath, type Backend, type Conn, type Profile, type Row, type SchemaResp } from "./api";
import { Banner, Button, cellText, Chip, DataGrid, filterRows, Select, StatTable, Tabs, TextInput, Textarea } from "./components";
import { useRowWindows } from "./windows";
import { usedVars, type OpenTab, type SubView } from "./workspace";

const fmtInt = (n?: number | null) => (n == null ? "?" : Number(n).toLocaleString());
// A struct column's type as the server reports it (`Struct("name": Utf8, …)`) — one with fields,
// since an empty struct is left whole by flattening.
const STRUCT = /^Struct\((?!\))/;
const basename = (p: string) => { const i = Math.max(p.lastIndexOf("/"), p.lastIndexOf("\\")); return i >= 0 ? p.slice(i + 1) : p; };
// Strip the Windows extended-length verbatim prefix (`\\?\`, `\\?\UNC\`) so the UI shows and copies
// a clean `C:\…` path instead of `\\?\C:\…` (the server canonicalizes to the verbatim form).
const cleanPath = (p: string) => p.replace(/^\\\\\?\\UNC\\/, "\\\\").replace(/^\\\\\?\\/, "");

/** One tab's explorer for a single source: its Grid, Schema, Profile and SQL views. */
export function TableView({ backend, conn, tab, onPatch, onRunSql, sqlAvailable, resolve, onOpenRow }: {
  backend: Backend; conn: Conn; tab: OpenTab;
  onPatch: (p: Partial<OpenTab>) => void;
  onRunSql: (sql: string) => void; sqlAvailable: boolean;
  resolve: (s: string) => string; onOpenRow: (r: Row) => void;
}) {
  const { sub, sort, filters, sql } = tab;
  const flatten = !!tab.flatten;
  const jsonPath = tab.jsonPath ?? null;    // null: the reader detects the records
  const rpath = resolve(tab.path);          // {{var}} → value; the engine only ever sees resolved paths
  // A whole-database URI (a DB URI with no ?table=) has no single grid — never fetch schema/rows for
  // it (that errors "names a whole database"); show a table picker instead. Guarded HERE so a tab
  // that lands on a whole-DB path by ANY route (restore, launcher, add) browses rather than errors.
  const wholeDb = /^(sqlite|postgres|postgresql|mysql):\/\//i.test(rpath) && !/[?&]table=/.test(rpath);
  const [schema, setSchema] = useState<SchemaResp | null>(null);
  const [profile, setProfile] = useState<Profile | null>(null);
  const [dbTables, setDbTables] = useState<{ name: string; path: string }[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [exportFmt, setExportFmt] = useState("csv");
  const [resultSearch, setResultSearch] = useState("");
  const [pathCopied, setPathCopied] = useState(false);
  const [pathDraft, setPathDraft] = useState<string | null>(null);   // non-null while editing the records path
  // What the tab read is another table once it reads its source another way (other records, other
  // columns), so it is dropped as the setting changes — during the render, so not even one frame
  // shows it under the new setting — rather than left up until the new reads land. A new sort or
  // filter keeps the rows on screen meanwhile: same table, same columns. (The grid's rows are
  // dropped the same way, by `useRowWindows`.)
  const readKey = JSON.stringify([rpath, flatten, jsonPath]);
  const [readFor, setReadFor] = useState(readKey);
  if (readFor !== readKey) {
    setReadFor(readKey);
    setSchema(null); setProfile(null);
  }
  // The grid's rows, read a window at a time as it scrolls.
  const grid = useRowWindows(backend, sub === "Grid" && !wholeDb ? { path: rpath, sort, filters, flatten, jsonPath } : null);
  const copyPath = () => {
    navigator.clipboard?.writeText(cleanPath(rpath))
      .then(() => { setPathCopied(true); setTimeout(() => setPathCopied(false), 1200); })
      .catch(() => { /* clipboard blocked (e.g. non-secure context) — ignore */ });
  };

  useEffect(() => {
    setErr(null);
    let cancelled = false;
    const fail = (e: unknown) => { if (!cancelled) setErr((e as Error).message); };
    if (wholeDb) {
      // Browse the database's tables instead of querying it as one.
      backend.list(rpath)
        .then((l) => { if (!cancelled) setDbTables(l.entries.map((e) => ({ name: e.name, path: e.path }))); })
        .catch((e) => { if (!cancelled) { setDbTables(null); fail(e); } });
      return () => { cancelled = true; };
    }
    if (sub === "Schema") backend.schema(rpath, flatten, jsonPath)
      .then((r) => { if (!cancelled) setSchema(r); }).catch((e) => { if (!cancelled) setSchema(null); fail(e); });
    else if (sub === "Profile") backend.stats({ path: rpath, filters, flatten, jsonPath })
      .then((r) => { if (!cancelled) setProfile(r); }).catch((e) => { if (!cancelled) setProfile(null); fail(e); });
    // The SQL view reads nothing of its own until a query runs; the schema says whether the source
    // has structs, and so whether to offer the flatten toggle. A failure here only hides it.
    // With a records path set, the failure is that path, and the tab's next run fails on it too — so
    // it is said now, where the Records control above can fix it.
    else if (sub === "SQL") backend.schema(rpath, flatten, jsonPath)
      .then((r) => { if (!cancelled) setSchema(r); }).catch((e) => { if (jsonPath != null) fail(e); });
    // Rows and a profile do not say where in a JSON document they came from; the schema does, for
    // the records control. Cheap after the first read — the server keeps it per file version.
    if ((sub === "Grid" || sub === "Profile") && isJsonSource(rpath)) backend.schema(rpath, flatten, jsonPath)
      .then((r) => { if (!cancelled) setSchema(r); }).catch(() => { /* the view's own read reports it */ });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [backend, rpath, sub, sort, JSON.stringify(filters), flatten, jsonPath]);

  const toggleSort = (c: string) => onPatch({ sort: !sort || sort.col !== c ? { col: c, desc: false } : !sort.desc ? { col: c, desc: true } : null });
  // Offered when there is a struct to spread, and while it is on so it can be turned off again.
  const shown: { name: string; data_type: string }[] = (grid.ready ? grid.columns : null) ?? schema?.columns ?? profile?.columns ?? [];
  const hasStructs = shown.some((c) => STRUCT.test(c.data_type || ""));
  const toggleFlatten = () => {
    // Flattening renames what it spreads (`user` ↔ `user.name`), so a sort or filter survives the
    // switch only on a column that reads the same either way.
    const same = new Set(shown.filter((c) => !STRUCT.test(c.data_type || "") && !(flatten && c.name.includes("."))).map((c) => c.name));
    const kept = Object.fromEntries(Object.entries(filters).filter(([c]) => same.has(c)));
    onPatch({ flatten: !flatten, sort: sort && same.has(sort.col) ? sort : null, filters: kept });
  };
  // Where a JSON source's rows come from: the path the tab was given — `""` being the whole
  // document — or else the member the reader found, if it found one.
  const isJson = schema?.format === "json" || isJsonSource(rpath);
  const detected = jsonPath == null ? schema?.records_path ?? null : null;
  const recordsLabel = jsonPath === "" ? "whole document" : jsonPath ?? (detected ? `${detected} · auto` : "auto");
  const applyPath = (next: string | null) => {
    setPathDraft(null);
    const p = next == null ? undefined : normalizeJsonPath(next);
    if (p === tab.jsonPath) return;
    // Other records are other columns, so a sort or filter on these would not carry over.
    onPatch({ jsonPath: p, sort: null, filters: {} });
  };
  const setFilter = (c: string, v: string) => onPatch({ filters: { ...filters, [c]: v } });
  const nFilters = Object.values(filters).filter((v) => v && v.trim()).length;

  const doExport = () => {
    if (conn.mode === "live") { const u = backend.exportUrl({ path: rpath, fmt: exportFmt, sort, filters, flatten, jsonPath }); if (u) window.location.href = u; return; }
    backend.rows({ path: rpath, offset: 0, limit: 100000, sort, filters, flatten, jsonPath }).then((g) => {
      const names = g.columns.map((c) => c.name);
      let body: string, mime: string, ext: string;
      if (exportFmt === "json") { body = JSON.stringify(g.rows, null, 0); mime = "application/json"; ext = "json"; }
      else if (exportFmt === "csv") {
        const esc = (v: unknown) => { const s = cellText(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
        body = names.map(esc).join(",") + "\n" + g.rows.map((r) => names.map((n) => esc(r[n])).join(",")).join("\n");
        mime = "text/csv"; ext = "csv";
      } else { setErr("Parquet export requires a live server."); return; }
      const url = URL.createObjectURL(new Blob([body], { type: mime }));
      const a = document.createElement("a"); a.href = url; a.download = basename(rpath) + "." + ext; a.click(); URL.revokeObjectURL(url);
    }).catch((e) => setErr((e as Error).message));
  };
  const pathVars = usedVars(tab.path);
  const unresolved = pathVars.filter((v) => resolve("{{" + v + "}}") === "{{" + v + "}}");

  const toolbar: CSSProperties = { display: "flex", gap: "var(--space-5)", alignItems: "center", padding: "var(--pad-toolbar)", borderBottom: "var(--border-hairline)", flexWrap: "wrap", flex: "0 0 auto" };
  const pane: CSSProperties = { flex: "1 1 auto", minHeight: 0, overflow: "auto", padding: "var(--gutter)" };
  const sqlOut = tab.sqlOut;
  const shownErr = err ?? (sub === "Grid" ? grid.error : null);

  return (
    <main style={{ flex: "1 1 auto", minWidth: 0, display: "flex", flexDirection: "column" }}>
      <div style={{ padding: "var(--space-4) var(--gutter) 0", borderBottom: "var(--border-hairline)", background: "var(--panel)", flex: "0 0 auto" }}>
        <Tabs tabs={["Grid", "Schema", "Profile", "SQL"]} value={sub} onChange={(t) => onPatch({ sub: t as SubView })} />
      </div>
      <div style={toolbar}>
        <span role="button" tabIndex={0} onClick={copyPath}
          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); copyPath(); } }}
          style={{ color: "var(--muted)", fontFamily: "var(--font-mono)", fontSize: "var(--text-12)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 520, cursor: "pointer" }}
          title={(rpath !== tab.path ? `${cleanPath(tab.path)}\n  →  ${cleanPath(rpath)}` : cleanPath(tab.path)) + "\n\nClick to copy"}>{cleanPath(tab.path)}</span>
        <Button size="sm" onClick={copyPath} title="copy the full path to the clipboard">{pathCopied ? "Copied ✓" : "Copy path"}</Button>
        {rpath !== tab.path && unresolved.length === 0 && <Chip title={rpath}>→ {basename(rpath)}</Chip>}
        {unresolved.length > 0 && <Chip tone="warn" title="define these in the Variables panel">unset: {unresolved.map((v) => "{{" + v + "}}").join(" ")}</Chip>}
        {(isJson || jsonPath != null) && (pathDraft == null ? (
          <Button size="sm" variant={jsonPath != null ? "primary" : "default"} onClick={() => setPathDraft(jsonPath ?? detected ?? "")}
            title={jsonPath === "" ? "Rows: the document as one row, unwrapped by nothing. Click to change."
              : jsonPath != null ? `Rows: the elements of the array at ${jsonPath}. Click to change.`
                : detected ? `Rows: the elements of ${detected}, the one member holding records. Click to read another path.`
                  : "Rows: the file as it is laid out — one per line or array element. Click to read records at a path."}>
            Records: {recordsLabel}
          </Button>
        ) : (
          <span style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
            <TextInput size="sm" value={pathDraft} onChange={setPathDraft} placeholder="/response/items or a member name"
              title="A JSON Pointer (/response/items) or a top-level member name (groups)" style={{ width: 240 }}
              onKeyDown={(e) => { if (e.key === "Enter" && pathDraft) applyPath(pathDraft); if (e.key === "Escape") setPathDraft(null); }} />
            <Button size="sm" variant="primary" onClick={() => applyPath(pathDraft)} disabled={!pathDraft}>Apply</Button>
            <Button size="sm" onClick={() => applyPath("")} title="Read the document as one row, unwrapped by nothing">Whole document</Button>
            <Button size="sm" onClick={() => applyPath(null)} title="Let the reader find the records">Auto</Button>
            <Button size="sm" onClick={() => setPathDraft(null)}>Cancel</Button>
          </span>
        ))}
        {(hasStructs || flatten) && (
          <Button size="sm" variant={flatten ? "primary" : "default"} onClick={toggleFlatten}
            title={flatten ? "Show struct columns whole again"
              : "Read each struct column as one column per field (user.geo.lat), so its fields sort and filter like any other column"}>
            {flatten ? "Flattened ✓" : "Flatten structs"}
          </Button>
        )}
        <span style={{ flex: 1 }} />
        <label style={{ color: "var(--muted)", fontSize: "var(--text-12)" }}>Export</label>
        <Select value={exportFmt} onChange={setExportFmt} options={
          conn.mode === "live"
            ? [{ value: "csv", label: "CSV" }, { value: "json", label: "JSON" }, { value: "parquet", label: "Parquet" }]
            : [{ value: "csv", label: "CSV" }, { value: "json", label: "JSON" }]} />
        <Button onClick={doExport}>Download view</Button>
      </div>

      {shownErr && <div style={{ padding: "var(--gutter)" }}><Banner tone="err">{shownErr}</Banner></div>}

      {!shownErr && wholeDb && (
        <div style={pane}>
          <div style={{ color: "var(--muted)", fontSize: "var(--text-12)", marginBottom: 10 }}>
            This is a database — pick a table to open{dbTables ? ` (${dbTables.length} tables)` : "…"}.
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4, maxWidth: 480 }}>
            {(dbTables || []).map((t) => (
              <button key={t.path} title={t.path}
                onClick={() => onPatch({ path: t.path, title: t.name, sub: "Grid", sort: null, filters: {} })}
                style={{ display: "flex", alignItems: "center", gap: 8, textAlign: "left", padding: "7px 10px", border: "var(--border-hairline)", borderRadius: "var(--radius-sm)", background: "var(--bg)", color: "var(--fg)", cursor: "pointer", font: "inherit" }}>
                <span style={{ color: "var(--accent)" }}>▦</span>{t.name}
              </button>
            ))}
            {dbTables && dbTables.length === 0 && <div style={{ color: "var(--muted)" }}>No tables found.</div>}
          </div>
        </div>
      )}

      {!shownErr && sub === "Grid" && grid.ready && (
        <DataGrid columns={grid.columns} count={grid.count} row={grid.row} onRange={grid.want} resetKey={grid.epoch}
          sort={sort} onSort={toggleSort} filters={filters} onFilter={setFilter} onOpenRow={onOpenRow}
          footer={<>
            <span>{grid.count === 0 ? "0 rows" : `of ${grid.exact ? fmtInt(grid.count) : `≥ ${fmtInt(grid.seen)}`}`}</span>
            <span>scanned {fmtInt(grid.scanned)}</span>
            {nFilters > 0 && <Chip>filtered: {nFilters} filter(s)</Chip>}
            {grid.bounded && <Chip tone="warn">bounded: first {fmtInt(grid.scanned)} rows</Chip>}
            {grid.loading && <span>reading…</span>}
            {grid.windowError && <Chip tone="warn" title={grid.windowError}>some rows couldn't be read; trying again</Chip>}
          </>} />
      )}

      {!shownErr && sub === "Schema" && schema && (
        <div style={pane}>
          <div style={{ color: "var(--muted)", fontSize: "var(--text-12)", marginBottom: 8 }}>
            source: {schema.source} · format: {schema.format} · engine: {schema.engine} · rows: {fmtInt(schema.row_count)}
          </div>
          <StatTable columns={[{ key: "name", label: "column" }, { key: "data_type", label: "type", type: true }, { key: "nullable", label: "null?" }]}
            rows={schema.columns.map((c) => ({ name: c.name, data_type: c.data_type, nullable: c.nullable ? "yes" : "no" }))} />
        </div>
      )}

      {!shownErr && sub === "Profile" && profile && (
        <div style={pane}>
          <div style={{ color: "var(--muted)", fontSize: "var(--text-12)", marginBottom: 8, display: "flex", gap: 8, alignItems: "center" }}>
            engine: {profile.engine} · scanned {fmtInt(profile.scanned_rows)} rows
            {nFilters > 0 && <Chip>filtered: {nFilters} filter(s)</Chip>}
          </div>
          <StatTable columns={[{ key: "name", label: "column" }, { key: "data_type", label: "type", type: true }, { key: "null_count", label: "nulls" }, { key: "null_pct", label: "null%" }, { key: "distinct", label: "distinct" }, { key: "min", label: "min" }, { key: "max", label: "max" }]}
            rows={profile.columns.map((c) => ({
              name: c.name, data_type: c.data_type, null_count: c.null_count,
              null_pct: (c.null_fraction * 100).toFixed(1) + "%",
              distinct: c.distinct + (c.distinct_capped ? "+" : ""), min: c.min, max: c.max,
            }))} />
        </div>
      )}

      {sub === "SQL" && (
        <div style={pane}>
          <Textarea value={sql} onChange={(v) => onPatch({ sql: v })} placeholder="SELECT * FROM t LIMIT 20" disabled={!sqlAvailable} />
          <div style={{ margin: "8px 0", display: "flex", gap: 8, alignItems: "center" }}>
            <Button variant="primary" onClick={() => onRunSql(sql)} disabled={!sqlAvailable || tab.sqlBusy}>{tab.sqlBusy ? "Running…" : "Run"}</Button>
            <span style={{ color: "var(--muted)", fontSize: "var(--text-12)" }}>
              {sqlAvailable ? <>The tab's source is registered as table <code>t</code>{flatten ? <>, flattened — quote a field as <code>"user.name"</code></> : null}. Runs are saved to this workspace's history.</>
                : <>Server built without the <code>sql</code> feature — restart with <code>--features serve,sql</code>.</>}
            </span>
          </div>
          {tab.sqlErr && <Banner tone="err">{tab.sqlErr}</Banner>}
          {sqlOut && !tab.sqlErr && (
            <>
              <div style={{ display: "flex", gap: 8, alignItems: "center", margin: "6px 0", maxWidth: 320 }}>
                <TextInput value={resultSearch} onChange={setResultSearch} placeholder="search result…" size="sm" />
              </div>
              <StatTable columns={sqlOut.columns.map((c) => ({ key: c.name, label: c.name }))} rows={filterRows(sqlOut.rows, resultSearch)} onRowClick={onOpenRow} />
              <div style={{ color: "var(--muted)", fontSize: "var(--text-12)", marginTop: 6 }}>
                {fmtInt(sqlOut.run.row_count)} row(s) · {sqlOut.run.duration_ms} ms{sqlOut.run.cached ? " · cached" : ""}
                {sqlOut.num_rows < (sqlOut.run.row_count ?? 0) ? ` · showing first ${fmtInt(sqlOut.num_rows)}` : ""}
              </div>
            </>
          )}
        </div>
      )}
    </main>
  );
}
