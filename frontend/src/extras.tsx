// Bells & whistles: a ⌘K command palette, a row-detail drawer, a "run across connections /
// folder" runner, and a two-run compare (diff) view. Kept together since they're all overlays or
// full-pane views composed from the design-system primitives.
import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import type { QueryResp, RunRecord, RunResponse, Row } from "./api";
import { Button, Chip, Textarea } from "./components";
import type { OpenTab } from "./workspace";

const fmtInt = (n?: number | null) => (n == null ? "?" : Number(n).toLocaleString());
const overlay: CSSProperties = { position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)", display: "flex", zIndex: 50 };

// ======================================================================
// Command palette (⌘K) — jump to any connection / query / file / tab / action
// ======================================================================
export interface PaletteItem { icon?: string; label: string; hint?: string; run: () => void; }
export function CommandPalette({ open, items, onClose }: { open: boolean; items: PaletteItem[]; onClose: () => void }) {
  const [q, setQ] = useState("");
  const [sel, setSel] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => { if (open) { setQ(""); setSel(0); setTimeout(() => inputRef.current?.focus(), 0); } }, [open]);
  const filtered = useMemo(() => {
    const t = q.trim().toLowerCase();
    const list = !t ? items : items.filter((i) => (i.label + " " + (i.hint || "")).toLowerCase().includes(t));
    return list.slice(0, 60);
  }, [q, items]);
  useEffect(() => { if (sel >= filtered.length) setSel(Math.max(0, filtered.length - 1)); }, [filtered.length, sel]);
  if (!open) return null;
  const pick = (i: number) => { const it = filtered[i]; if (it) { onClose(); it.run(); } };
  return (
    <div style={{ ...overlay, alignItems: "flex-start", justifyContent: "center", paddingTop: "12vh" }} onClick={onClose}>
      <div onClick={(e) => e.stopPropagation()}
        style={{ width: "min(640px, 92vw)", background: "var(--bg)", border: "var(--border-hairline)", borderRadius: "var(--radius-lg)", boxShadow: "var(--shadow-popover)", overflow: "hidden", display: "flex", flexDirection: "column", maxHeight: "70vh" }}>
        <div style={{ padding: 8, borderBottom: "var(--border-hairline)" }}>
          <input ref={inputRef} value={q} placeholder="Jump to a connection, query, file, or command…" spellCheck={false}
            onChange={(e) => { setQ(e.target.value); setSel(0); }}
            onKeyDown={(e) => {
              if (e.key === "ArrowDown") { e.preventDefault(); setSel((s) => Math.min(s + 1, filtered.length - 1)); }
              else if (e.key === "ArrowUp") { e.preventDefault(); setSel((s) => Math.max(s - 1, 0)); }
              else if (e.key === "Enter") { e.preventDefault(); pick(sel); }
              else if (e.key === "Escape") { e.preventDefault(); onClose(); }
            }}
            style={{ width: "100%", padding: "8px 10px", border: "none", outline: "none", background: "transparent", color: "var(--fg)", fontFamily: "var(--font-mono)", fontSize: "var(--text-ui)" }} />
        </div>
        <div style={{ overflow: "auto" }}>
          {filtered.length === 0 && <div style={{ padding: 12, color: "var(--muted)", fontSize: "var(--text-base)" }}>No matches.</div>}
          {filtered.map((it, i) => (
            <div key={i} onMouseEnter={() => setSel(i)} onClick={() => pick(i)}
              style={{ display: "flex", gap: 8, alignItems: "center", padding: "6px 12px", cursor: "pointer", background: i === sel ? "var(--hover)" : "transparent", borderLeft: `2px solid ${i === sel ? "var(--accent)" : "transparent"}` }}>
              <span style={{ width: 16, textAlign: "center", color: "var(--muted)" }}>{it.icon || "›"}</span>
              <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{it.label}</span>
              {it.hint && <span style={{ color: "var(--muted)", fontSize: "var(--text-xs)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 220 }}>{it.hint}</span>}
            </div>
          ))}
        </div>
        <div style={{ padding: "5px 12px", borderTop: "var(--border-hairline)", color: "var(--muted)", fontSize: "var(--text-xs)", display: "flex", gap: 12 }}>
          <span>↑↓ navigate</span><span>↵ open</span><span>esc close</span>
        </div>
      </div>
    </div>
  );
}

// ======================================================================
// Row detail drawer — the full row as pretty JSON (great for wide tables)
// ======================================================================
export function RowDetail({ row, onClose }: { row: Row | null; onClose: () => void }) {
  if (!row) return null;
  const json = JSON.stringify(row, null, 2);
  return (
    <aside style={{ flex: "0 0 340px", borderLeft: "var(--border-hairline)", background: "var(--panel)", display: "flex", flexDirection: "column", minHeight: 0 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, padding: "var(--pad-toolbar)", borderBottom: "var(--border-hairline)" }}>
        <span style={{ fontSize: "var(--text-xs)", textTransform: "uppercase", letterSpacing: "0.06em", color: "var(--muted)", fontWeight: "var(--weight-semibold)" }}>Row detail</span>
        <span style={{ flex: 1 }} />
        <Button size="sm" onClick={() => navigator.clipboard?.writeText(json).catch(() => {})} title="copy JSON">Copy</Button>
        <Button size="sm" onClick={onClose} title="close">×</Button>
      </div>
      <div style={{ overflow: "auto", padding: "var(--gutter)" }}>
        <table style={{ borderCollapse: "collapse", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)", width: "100%" }}>
          <tbody>
            {Object.entries(row).map(([k, v]) => {
              const isNull = v === null || v === undefined;
              return (
                <tr key={k}>
                  <td style={{ verticalAlign: "top", padding: "3px 8px 3px 0", color: "var(--muted)", whiteSpace: "nowrap" }}>{k}</td>
                  <td style={{ padding: "3px 0", color: isNull ? "var(--null)" : "var(--fg)", wordBreak: "break-word" }}>{isNull ? "·" : String(v)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </aside>
  );
}

// ======================================================================
// Runner — run a SQL across selected connections, or run every job in a folder
// ======================================================================
export interface RunJob { id: string; label: string; path: string; sql: string; }
interface JobResult { status: "ok" | "error" | "running"; rows?: number; ms?: number; error?: string; run?: RunRecord; }
export function RunnerModal({ open, title, sharedSql, jobs, exec, onOpenResult, onClose }: {
  open: boolean; title: string;
  sharedSql: string | null;                          // set → editable SQL applied to every job (across-connections); null → each job's own sql (folder)
  jobs: RunJob[];
  exec: (sql: string, path: string) => Promise<RunResponse>;
  onOpenResult: (run: RunRecord) => void; onClose: () => void;
}) {
  const [sql, setSql] = useState(sharedSql || "");
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [results, setResults] = useState<Record<string, JobResult>>({});
  const [running, setRunning] = useState(false);
  useEffect(() => { if (open) { setSql(sharedSql || ""); setSelected(Object.fromEntries(jobs.map((j) => [j.id, true]))); setResults({}); setRunning(false); } }, [open, sharedSql, jobs]);
  if (!open) return null;
  const chosen = jobs.filter((j) => selected[j.id]);
  const run = async () => {
    setRunning(true);
    for (const j of chosen) {
      setResults((r) => ({ ...r, [j.id]: { status: "running" } }));
      try {
        const resp = await exec(sharedSql != null ? sql : j.sql, j.path);
        setResults((r) => ({ ...r, [j.id]: { status: "ok", rows: resp.run.row_count ?? resp.num_rows, ms: resp.run.duration_ms, run: resp.run } }));
      } catch (e) { setResults((r) => ({ ...r, [j.id]: { status: "error", error: (e as Error).message } })); }
    }
    setRunning(false);
  };
  const okCount = Object.values(results).filter((r) => r.status === "ok").length;
  const errCount = Object.values(results).filter((r) => r.status === "error").length;
  return (
    <div style={overlay} onClick={onClose}>
      <div onClick={(e) => e.stopPropagation()}
        style={{ margin: "auto", width: "min(760px, 94vw)", maxHeight: "84vh", background: "var(--bg)", border: "var(--border-hairline)", borderRadius: "var(--radius-lg)", boxShadow: "var(--shadow-popover)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "var(--pad-chrome)", borderBottom: "var(--border-hairline)" }}>
          <strong>{title}</strong>
          <span style={{ flex: 1 }} />
          <Button size="sm" onClick={onClose}>×</Button>
        </div>
        <div style={{ padding: "var(--gutter)", overflow: "auto" }}>
          {sharedSql != null && (
            <div style={{ marginBottom: 10 }}>
              <Textarea value={sql} onChange={setSql} rows={3} placeholder="SELECT * FROM t LIMIT 20" />
              <div style={{ color: "var(--muted)", fontSize: "var(--text-xs)", marginTop: 3 }}>Runs against each selected connection (source registered as <code>t</code>). <code>{"{{variables}}"}</code> resolve per run.</div>
            </div>
          )}
          <table style={{ borderCollapse: "collapse", width: "100%", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)" }}>
            <thead><tr>{["", "target", sharedSql != null ? "source" : "query", "result", ""].map((h, i) => (
              <th key={i} style={{ textAlign: "left", padding: "5px 8px", borderBottom: "var(--border-hairline)", color: "var(--muted)", fontWeight: "var(--weight-semibold)" }}>{h}</th>
            ))}</tr></thead>
            <tbody>
              {jobs.map((j) => {
                const res = results[j.id];
                return (
                  <tr key={j.id}>
                    <td style={{ padding: "4px 8px", borderBottom: "var(--border-hairline)" }}>
                      <input type="checkbox" checked={!!selected[j.id]} disabled={running} onChange={(e) => setSelected((s) => ({ ...s, [j.id]: e.target.checked }))} />
                    </td>
                    <td style={{ padding: "4px 8px", borderBottom: "var(--border-hairline)", whiteSpace: "nowrap" }}>{j.label}</td>
                    <td style={{ padding: "4px 8px", borderBottom: "var(--border-hairline)", color: "var(--muted)", maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={sharedSql != null ? j.path : j.sql}>{sharedSql != null ? j.path : j.sql.replace(/\s+/g, " ")}</td>
                    <td style={{ padding: "4px 8px", borderBottom: "var(--border-hairline)", whiteSpace: "nowrap" }}>
                      {!res && <span style={{ color: "var(--muted)" }}>—</span>}
                      {res?.status === "running" && <span style={{ color: "var(--muted)" }}>running…</span>}
                      {res?.status === "ok" && <span><span style={{ color: "var(--accent)" }}>●</span> {fmtInt(res.rows)} rows · {res.ms}ms</span>}
                      {res?.status === "error" && <span style={{ color: "var(--err-fg)" }} title={res.error}>▲ error</span>}
                    </td>
                    <td style={{ padding: "4px 8px", borderBottom: "var(--border-hairline)" }}>
                      {res?.status === "ok" && res.run && <Button size="sm" onClick={() => onOpenResult(res.run!)}>open</Button>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "var(--pad-toolbar)", borderTop: "var(--border-hairline)" }}>
          <Button variant="primary" onClick={run} disabled={running || chosen.length === 0}>{running ? "Running…" : `Run ${chosen.length}`}</Button>
          {(okCount > 0 || errCount > 0) && <span style={{ color: "var(--muted)", fontSize: "var(--text-12)" }}>{okCount} ok{errCount ? ` · ${errCount} error` : ""}</span>}
          <span style={{ flex: 1 }} />
          <Button size="sm" onClick={onClose}>Close</Button>
        </div>
      </div>
    </div>
  );
}

// ======================================================================
// Compare view — two cached run results side by side, with a diff summary
// ======================================================================
function diffCells(a: QueryResp, b: QueryResp, common: string[]): Set<number> {
  const n = Math.min(a.rows.length, b.rows.length);
  const diff = new Set<number>();
  for (let i = 0; i < n; i++) for (const c of common) if (String(a.rows[i]?.[c] ?? "") !== String(b.rows[i]?.[c] ?? "")) { diff.add(i); break; }
  return diff;
}
function ResultBlock({ title, data, highlight }: { title: string; data: QueryResp | null | undefined; highlight?: Set<number> }) {
  const cell: CSSProperties = { textAlign: "left", padding: "4px 10px", borderBottom: "var(--border-hairline)", whiteSpace: "nowrap" };
  return (
    <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", borderRight: "var(--border-hairline)" }}>
      <div style={{ padding: "6px 10px", borderBottom: "var(--border-hairline)", color: "var(--muted)", fontSize: "var(--text-12)", fontFamily: "var(--font-mono)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{title}</div>
      <div style={{ overflow: "auto", minHeight: 0 }}>
        {data ? (
          <table style={{ borderCollapse: "collapse", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)" }}>
            <thead><tr>{data.columns.map((c) => <th key={c.name} style={{ ...cell, background: "var(--panel)", fontWeight: "var(--weight-semibold)" }}>{c.name}</th>)}</tr></thead>
            <tbody>
              {data.rows.map((r, i) => (
                <tr key={i} style={{ background: highlight?.has(i) ? "var(--sel)" : undefined }}>
                  {data.columns.map((c) => { const v = r[c.name]; const isNull = v == null; return <td key={c.name} style={{ ...cell, color: isNull ? "var(--null)" : "var(--fg)" }}>{isNull ? "·" : String(v)}</td>; })}
                </tr>
              ))}
            </tbody>
          </table>
        ) : <div style={{ padding: "var(--gutter)", color: "var(--muted)" }}>loading…</div>}
      </div>
    </div>
  );
}
export function CompareView({ tab }: { tab: OpenTab }) {
  const a = tab.resultA, b = tab.resultB;
  const summary = useMemo(() => {
    if (!a || !b) return null;
    const ca = a.columns.map((c) => c.name), cb = b.columns.map((c) => c.name);
    const added = cb.filter((x) => !ca.includes(x));
    const removed = ca.filter((x) => !cb.includes(x));
    const common = ca.filter((x) => cb.includes(x));
    const rowsDiff = diffCells(a, b, common);
    return { added, removed, common, rowsA: a.rows.length, rowsB: b.rows.length, changed: rowsDiff, };
  }, [a, b]);
  return (
    <main style={{ flex: "1 1 auto", minWidth: 0, display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", padding: "var(--pad-toolbar)", borderBottom: "var(--border-hairline)", fontSize: "var(--text-12)" }}>
        <Chip tone="neutral">compare</Chip>
        {summary ? <>
          <span style={{ color: "var(--muted)" }}>rows {fmtInt(summary.rowsA)} → {fmtInt(summary.rowsB)}</span>
          {summary.added.length > 0 && <Chip>+{summary.added.length} col: {summary.added.join(", ")}</Chip>}
          {summary.removed.length > 0 && <Chip tone="warn">−{summary.removed.length} col: {summary.removed.join(", ")}</Chip>}
          <Chip tone={summary.changed.size ? "warn" : "neutral"}>{summary.changed.size} changed row(s){summary.rowsA !== summary.rowsB ? " (aligned by position)" : ""}</Chip>
        </> : <span style={{ color: "var(--muted)" }}>loading both results…</span>}
      </div>
      {(tab.resultErr) && <div style={{ padding: "var(--gutter)", color: "var(--err-fg)" }}>{tab.resultErr}</div>}
      <div style={{ flex: "1 1 auto", display: "flex", minHeight: 0 }}>
        <ResultBlock title={"A · " + (tab.runA?.sql ? tab.runA.sql.replace(/\s+/g, " ").trim() : "scan")} data={a} highlight={summary?.changed} />
        <ResultBlock title={"B · " + (tab.runB?.sql ? tab.runB.sql.replace(/\s+/g, " ").trim() : "scan")} data={b} highlight={summary?.changed} />
      </div>
    </main>
  );
}
