// DiffPane — a table's Diff view: what changed between it and another table, or between it now
// and one of its own snapshots, as `lakeleto diff` reports it (`GET /v1/diff`). Nothing is read
// until Compare is pressed: a diff with exact statistics reads both tables whole. The form and the
// last report live on the tab, so another view and back keeps them. One Compare runs at a time:
// another (the button, or Enter in a field) waits until it ends. One in flight is abandoned by
// Cancel or by leaving the view; its answer is then dropped, though the server still runs it to
// its end. The report says which two tables it compared, which the form may since have changed.
// The tab's records path and flattening read both sides: a report or error is shown only while
// the tab still reads its table the way it did for it, and changing either abandons a Compare.
import { useEffect, useRef, useState, type CSSProperties } from "react";
import { displayPath, type Backend, type DiffQuery, type DiffReport, type DiffSide, type Row } from "./api";
import { Banner, Button, Chip, Select, StatTable, TextInput } from "./components";
import type { OpenTab } from "./workspace";

const fmtInt = (n?: number | null) => (n == null ? "?" : Number(n).toLocaleString());
const cell = (v: string | null | undefined) => (v == null ? "∅" : v);
/** `a → b`, or `a` alone when the two are the same. */
const arrow = (a: string, b: string) => (a === b ? a : `${a} → ${b}`);

/** How a side was read, for the header. */
function sideLabel(side: DiffSide): string {
  let s = displayPath(side.source);
  if (side.snapshot != null) s += ` @${side.snapshot}`;
  if (side.committed_ms != null) s += ` · committed ${new Date(side.committed_ms).toISOString().replace(".000Z", "Z")}`;
  return s;
}

const METHOD: Record<DiffReport["stats"]["method"], string> = {
  exact: "exact, every row",
  footer: "Parquet footers, no row read",
  sample: "a sample of the first rows",
  metadata: "table metadata, no row read",
};

/** One tab's Diff view. `path` is the tab's table, resolved; `flatten` and `jsonPath` read both
 *  sides, as the tab reads its own. */
export function DiffPane({ backend, path, flatten, jsonPath, tab, onPatch, sqlAvailable }: {
  backend: Backend; path: string; flatten: boolean; jsonPath: string | null;
  tab: OpenTab; onPatch: (p: Partial<OpenTab>) => void; sqlAvailable: boolean;
}) {
  const other = tab.diffWith ?? "";
  const keyText = tab.diffKey ?? "";
  const mode = tab.diffStats ?? "auto";
  const thisIsOld = !!tab.diffThisIsOld;
  const report = tab.diffOut ?? null;
  const [busy, setBusy] = useState(false);
  const [onlyChanged, setOnlyChanged] = useState(true);
  // How this tab reads its table now; a report read another way is not this table's.
  const readFor = JSON.stringify([path, flatten, jsonPath]);
  // What is shown: an error, else a report, each only if read the way the tab reads now.
  const shownErr = tab.diffErrFor === readFor ? tab.diffErr ?? null : null;
  const shownReport = !shownErr && tab.diffFor === readFor ? report : null;
  // The request in flight: aborted by Cancel, and when the view goes away or reads another way.
  const inFlight = useRef<AbortController | null>(null);
  // Leaving the view, or reading the table another way, stops waiting and says nothing: detached
  // first, so its handlers ignore it.
  useEffect(() => () => {
    const c = inFlight.current;
    inFlight.current = null;
    c?.abort();
    setBusy(false);
  }, [readFor]);

  // `@2` alone is this table at snapshot 2.
  const otherRef = other.trim().startsWith("@") ? path + other.trim() : other.trim();
  const snapshotSide = otherRef !== other.trim();
  // A snapshot is compared by its metadata: no key and no other statistics, whatever the form
  // held for a table before (its fields are kept for when it names one again).
  const key = snapshotSide ? [] : keyText.split(",").map((k) => k.trim()).filter(Boolean);
  const effMode = snapshotSide ? "auto" : mode;

  const compare = () => {
    // Not while one runs: replacing it would leave the server running both.
    if (!backend.diff || !otherRef || inFlight.current) return;
    const ctl = new AbortController();
    inFlight.current = ctl;
    const forKey = readFor;
    const q: DiffQuery = {
      left: thisIsOld ? path : otherRef, right: thisIsOld ? otherRef : path,
      key, fast: effMode === "fast", scan: effMode === "scan" ? 10_000 : undefined,
      flatten, jsonPath,
    };
    setBusy(true);
    onPatch({ diffErr: null });
    backend.diff(q, ctl.signal)
      .then((r) => { if (inFlight.current === ctl) onPatch({ diffOut: r, diffErr: null, diffFor: forKey }); })
      .catch((e) => {
        if (inFlight.current !== ctl) return;
        onPatch({ diffErr: ctl.signal.aborted ? "Cancelled." : (e as Error).message, diffErrFor: forKey });
      })
      .finally(() => { if (inFlight.current === ctl) { inFlight.current = null; setBusy(false); } });
  };
  const cancel = () => inFlight.current?.abort();

  const pane: CSSProperties = { flex: "1 1 auto", minHeight: 0, overflow: "auto", padding: "var(--gutter)" };
  const row: CSSProperties = { display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 8 };
  const muted: CSSProperties = { color: "var(--muted)", fontSize: "var(--text-12)" };
  const h: CSSProperties = { margin: "18px 0 6px", fontSize: "var(--text-base)", fontWeight: "var(--weight-semibold)" };

  return (
    <div style={pane}>
      <div style={row}>
        <label style={muted}>{thisIsOld ? "Compare this table with" : "Compare with"}</label>
        <TextInput value={other} onChange={(v) => onPatch({ diffWith: v })} style={{ width: 360 }}
          placeholder="a path or URI, or @2 for this table at snapshot 2"
          title="Another table (a path, an object-store or catalog URI), or @<snapshot> for this table at one of its snapshots: an Iceberg snapshot id, a Delta version, or an RFC 3339 instant"
          onKeyDown={(e) => { if (e.key === "Enter") compare(); }} />
        <Button size="sm" onClick={() => onPatch({ diffThisIsOld: !thisIsOld })}
          title="Which side this table is: the new one (compared from the other), or the old one">
          this table is {thisIsOld ? "the old side" : "the new side"} ⇄
        </Button>
      </div>
      <div style={row}>
        <label style={muted}>Key</label>
        <TextInput size="sm" value={keyText} onChange={(v) => onPatch({ diffKey: v })} style={{ width: 200 }}
          placeholder="id, or a, b (optional)" disabled={snapshotSide}
          title={snapshotSide ? "A snapshot is compared by its metadata; its rows are not read, so there is no key"
            : sqlAvailable ? "Compare the rows too, matched by these columns, which must name one row on each side"
              : "Comparing rows needs a server with the sql feature"}
          onKeyDown={(e) => { if (e.key === "Enter") compare(); }} />
        <label style={muted}>Statistics</label>
        <Select value={effMode} disabled={snapshotSide} onChange={(v) => onPatch({ diffStats: v as OpenTab["diffStats"] })}
          options={[
            { value: "auto", label: sqlAvailable ? "exact (every row)" : "a sample (no sql)" },
            { value: "fast", label: "fast: Parquet footers" },
            { value: "scan", label: "sample: first 10,000 rows" },
          ]} />
        <Button variant="primary" onClick={compare} disabled={!otherRef || busy}>{busy ? "Comparing…" : "Compare"}</Button>
        {busy && <Button size="sm" onClick={cancel} title="Stop waiting for this diff. The server still runs it to its end, and refuses more than two at once">Cancel</Button>}
      </div>
      <div style={{ ...muted, marginBottom: 12 }}>
        {(flatten || jsonPath != null) && <>Both tables are read as this tab reads its own{flatten ? ", flattened" : ""}{jsonPath != null ? `, records at ${jsonPath || "the whole document"}` : ""}. </>}
        Exact statistics read both tables whole; on large tables, a sample or footers answer sooner.
      </div>

      {!shownErr && !shownReport && !busy && (report || tab.diffErr) && (
        <div style={{ ...muted, marginBottom: 12 }}>
          The last result read this table another way: its records path or flattening has changed since. Compare to read it as it is read now.
        </div>
      )}
      {shownErr && <Banner tone="err">{shownErr}</Banner>}
      {shownReport && <Report report={shownReport} onlyChanged={onlyChanged} setOnlyChanged={setOnlyChanged} h={h} muted={muted} />}
    </div>
  );
}

function Report({ report, onlyChanged, setOnlyChanged, h, muted }: {
  report: DiffReport; onlyChanged: boolean; setOnlyChanged: (b: boolean) => void; h: CSSProperties; muted: CSSProperties;
}) {
  const s = report.schema;
  const schemaRows: Row[] = [
    ...s.added.map((c) => ({ change: "added", column: c.name, old: "", new: c.data_type + (c.nullable ? "" : " not null") })),
    ...s.removed.map((c) => ({ change: "removed", column: c.name, old: c.data_type + (c.nullable ? "" : " not null"), new: "" })),
    ...s.changed.map((c) => ({
      change: c.left_type !== c.right_type ? "retyped" : "nullability", column: c.name,
      old: c.left_type + (c.left_nullable ? "" : " not null"), new: c.right_type + (c.right_nullable ? "" : " not null"),
    })),
  ];
  const stats = report.stats.columns.filter((c) => !onlyChanged || c.changed || c.not_compared);
  const statRows: Row[] = stats.map((c) => ({
    column: c.name,
    changed: c.not_compared ? "not compared" : c.changed ? "●" : "",
    nulls: arrow(fmtInt(c.left.nulls), fmtInt(c.right.nulls)),
    min: arrow(cell(c.left.min), cell(c.right.min)),
    max: arrow(cell(c.left.max), cell(c.right.max)),
  }));
  const { left: rl, right: rr } = report.rows;
  const delta = rl != null && rr != null ? rr - rl : null;
  const k = report.keyed;

  return (
    <div>
      <div style={{ ...muted, display: "grid", gridTemplateColumns: "auto 1fr", gap: "2px 10px", maxWidth: 900 }}>
        <span>old</span><span style={{ fontFamily: "var(--font-mono)" }}>{sideLabel(report.left)}</span>
        <span>new</span><span style={{ fontFamily: "var(--font-mono)" }}>{sideLabel(report.right)}</span>
        <span>statistics</span>
        <span>{METHOD[report.stats.method]} · {report.stats.engine}
          {report.stats.method === "exact" || report.stats.method === "sample"
            ? ` · read ${fmtInt(report.stats.left_rows_read)} + ${fmtInt(report.stats.right_rows_read)} rows` : ""}</span>
      </div>

      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", margin: "12px 0" }}>
        {report.found.length === 0
          ? <Chip tone="neutral">no difference found</Chip>
          : report.found.map((f) => <Chip key={f} tone="warn">{f}</Chip>)}
      </div>

      <div style={h}>Rows</div>
      <div style={{ fontFamily: "var(--font-mono)" }}>
        {arrow(fmtInt(rl), fmtInt(rr))}{delta != null && delta !== 0 ? `  (${delta > 0 ? "+" : ""}${delta.toLocaleString()})` : ""}
      </div>

      <div style={h}>Columns</div>
      {schemaRows.length === 0 && !s.reordered
        ? <div style={muted}>The same columns, of the same types, in the same order.</div>
        : <>
          {schemaRows.length > 0 && <StatTable columns={[{ key: "change", label: "change" }, { key: "column", label: "column" }, { key: "old", label: "old", type: true }, { key: "new", label: "new", type: true }]} rows={schemaRows} />}
          {s.reordered && <div style={{ marginTop: 6 }}><Chip tone="warn">the columns both hold are in a different order</Chip></div>}
        </>}

      <div style={{ ...h, display: "flex", gap: 10, alignItems: "center" }}>
        Column statistics
        <label style={{ ...muted, fontWeight: "normal", display: "inline-flex", gap: 4, alignItems: "center" }}>
          <input type="checkbox" checked={onlyChanged} onChange={(e) => setOnlyChanged(e.target.checked)} /> only what changed
        </label>
      </div>
      {statRows.length === 0
        ? <div style={muted}>{report.stats.columns.length === 0 ? "No column both tables hold." : "No statistic changed."}</div>
        : <StatTable columns={[{ key: "column", label: "column" }, { key: "changed", label: "" }, { key: "nulls", label: "nulls" }, { key: "min", label: "min" }, { key: "max", label: "max" }]} rows={statRows} />}

      {k && <>
        <div style={h}>Rows by key ({k.key.join(", ")})</div>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 8 }}>
          <Chip tone={k.added ? "warn" : "neutral"}>{fmtInt(k.added)} added</Chip>
          <Chip tone={k.removed ? "warn" : "neutral"}>{fmtInt(k.removed)} removed</Chip>
          <Chip tone={k.changed ? "warn" : "neutral"}>{fmtInt(k.changed)} changed</Chip>
          <Chip tone="neutral">{fmtInt(k.unchanged)} unchanged</Chip>
        </div>
        {k.changed_by_column.some((c) => c.changed > 0) && (
          <StatTable columns={[{ key: "column", label: "column" }, { key: "changed", label: "rows changed" }]}
            rows={k.changed_by_column.filter((c) => c.changed > 0).map((c) => ({ column: c.name, changed: fmtInt(c.changed) }))} />
        )}
        {k.not_compared.length > 0 && <div style={{ ...muted, marginTop: 6 }}>Not compared: {k.not_compared.map((c) => `${c.name} (${c.reason})`).join("; ")}</div>}
        {(["added", "removed"] as const).map((kind) => k.samples[kind].length > 0 && (
          <div key={kind} style={{ marginTop: 10 }}>
            <div style={muted}>{kind}, first {k.samples[kind].length}</div>
            <StatTable columns={[...k.key, ...k.compared].map((c, i) => ({ key: String(i), label: c }))}
              rows={k.samples[kind].map((r) => Object.fromEntries([...r.key, ...r.values].map((v, i) => [String(i), cell(v)])))} />
          </div>
        ))}
        {k.samples.changed.length > 0 && (
          <div style={{ marginTop: 10 }}>
            <div style={muted}>changed, first {k.samples.changed.length}</div>
            <StatTable columns={[...k.key.map((c, i) => ({ key: "k" + i, label: c })), { key: "changes", label: "what changed" }]}
              rows={k.samples.changed.map((r) => ({
                ...Object.fromEntries(r.key.map((v, i) => ["k" + i, cell(v)])),
                changes: r.changes.map((c) => `${c.column}: ${cell(c.left)} → ${cell(c.right)}`).join("; "),
              }))} />
          </div>
        )}
      </>}

      {(report.notes?.length ?? 0) > 0 && <>
        <div style={h}>Notes</div>
        <ul style={{ ...muted, margin: 0, paddingLeft: 18 }}>{report.notes!.map((n, i) => <li key={i}>{n}</li>)}</ul>
      </>}
    </div>
  );
}
