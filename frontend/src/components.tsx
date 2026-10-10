// Lakeleto design-system components — TSX ports of the Lakeleto Design System export.
// Faithful to the shipped SPA: 1px hairlines, one accent, monospace data, CSS-var tokens.
import { useCallback, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode, type KeyboardEvent } from "react";
import type { Column, Filters, Sort, Row } from "./api";

/** The Strata mark: a rounded tile of lake water with sediment bands — the table's rows.
 *
 * Inline SVG rather than an <img>: it costs no extra request, it scales for free, and it is the
 * same geometry the tray icon and the installer icons are drawn from (see `src/desktop.rs`), so
 * the brand has one shape wherever it appears. Kept decorative (`aria-hidden`) because the
 * wordmark beside it already carries the name. */
export function LakeletoMark({ size = 22 }: { size?: number }) {
  const id = "lakeleto-mark-gradient";
  return (
    <svg width={size} height={size} viewBox="0 0 64 64" aria-hidden="true" focusable="false" style={{ display: "block", flex: "0 0 auto" }}>
      <defs>
        <linearGradient id={id} x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#2563eb" />
          <stop offset="1" stopColor="#22d3ee" />
        </linearGradient>
      </defs>
      <rect x="6" y="6" width="52" height="52" rx="14" fill={`url(#${id})`} />
      <g fill="#fff">
        <rect x="15" y="14.5" width="34" height="5" rx="2.5" />
        <rect x="15" y="24.5" width="22" height="5" rx="2.5" fillOpacity=".92" />
        <rect x="15" y="34.5" width="29" height="5" rx="2.5" fillOpacity=".84" />
        <rect x="15" y="44.5" width="14" height="5" rx="2.5" fillOpacity=".76" />
      </g>
    </svg>
  );
}

/** Theme switch — cycles Auto → Light → Dark, persisted in localStorage. "auto" follows the OS
 * (`prefers-color-scheme`); Light/Dark set `data-theme` on <html> and override the OS. */
export function ThemeToggle() {
  const read = (): "auto" | "light" | "dark" => {
    const v = typeof localStorage !== "undefined" ? localStorage.getItem("lakeleto-theme") : null;
    return v === "light" || v === "dark" ? v : "auto";
  };
  const [theme, setTheme] = useState<"auto" | "light" | "dark">(read);
  useEffect(() => {
    const root = document.documentElement;
    if (theme === "auto") { delete root.dataset.theme; localStorage.removeItem("lakeleto-theme"); }
    else { root.dataset.theme = theme; localStorage.setItem("lakeleto-theme", theme); }
  }, [theme]);
  const next = { auto: "light", light: "dark", dark: "auto" } as const;
  const face = { auto: "◐ Auto", light: "☀ Light", dark: "☾ Dark" } as const;
  return (
    <Button size="sm" onClick={() => setTheme((t) => next[t])}
      title={`Theme: ${theme}${theme === "auto" ? " (follows your OS)" : ""} — click for ${next[theme]}`}>
      {face[theme]}
    </Button>
  );
}

/* ---------- Button ---------- */
export function Button({ variant = "default", size = "md", disabled = false, title, onClick, type = "button", children, style, ...rest }: {
  variant?: "default" | "primary"; size?: "md" | "sm"; disabled?: boolean; title?: string;
  onClick?: () => void; type?: "button" | "submit"; children: ReactNode; style?: CSSProperties;
}) {
  const primary = variant === "primary", sm = size === "sm";
  const s: CSSProperties = {
    font: "inherit", fontSize: sm ? "var(--text-12)" : undefined,
    padding: sm ? "3px 8px" : "var(--pad-control)", border: "var(--border-hairline)",
    borderColor: primary ? "transparent" : "var(--line)", borderRadius: sm ? "var(--radius-sm)" : "var(--radius-md)",
    cursor: disabled ? "not-allowed" : "pointer", background: primary ? "var(--accent)" : "var(--bg)",
    color: primary ? "var(--accent-fg)" : "var(--fg)", opacity: disabled ? 0.5 : 1,
    whiteSpace: "nowrap", lineHeight: "var(--line-body)", ...style,
  };
  return <button type={type} title={title} disabled={disabled} onClick={onClick} style={s} {...rest}>{children}</button>;
}

/* ---------- Select ---------- */
export function Select({ value, onChange, options = [], disabled = false, title, style }: {
  value: string; onChange?: (v: string) => void; options?: ({ value: string; label: string } | string)[];
  disabled?: boolean; title?: string; style?: CSSProperties;
}) {
  const s: CSSProperties = {
    font: "inherit", padding: "var(--pad-control)", border: "var(--border-hairline)",
    borderRadius: "var(--radius-md)", cursor: disabled ? "not-allowed" : "pointer",
    background: "var(--bg)", color: "var(--fg)", opacity: disabled ? 0.5 : 1, ...style,
  };
  return (
    <select value={value} title={title} disabled={disabled} onChange={(e) => onChange && onChange(e.target.value)} style={s}>
      {options.map((o) => {
        const val = typeof o === "string" ? o : o.value;
        const label = typeof o === "string" ? o : o.label;
        return <option key={val} value={val}>{label}</option>;
      })}
    </select>
  );
}

/* ---------- TextInput ---------- */
export function TextInput({ value, onChange, placeholder, mono = true, size = "md", title, spellCheck = false, onKeyDown, disabled = false, style }: {
  value: string; onChange?: (v: string) => void; placeholder?: string; mono?: boolean; size?: "md" | "sm";
  title?: string; spellCheck?: boolean; onKeyDown?: (e: KeyboardEvent<HTMLInputElement>) => void; disabled?: boolean; style?: CSSProperties;
}) {
  const sm = size === "sm";
  const s: CSSProperties = {
    width: "100%", fontFamily: mono ? "var(--font-mono)" : "var(--font-sans)",
    fontSize: sm ? "var(--text-12)" : "var(--text-ui)", padding: sm ? "2px 5px" : "var(--pad-input)",
    border: "var(--border-hairline)", borderRadius: sm ? "var(--radius-sm)" : "var(--radius-md)",
    background: "var(--bg)", color: "var(--fg)", opacity: disabled ? 0.5 : 1, ...style,
  };
  return <input value={value} placeholder={placeholder} title={title} spellCheck={spellCheck} disabled={disabled}
    onChange={(e) => onChange && onChange(e.target.value)} onKeyDown={onKeyDown} style={s} />;
}

/* ---------- Textarea ---------- */
export function Textarea({ value, onChange, placeholder, disabled = false, rows, style }: {
  value: string; onChange?: (v: string) => void; placeholder?: string; disabled?: boolean; rows?: number; style?: CSSProperties;
}) {
  const s: CSSProperties = {
    width: "100%", minHeight: "90px", padding: "var(--space-7)", border: "var(--border-hairline)",
    borderRadius: "var(--radius-lg)", background: "var(--bg)", color: "var(--fg)",
    fontFamily: "var(--font-mono)", fontSize: "var(--text-base)", resize: "vertical", opacity: disabled ? 0.5 : 1, ...style,
  };
  return <textarea value={value} placeholder={placeholder} disabled={disabled} rows={rows}
    onChange={(e) => onChange && onChange(e.target.value)} style={s} />;
}

/* ---------- Chip ---------- */
export function Chip({ tone = "indigo", children, title, style, ...rest }: {
  tone?: "indigo" | "warn" | "neutral"; children: ReactNode; title?: string; style?: CSSProperties;
}) {
  const tones: Record<string, CSSProperties> = {
    indigo: { background: "var(--chip)", color: "var(--chip-fg)" },
    warn: { background: "var(--warn-bg)", color: "var(--warn-fg)" },
    neutral: { background: "var(--panel)", color: "var(--muted)" },
  };
  const s: CSSProperties = {
    display: "inline-block", fontSize: "var(--text-12)", padding: "3px 8px",
    borderRadius: "var(--radius-pill)", whiteSpace: "nowrap", ...(tones[tone] || tones.indigo), ...style,
  };
  return <span title={title} style={s} {...rest}>{children}</span>;
}

/* ---------- Tabs ---------- */
export function Tabs({ tabs = [], value, onChange, style }: {
  tabs?: string[]; value: string; onChange?: (v: string) => void; style?: CSSProperties;
}) {
  return (
    <nav style={{ display: "flex", gap: "var(--space-1)", ...style }}>
      {tabs.map((t) => {
        const active = t === value;
        return (
          <button key={t} onClick={() => onChange && onChange(t)} style={{
            border: "none", borderBottom: `var(--accent-underline) solid ${active ? "var(--accent)" : "transparent"}`,
            borderRadius: 0, padding: "7px 12px", background: "transparent", cursor: "pointer", font: "inherit",
            color: active ? "var(--fg)" : "var(--muted)", fontWeight: active ? "var(--weight-semibold)" : "var(--weight-normal)",
          }}>{t}</button>
        );
      })}
    </nav>
  );
}

/* ---------- Banner ---------- */
export function Banner({ tone = "err", children, style }: { tone?: "err" | "warn"; children: ReactNode; style?: CSSProperties }) {
  const err = tone === "err";
  const s: CSSProperties = err
    ? { background: "var(--err-bg)", color: "var(--err-fg)", padding: "10px 12px", borderRadius: "var(--radius-lg)", whiteSpace: "pre-wrap" }
    : { background: "var(--warn-bg)", color: "var(--warn-fg)", padding: "6px 10px", borderRadius: "var(--radius-md)", fontSize: "var(--text-12)" };
  return <div style={{ ...s, ...style }}>{children}</div>;
}

/* ---------- FileBrowser ---------- */
const fmtBytes = (n?: number | null): string => {
  if (n == null) return "";
  const u = ["B", "KB", "MB", "GB", "TB"]; let v = n, i = 0;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return i ? `${v.toFixed(1)} ${u[i]}` : `${n} B`;
};
function FbEntry({ ic, name, size, onClick }: { ic: string; name: string; size?: number | null; onClick: () => void }) {
  const [hover, setHover] = useState(false);
  const row: CSSProperties = { display: "flex", gap: "var(--space-4)", alignItems: "center", padding: "4px 6px", borderRadius: "6px", cursor: "pointer", fontSize: "var(--text-base)" };
  return (
    <div role="button" tabIndex={0} onClick={onClick}
      onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(); } }}
      onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)} style={{ ...row, background: hover ? "var(--hover)" : "transparent" }}>
      <span style={{ width: 16, textAlign: "center", color: "var(--muted)" }}>{ic}</span>
      <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{name}</span>
      {size != null && <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: "var(--text-sm)" }}>{fmtBytes(size)}</span>}
    </div>
  );
}
export function FileBrowser({ cwd, parent, entries = [], onOpenDir, onOpenFile, style }: {
  cwd?: string; parent?: string | null; entries?: { name: string; path: string; kind: "dir" | "file"; size?: number | null }[];
  onOpenDir?: (p: string) => void; onOpenFile?: (p: string) => void; style?: CSSProperties;
}) {
  const aside: CSSProperties = { flex: "0 0 240px", borderRight: "var(--border-hairline)", overflow: "auto", padding: "var(--space-5)", background: "var(--panel)", ...style };
  return (
    <aside style={aside}>
      {cwd && <div style={{ fontFamily: "var(--font-mono)", fontSize: "var(--text-sm)", color: "var(--muted)", wordBreak: "break-all", marginBottom: "var(--space-4)" }}>{cwd}</div>}
      {parent != null && <FbEntry ic="↑" name=".." onClick={() => onOpenDir && onOpenDir(parent)} />}
      {entries.map((e) => (
        <FbEntry key={e.path} ic={e.kind === "dir" ? "▸" : "▤"} name={e.name} size={e.kind === "dir" ? null : e.size}
          onClick={() => (e.kind === "dir" ? onOpenDir && onOpenDir(e.path) : onOpenFile && onOpenFile(e.path))} />
      ))}
    </aside>
  );
}

/* ---------- cell text (every place a cell becomes a string) ---------- */
/** A cell as text: compact JSON for a nested value — a struct arrives as an object and a list as an
 *  array — so it shows its contents instead of `[object Object]`; `""` for null. */
export const cellText = (v: unknown): string =>
  v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);

/* ---------- shared row search (SQL results + cached results) ---------- */
/** Case-insensitive substring match across all cell values; empty query returns rows as-is. */
export const filterRows = (rows: Row[], q: string): Row[] => {
  const t = q.trim().toLowerCase();
  if (!t) return rows;
  return rows.filter((r) => Object.values(r).some((v) => v != null && cellText(v).toLowerCase().includes(t)));
};

/* ---------- StatTable (Schema / Profile / SQL results) ---------- */
export interface StatCol { key: string; label: string; type?: boolean; }
export function StatTable({ columns = [], rows = [], onRowClick, style }: { columns?: StatCol[]; rows?: Row[]; onRowClick?: (row: Row) => void; style?: CSSProperties }) {
  const table: CSSProperties = { borderCollapse: "collapse", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)", ...style };
  const cell: CSSProperties = { textAlign: "left", padding: "6px 10px", borderBottom: "var(--border-hairline)", whiteSpace: "nowrap" };
  const th: CSSProperties = { ...cell, background: "var(--panel)", fontWeight: "var(--weight-semibold)" };
  return (
    <table style={table}>
      <thead><tr>{onRowClick && <th style={th} aria-label="row actions" />}{columns.map((c) => <th key={c.key} style={th}>{c.label}</th>)}</tr></thead>
      <tbody>
        {rows.map((r, i) => (
          // The row keeps its <tr> semantics (no role/tabIndex — a row-as-button hides the cell
          // structure from assistive tech). Keyboard/AT access goes through the real <button> in
          // the leading cell; the whole-row onClick is a redundant mouse convenience.
          <tr key={i} onClick={onRowClick ? () => onRowClick(r) : undefined} style={onRowClick ? { cursor: "pointer" } : undefined}>
            {onRowClick && (
              <td style={{ ...cell, padding: "0 4px" }}>
                <button type="button" aria-label={`open row ${i + 1} details`} title="row details"
                  onClick={(e) => { e.stopPropagation(); onRowClick(r); }}
                  style={{ font: "inherit", border: "none", background: "transparent", color: "var(--muted)", cursor: "pointer", padding: "2px 4px" }}>›</button>
              </td>
            )}
            {columns.map((c) => {
              const v = r[c.key];
              const isNull = v === null || v === undefined;
              return (
                <td key={c.key} style={{ ...cell, color: isNull ? "var(--null)" : (c.type ? "var(--muted)" : "var(--fg)"), fontSize: c.type ? "var(--text-sm)" : undefined }}>
                  {isNull ? "·" : cellText(v)}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/* ---------- DataGrid (the centerpiece) ---------- */
/** Nested values arrive as JSON (see `cellText`), which needs room to be read; scalars size to
 *  their header. */
const NESTED = /^(Struct|List|LargeList|FixedSizeList|ListView|LargeListView|Map)\(/;
const colWidth = (c: Column) => NESTED.test(c.data_type || "")
  ? Math.min(420, Math.max(260, c.name.length * 9 + 30))
  : Math.min(320, Math.max(90, c.name.length * 9 + 30));
/** The tallest the grid's scroll area is drawn. Browsers stop growing an element somewhere past 17
 *  million pixels (Firefox) or 33 million (Chrome, Safari), short of a 2-million-row table, so a
 *  taller table is drawn this tall, and a position on the scrollbar maps onto its rows in
 *  proportion. */
const MAX_SCROLL_PX = 8_000_000;
/** Rows drawn above and below the view, so a scroll shows rows rather than gaps. */
const OVERSCAN = 8;
/** The row-number column's width. */
const GUTTER = 72;

/** A table of `count` rows, of which only those in view are drawn. Its rows come from `row`, and
 *  `onRange` says which are in view, so the caller can read them. */
export function DataGrid({ columns = [], count, row, onRange, sort = null, onSort, filters = {}, onFilter, showFilters = true, onOpenRow, resetKey, footer, style }: {
  columns?: Column[];
  /** Rows the grid spans. */
  count: number;
  /** The row at `i`, or `undefined` while it is being read. */
  row: (i: number) => Row | undefined;
  /** The rows [start, end) are in view, or nearly. */
  onRange?: (start: number, end: number) => void;
  sort?: Sort | null; onSort?: (c: string) => void;
  filters?: Filters; onFilter?: (c: string, v: string) => void; showFilters?: boolean;
  /** Clicking a row's number opens it. */
  onOpenRow?: (r: Row) => void;
  /** A change takes the grid back to its first row. */
  resetKey?: unknown;
  footer?: ReactNode; style?: CSSProperties;
}) {
  const scroller = useRef<HTMLDivElement>(null);
  const head = useRef<HTMLDivElement>(null);
  const [rowH, setRowH] = useState(28);
  const [viewH, setViewH] = useState(0);
  const [headH, setHeadH] = useState(0);
  const [scrollTop, setScrollTop] = useState(0);
  // Where the view is, in rows' pixels: the scrollbar's position when the table fits the scroll
  // area, and in proportion to it when the table is taller. Kept here rather than read back from
  // the scrollbar, which rounds: in a scaled table a small scroll would round away.
  const [pos, setPos] = useState(0);
  const posRef = useRef(0);
  /** A scroll position this grid set itself, so the scroll event it causes isn't taken for a drag. */
  const expected = useRef<number | null>(null);
  const [copied, setCopied] = useState<string | null>(null);
  const [hoverRow, setHoverRow] = useState<number | null>(null);
  const width = (c: Column) => colWidth(c);
  const totalWidth = GUTTER + columns.reduce((a, c) => a + width(c), 0);

  const contentH = count * rowH;
  const spacerH = Math.min(contentH, MAX_SCROLL_PX);
  const bodyH = Math.max(viewH - headH, rowH);
  const maxScroll = Math.max(spacerH - bodyH, 0);
  const maxContent = Math.max(contentH - bodyH, 0);
  const toScroll = (p: number) => (maxContent > 0 ? (p / maxContent) * maxScroll : 0);
  const toPos = (st: number) => (maxScroll > 0 ? (Math.min(st, maxScroll) / maxScroll) * maxContent : 0);
  const at = Math.min(pos, maxContent);
  const first = Math.min(Math.floor(at / rowH), Math.max(count - 1, 0));
  // Row `first` is drawn where the view starts, less the part of it scrolled past.
  const drawTop = Math.min(scrollTop, maxScroll) - (at - first * rowH);
  const start = Math.max(0, first - OVERSCAN);
  const end = Math.min(count, first + Math.ceil(bodyH / rowH) + 1 + OVERSCAN);
  const lastShown = Math.min(count, Math.floor((at + bodyH - 1) / rowH) + 1);
  const geom = useRef({ scaled: false, toScroll, toPos, maxContent, rowH, bodyH });
  geom.current = { scaled: contentH > spacerH, toScroll, toPos, maxContent, rowH, bodyH };

  const moveTo = useCallback((p: number) => {
    const g = geom.current;
    const next = Math.max(0, Math.min(p, g.maxContent));
    posRef.current = next;
    setPos(next);
    const el = scroller.current;
    if (el) {
      expected.current = g.toScroll(next);
      el.scrollTop = expected.current;
      setScrollTop(el.scrollTop);
    }
  }, []);

  const onScroll = () => {
    const el = scroller.current;
    if (!el) return;
    const st = el.scrollTop;
    setScrollTop(st);
    if (expected.current != null && Math.abs(st - expected.current) <= 1) { expected.current = null; return; }
    // The scrollbar moved by itself (a drag, the browser's own scrolling): it says where the view is.
    expected.current = null;
    posRef.current = geom.current.toPos(st);
    setPos(posRef.current);
  };

  useLayoutEffect(() => {
    const el = scroller.current;
    if (!el) return;
    const h = parseFloat(getComputedStyle(el).getPropertyValue("--row-height"));
    if (h > 0) setRowH(h);
    const measure = () => { setViewH(el.clientHeight); setHeadH(head.current?.offsetHeight ?? 0); };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    if (head.current) ro.observe(head.current);
    return () => ro.disconnect();
  }, []);

  // A table that grows or shrinks moves the scrollbar under the view, not the view.
  useLayoutEffect(() => {
    const el = scroller.current;
    if (!el) return;
    const p = Math.min(posRef.current, maxContent);
    const st = toScroll(p);
    if (p !== posRef.current) { posRef.current = p; setPos(p); }
    if (Math.abs(el.scrollTop - st) > 1) { expected.current = st; el.scrollTop = st; setScrollTop(el.scrollTop); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [count, rowH, bodyH]);

  useLayoutEffect(() => { moveTo(0); }, [resetKey, moveTo]);

  useEffect(() => { onRange?.(start, end); }, [start, end, onRange]);

  // In a table taller than the scroll area, the scrollbar's pixels are several rows' worth, so the
  // wheel and the keys move the view by the rows' pixels themselves.
  useEffect(() => {
    const el = scroller.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      const g = geom.current;
      if (!g.scaled || e.ctrlKey) return;
      e.preventDefault();
      const unit = e.deltaMode === 1 ? g.rowH : e.deltaMode === 2 ? g.bodyH : 1;
      const dx = e.deltaX || (e.shiftKey ? e.deltaY : 0);
      const dy = e.shiftKey && !e.deltaX ? 0 : e.deltaY;
      if (dy) moveTo(posRef.current + dy * unit);
      if (dx) el.scrollLeft += dx * unit;
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [moveTo]);

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if ((e.target as HTMLElement).tagName === "INPUT") return;
    const g = geom.current;
    const page = Math.max(g.bodyH - g.rowH, g.rowH);
    const step: Record<string, number> = { ArrowDown: g.rowH, ArrowUp: -g.rowH, PageDown: page, PageUp: -page };
    if (e.key in step) { e.preventDefault(); moveTo(posRef.current + step[e.key]); }
    else if (e.key === "Home" && !e.shiftKey) { e.preventDefault(); moveTo(0); }
    else if (e.key === "End" && !e.shiftKey) { e.preventDefault(); moveTo(g.maxContent); }
  };

  const copyCell = (key: string, v: unknown) => {
    navigator.clipboard?.writeText(cellText(v)).catch(() => { /* ignore */ });
    setCopied(key); setTimeout(() => setCopied((k) => (k === key ? null : k)), 500);
  };

  const gcell: CSSProperties = {
    flex: "0 0 auto", padding: "var(--pad-cell)", borderRight: "var(--border-hairline)",
    overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
    fontFamily: "var(--font-mono)", fontSize: "var(--text-base)", position: "relative",
  };
  // The row numbers stay in view as the columns scroll sideways.
  const gutter: CSSProperties = {
    flex: "0 0 auto", width: GUTTER, position: "sticky", left: 0, zIndex: 1, background: "var(--panel)",
    borderRight: "var(--border-hairline)", textAlign: "right", padding: "0 8px", overflow: "hidden",
    whiteSpace: "nowrap", fontFamily: "var(--font-mono)", fontSize: "var(--text-12)", color: "var(--muted)",
  };
  const rows: ReactNode[] = [];
  for (let i = start; i < end; i++) {
    const r = row(i);
    rows.push(
      <div key={i} onMouseEnter={() => setHoverRow(i)} onMouseLeave={() => setHoverRow((h) => (h === i ? null : h))}
        style={{ position: "absolute", top: drawTop + (i - first) * rowH, left: 0, width: totalWidth, height: rowH, display: "flex", alignItems: "center", borderBottom: "var(--border-hairline)", background: hoverRow === i ? "var(--hover)" : "var(--bg)" }}>
        <div role={r && onOpenRow ? "button" : undefined} tabIndex={r && onOpenRow ? 0 : undefined}
          onClick={r && onOpenRow ? () => onOpenRow(r) : undefined}
          onKeyDown={r && onOpenRow ? (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onOpenRow(r); } } : undefined}
          title={r && onOpenRow ? "row details" : undefined}
          style={{ ...gutter, lineHeight: `${rowH}px`, cursor: r && onOpenRow ? "pointer" : "default" }}>
          {(i + 1).toLocaleString()}
        </div>
        {r ? columns.map((c) => {
          const v = r[c.name];
          const isNull = v === undefined || v === null;
          const key = i + ":" + c.name;
          const isCopied = copied === key;
          return (
            <div key={c.name} role="button" tabIndex={0} onClick={() => copyCell(key, v)}
              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); copyCell(key, v); } }}
              title="click to copy"
              style={{
                flex: "0 0 auto", width: width(c), padding: "0 8px", overflow: "hidden", textOverflow: "ellipsis",
                whiteSpace: "nowrap", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)",
                lineHeight: `${rowH}px`, cursor: "pointer", color: isNull ? "var(--null)" : "var(--fg)",
                outline: isCopied ? "2px solid var(--accent)" : "none", outlineOffset: "-2px",
                background: isCopied ? "var(--sel)" : undefined,
              }}>
              {isNull ? "·" : cellText(v)}
            </div>
          );
        }) : <div style={{ padding: "0 8px", color: "var(--muted)", fontFamily: "var(--font-mono)", fontSize: "var(--text-12)" }}>…</div>}
      </div>,
    );
  }

  return (
    // One scroll area for both directions, so its vertical scrollbar is always at its right edge
    // and the header, sticky at its top, scrolls sideways with the columns. minWidth/minHeight:0
    // let it shrink inside <main> rather than paint over the side panels.
    <div style={{ display: "flex", flexDirection: "column", minHeight: 0, flex: "1 1 auto", minWidth: 0, ...style }}>
      <div ref={scroller} tabIndex={0} onScroll={onScroll} onKeyDown={onKeyDown} role="grid" aria-rowcount={count}
        style={{ flex: "1 1 auto", minHeight: 0, overflow: "auto", outline: "none" }}>
        <div ref={head} style={{ position: "sticky", top: 0, zIndex: 2, width: totalWidth, background: "var(--panel)", borderBottom: "var(--border-hairline)" }}>
          <div style={{ display: "flex" }}>
            <div style={{ ...gcell, ...gutter, padding: "var(--pad-cell)", fontFamily: "var(--font-sans)" }} title="row number; click one to see the row">#</div>
            {columns.map((c) => {
              const arrow = sort && sort.col === c.name ? (sort.desc ? " ▼" : " ▲") : "";
              return (
                <div key={c.name} role="button" tabIndex={0} onClick={() => onSort && onSort(c.name)}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onSort && onSort(c.name); } }}
                  title="click to sort"
                  style={{ ...gcell, width: width(c), background: "var(--panel)", fontWeight: "var(--weight-semibold)", cursor: "pointer", userSelect: "none", fontFamily: "var(--font-sans)" }}>
                  {c.name}<span style={{ color: "var(--accent)" }}>{arrow}</span>
                  <div style={{ color: "var(--muted)", fontSize: "var(--text-xs)", fontWeight: "var(--weight-normal)" }}>{c.data_type}</div>
                </div>
              );
            })}
          </div>
          {showFilters && (
            <div style={{ display: "flex" }}>
              <div style={{ ...gutter, padding: "3px 5px" }} />
              {columns.map((c) => (
                <div key={c.name} style={{ ...gcell, width: width(c), background: "var(--panel)", padding: "3px 5px" }}>
                  <input value={filters[c.name] || ""} placeholder="filter…"
                    title={"contains by default. Prefix:  >  <  >=  <=  =  !=  for comparisons; "
                      + "~ contains, !~ does not contain, ^ starts with, $ ends with. "
                      + "Type  in:a,b,c  for any of a list, or  null  /  !null  for empty cells."}
                    onChange={(e) => onFilter && onFilter(c.name, e.target.value)}
                    style={{ width: "100%", padding: "2px 5px", border: "var(--border-hairline)", borderRadius: "var(--radius-sm)", background: "var(--bg)", color: "var(--fg)", fontFamily: "var(--font-mono)", fontSize: "var(--text-12)" }} />
                </div>
              ))}
            </div>
          )}
        </div>
        <div style={{ position: "relative", height: spacerH, width: totalWidth }}>
          {rows}
        </div>
        {count === 0 && <div style={{ padding: "var(--gutter)", color: "var(--muted)" }}>No rows.</div>}
      </div>

      {footer != null && (
        <div style={{ flex: "0 0 auto", padding: "5px 14px", borderTop: "var(--border-hairline)", color: "var(--muted)", fontSize: "var(--text-12)", display: "flex", gap: "var(--space-8)", alignItems: "center" }}>
          {count > 0 && <span>rows {(first + 1).toLocaleString()}–{lastShown.toLocaleString()}</span>}
          {footer}
        </div>
      )}
    </div>
  );
}
