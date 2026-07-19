// Lakeleto design-system components — TSX ports of the Lakeleto Design System export.
// Faithful to the shipped SPA: 1px hairlines, one accent, monospace data, CSS-var tokens.
import { useState, type CSSProperties, type ReactNode, type KeyboardEvent } from "react";
import type { Column, Filters, Sort, Row } from "./api";

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

/* ---------- shared row search (SQL results + cached results) ---------- */
/** Case-insensitive substring match across all cell values; empty query returns rows as-is. */
export const filterRows = (rows: Row[], q: string): Row[] => {
  const t = q.trim().toLowerCase();
  if (!t) return rows;
  return rows.filter((r) => Object.values(r).some((v) => v != null && String(v).toLowerCase().includes(t)));
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
                  {isNull ? "·" : String(v)}
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
const colWidth = (name: string) => Math.min(320, Math.max(90, name.length * 9 + 30));
export function DataGrid({ columns = [], rows = [], sort = null, onSort, filters = {}, onFilter, showFilters = true, footer, style }: {
  columns?: Column[]; rows?: Row[]; sort?: Sort | null; onSort?: (c: string) => void;
  filters?: Filters; onFilter?: (c: string, v: string) => void; showFilters?: boolean; footer?: ReactNode; style?: CSSProperties;
}) {
  const [copied, setCopied] = useState<string | null>(null);
  const [hoverRow, setHoverRow] = useState<number | null>(null);
  const width = (c: Column) => colWidth(c.name);
  const totalWidth = columns.reduce((a, c) => a + width(c), 0);

  const copyCell = (key: string, v: unknown) => {
    navigator.clipboard?.writeText(v == null ? "" : String(v)).catch(() => { /* ignore */ });
    setCopied(key); setTimeout(() => setCopied((k) => (k === key ? null : k)), 500);
  };

  const gcell: CSSProperties = {
    flex: "0 0 auto", padding: "var(--pad-cell)", borderRight: "var(--border-hairline)",
    overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
    fontFamily: "var(--font-mono)", fontSize: "var(--text-base)", position: "relative",
  };

  return (
    // The header + body are `width: totalWidth` (sum of column widths), which can exceed the
    // viewport. This root MUST own the horizontal scroll and clip to its flex box, else the wide
    // grid paints OUTSIDE <main> and overlaps the side panels (Row detail / History). minWidth:0
    // lets it shrink inside the flex row; overflowX scrolls the columns; the body keeps its own
    // vertical scroll so the header row stays put.
    <div style={{ display: "flex", flexDirection: "column", minHeight: 0, flex: "1 1 auto", minWidth: 0, overflowX: "auto", overflowY: "hidden", ...style }}>
      <div style={{ overflow: "hidden", flex: "0 0 auto", borderBottom: "var(--border-hairline)", width: totalWidth }}>
        <div style={{ display: "flex" }}>
          {columns.map((c) => {
            const arrow = sort && sort.col === c.name ? (sort.desc ? " ▼" : " ▲") : "";
            return (
              <div key={c.name} role="button" tabIndex={0} onClick={() => onSort && onSort(c.name)}
                onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onSort && onSort(c.name); } }}
                title="click to sort"
                style={{ ...gcell, width: width(c), background: "var(--panel)", fontWeight: "var(--weight-semibold)", cursor: "pointer", userSelect: "none", fontFamily: "var(--font-sans)" }}>
                {c.name}<span style={{ color: "var(--accent)" }}>{arrow}</span>
                <div style={{ color: "var(--muted)", fontSize: "var(--text-xs)", fontWeight: "var(--weight-normal)" }}>{c.data_type}</div>
                <div style={{ position: "absolute", top: 0, right: 0, width: 7, height: "100%", cursor: "col-resize" }} />
              </div>
            );
          })}
        </div>
        {showFilters && (
          <div style={{ display: "flex" }}>
            {columns.map((c) => (
              <div key={c.name} style={{ ...gcell, width: width(c), background: "var(--panel)", padding: "3px 5px" }}>
                <input value={filters[c.name] || ""} placeholder="filter…"
                  title="contains by default; prefix >  <  >=  <=  =  != for comparisons"
                  onChange={(e) => onFilter && onFilter(c.name, e.target.value)}
                  style={{ width: "100%", padding: "2px 5px", border: "var(--border-hairline)", borderRadius: "var(--radius-sm)", background: "var(--bg)", color: "var(--fg)", fontFamily: "var(--font-mono)", fontSize: "var(--text-12)" }} />
              </div>
            ))}
          </div>
        )}
      </div>

      <div style={{ flex: "1 1 auto", overflow: "auto", minHeight: 0, width: totalWidth }}>
        {rows.map((row, ri) => (
          <div key={ri} onMouseEnter={() => setHoverRow(ri)} onMouseLeave={() => setHoverRow(null)}
            style={{ display: "flex", height: "var(--row-height)", alignItems: "center", borderBottom: "var(--border-hairline)", background: hoverRow === ri ? "var(--hover)" : "transparent" }}>
            {columns.map((c) => {
              const v = row[c.name];
              const isNull = v === undefined || v === null;
              const key = ri + ":" + c.name;
              const isCopied = copied === key;
              return (
                <div key={c.name} role="button" tabIndex={0} onClick={() => copyCell(key, v)}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); copyCell(key, v); } }}
                  title="click to copy"
                  style={{
                    flex: "0 0 auto", width: width(c), padding: "0 8px", overflow: "hidden", textOverflow: "ellipsis",
                    whiteSpace: "nowrap", fontFamily: "var(--font-mono)", fontSize: "var(--text-base)",
                    lineHeight: "var(--row-height)", cursor: "pointer", color: isNull ? "var(--null)" : "var(--fg)",
                    outline: isCopied ? "2px solid var(--accent)" : "none", outlineOffset: "-2px",
                    background: isCopied ? "var(--sel)" : undefined,
                  }}>
                  {isNull ? "·" : String(v)}
                </div>
              );
            })}
          </div>
        ))}
      </div>

      {footer != null && (
        <div style={{ flex: "0 0 auto", padding: "5px 14px", borderTop: "var(--border-hairline)", color: "var(--muted)", fontSize: "var(--text-12)", display: "flex", gap: "var(--space-8)", alignItems: "center" }}>
          {footer}
        </div>
      )}
    </div>
  );
}
