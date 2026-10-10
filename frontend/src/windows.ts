// The grid's rows, read from `/v1/rows` a window at a time as the grid scrolls. Only the windows
// in view, and the one after them, are asked for. They are kept until enough are held, then the
// ones farthest from view are dropped, so a table of any length scrolls in bounded memory.
import { useCallback, useEffect, useRef, useState } from "react";
import type { Backend, Column, Filters, Row, RowsResp, Sort } from "./api";

/** Rows per request. */
export const WINDOW = 200;
/** Windows held at once (8,000 rows). Past that, the ones farthest from view are dropped. */
const KEEP = 40;
/** Requests in flight at once. */
const IN_FLIGHT = 2;
/** How often a scroll asks for windows, at most: a drag across a large table asks for the rows
 *  where it stops, not for every window it passes. */
const THROTTLE_MS = 80;
/** How long a window that couldn't be read waits before it is asked for again: this, doubled for
 *  each failure since, up to 30 seconds. */
const RETRY_MS = 3000;
const retryAfter = (tries: number) => Math.min(RETRY_MS * 2 ** (tries - 1), 30_000);

export interface RowQuery { path: string; sort: Sort | null; filters: Filters; flatten: boolean; jsonPath: string | null; }

export interface RowWindows {
  /** Has the query's first window been read? */
  ready: boolean;
  /** The table's columns, once its first window is read. */
  columns: Column[];
  /** Rows the grid spans: the table's, when `exact`; else the rows read so far and, while more
   *  may follow, one window more. */
  count: number;
  /** Is `count` the table's row count? */
  exact: boolean;
  /** Rows known to exist. */
  seen: number;
  /** The row at `i`, once its window is read. */
  row(i: number): Row | undefined;
  /** The grid shows rows [start, end): read the windows they are in. */
  want(start: number, end: number): void;
  /** From the last window read: the rows the engine read for it. */
  scanned: number;
  /** Did a sort or filter run over a capped part of the table? Then `count` is what matched there. */
  bounded: boolean;
  /** Goes up when a query's first window replaces the rows shown, so the grid returns to the top. */
  epoch: number;
  /** Is a window being read? */
  loading: boolean;
  /** Why the first window couldn't be read: there is nothing to show. */
  error: string | null;
  /** Why a later window couldn't be read. It is asked for again, less often each time. */
  windowError: string | null;
}

/** The rows one query has read. */
interface Read {
  query: RowQuery;
  /** Another way of reading the source: another table. */
  readKey: string;
  gen: number;
  columns: Column[];
  windows: Map<number, Row[]>;
  /** Windows that couldn't be read: when last, and how many times running. */
  failed: Map<number, { at: number; tries: number }>;
  /** The table's row count, when the server knows it. */
  total: number | null;
  /** What the server counted otherwise: a lower bound. */
  matched: number;
  /** Rows known to exist. */
  seen: number;
  /** Where the rows end, once a window came back short. */
  end: number | null;
  scanned: number;
  bounded: boolean;
}

/** What reads the source: another value is another table, whatever its sort and filters. */
const readKeyOf = (q: RowQuery) => JSON.stringify([q.path, q.flatten, q.jsonPath]);

/** Keep window `w`'s rows, and what the reply says about the table's length. */
function absorb(read: Read, w: number, r: RowsResp) {
  read.windows.set(w, r.rows);
  read.failed.delete(w);
  if (!read.columns.length) read.columns = r.columns;
  read.scanned = r.scanned_rows;
  read.bounded = r.bounded;
  read.seen = Math.max(read.seen, r.offset + r.num_rows);
  if (r.total_known) read.total = r.matched_rows;
  else read.matched = Math.max(read.matched, r.matched_rows);
  // A short window is the last one. Windows are only asked for up to one past the rows already
  // seen, so it starts where those end, and its end is the table's.
  if (r.num_rows < WINDOW) read.end = Math.max(read.seen, r.offset + r.num_rows);
}

/** How many rows the grid spans for `read`, whether that is the table's count, and how many are
 *  known to exist. */
function extent(read: Read): { count: number; exact: boolean; seen: number } {
  if (read.total != null) return { count: read.total, exact: true, seen: read.total };
  const seen = Math.max(read.seen, read.matched);
  if (read.bounded) return { count: seen, exact: false, seen };
  if (read.end != null) return { count: read.end, exact: true, seen: read.end };
  return { count: seen + WINDOW, exact: false, seen };
}

/** The rows of `query`, read as the grid asks for them; `null` reads nothing. */
export function useRowWindows(backend: Backend, query: RowQuery | null): RowWindows {
  const key = query && JSON.stringify([query.path, query.sort, query.filters, query.flatten, query.jsonPath]);
  const [, setVersion] = useState(0);
  const redraw = () => setVersion((v) => v + 1);
  const [epoch, setEpoch] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [windowError, setWindowError] = useState<string | null>(null);

  const backendRef = useRef(backend);
  backendRef.current = backend;
  /** The rows shown. */
  const shown = useRef<Read | null>(null);
  /** Every request carries the generation it was made in, and a reply from an older one is dropped. */
  const gen = useRef(0);
  /** A new query whose first window is on its way: what is shown stays until it lands. */
  const next = useRef<number | null>(null);
  const inflight = useRef(new Set<number>());
  const wanted = useRef<[number, number]>([0, 0]);
  const timer = useRef<number | null>(null);
  /** A wake-up for a window waiting out a failure, and when it fires. */
  const retry = useRef<{ id: number; at: number } | null>(null);

  const pump = useCallback(() => {
    const read = shown.current;
    if (!read || read.gen !== gen.current || next.current != null) return;
    const { count } = extent(read);
    const [start, end] = wanted.current;
    if (count === 0 || end <= start) return;
    const first = Math.floor(start / WINDOW);
    const last = Math.min(Math.floor((Math.min(end, count) - 1) / WINDOW) + 1, Math.floor((count - 1) / WINDOW));
    // Nearest the middle of the view first; the window after the view last, as a read-ahead.
    const mid = (first + last - 1) / 2;
    const order: number[] = [];
    for (let w = first; w <= last; w++) order.push(w);
    order.sort((a, b) => Math.abs(a - mid) - Math.abs(b - mid));
    const now = Date.now();
    let wake = Infinity;
    for (const w of order) {
      if (inflight.current.size >= IN_FLIGHT) break;
      if (read.windows.has(w) || inflight.current.has(w)) continue;
      const failed = read.failed.get(w);
      const ready = failed ? failed.at + retryAfter(failed.tries) : now;
      if (ready > now) { wake = Math.min(wake, ready); continue; }
      inflight.current.add(w);
      const g = read.gen;
      backendRef.current.rows({ ...read.query, offset: w * WINDOW, limit: WINDOW })
        .then((r) => {
          if (g !== gen.current) return;
          absorb(read, w, r);
          setWindowError(null);
          // Past the windows to keep, drop the farthest from view.
          const at = (wanted.current[0] + wanted.current[1]) / 2 / WINDOW;
          const far = [...read.windows.keys()].sort((a, b) => Math.abs(b - at) - Math.abs(a - at));
          for (const f of far) { if (read.windows.size <= KEEP) break; read.windows.delete(f); }
        })
        .catch((e) => {
          if (g !== gen.current) return;
          const tries = (read.failed.get(w)?.tries ?? 0) + 1;
          read.failed.set(w, { at: Date.now(), tries });
          setWindowError((e as Error).message);
        })
        .finally(() => {
          if (g !== gen.current) return;
          inflight.current.delete(w);
          redraw();
          pump();
        });
    }
    // A window waiting out a failure is asked for again when the wait ends, whether or not the
    // grid scrolls. The wake-up is a millisecond late rather than early: a timer can fire before
    // Date.now() says its delay has passed, and this window would then be skipped with nothing
    // left to wake it.
    if (wake < Infinity && (retry.current == null || retry.current.at > wake)) {
      if (retry.current != null) window.clearTimeout(retry.current.id);
      const id = window.setTimeout(() => { retry.current = null; pump(); }, wake - now + 1);
      retry.current = { id, at: wake };
    }
  }, []);

  useEffect(() => {
    const g = ++gen.current;
    inflight.current.clear();
    if (retry.current != null) { window.clearTimeout(retry.current.id); retry.current = null; }
    next.current = null;
    setError(null);
    setWindowError(null);
    if (!query) return;
    // Another way of reading the source is another table, so what is shown goes now. A new sort or
    // filter is the same table: its rows stay up until the new ones land.
    if (shown.current && shown.current.readKey !== readKeyOf(query)) shown.current = null;
    next.current = g;
    redraw();
    backendRef.current.rows({ ...query, offset: 0, limit: WINDOW })
      .then((r) => {
        if (g !== gen.current) return;
        const read: Read = {
          query, readKey: readKeyOf(query), gen: g, columns: r.columns, windows: new Map(), failed: new Map(),
          total: null, matched: 0, seen: 0, end: null, scanned: 0, bounded: false,
        };
        absorb(read, 0, r);
        shown.current = read;
        // The grid goes back to the top for these rows, so the windows wanted are the top's, not
        // those of wherever the last rows were scrolled to.
        wanted.current = [0, wanted.current[1] - wanted.current[0]];
        setEpoch((e) => e + 1);
      })
      .catch((e) => {
        if (g !== gen.current) return;
        shown.current = null;
        setError((e as Error).message);
      })
      .finally(() => {
        if (g !== gen.current) return;
        next.current = null;
        redraw();
        pump();
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [backend, key, pump]);

  // Replies to a hook that is gone change nothing. React can run this and then the effects again
  // (StrictMode, Fast Refresh), so the timers are forgotten as well as stopped.
  useEffect(() => () => {
    gen.current++;
    if (timer.current != null) { window.clearTimeout(timer.current); timer.current = null; }
    if (retry.current != null) { window.clearTimeout(retry.current.id); retry.current = null; }
  }, []);

  const want = useCallback((start: number, end: number) => {
    wanted.current = [start, end];
    if (timer.current == null) {
      timer.current = window.setTimeout(() => { timer.current = null; pump(); }, THROTTLE_MS);
    }
  }, [pump]);

  // What reads the source another way is never shown, not even for the render before the effect
  // above drops it.
  const read = shown.current && query && shown.current.readKey === readKeyOf(query) ? shown.current : null;
  const { count, exact, seen } = read ? extent(read) : { count: 0, exact: true, seen: 0 };
  const row = (i: number) => read?.windows.get(Math.floor(i / WINDOW))?.[i % WINDOW];
  return {
    ready: read != null, columns: read?.columns ?? [], count, exact, seen, row, want,
    scanned: read?.scanned ?? 0, bounded: read?.bounded ?? false, epoch,
    loading: next.current != null || inflight.current.size > 0,
    error, windowError,
  };
}
