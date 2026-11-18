/* Lakeleto — progressive enhancement only.
   Everything is readable with JavaScript off: the first install command, the first query and
   its output, and the first screenshot are in the HTML. This adds tabs, copy buttons and the
   live query. */
(function () {
  "use strict";
  var still = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  /* ---- Mobile nav ---- */
  document.addEventListener("click", function (ev) {
    var btn = ev.target.closest("[data-nav-toggle]");
    var nav = document.getElementById("nav");
    if (btn && nav) {
      var open = nav.classList.toggle("open");
      btn.setAttribute("aria-expanded", open ? "true" : "false");
      return;
    }
    if (nav && nav.classList.contains("open") && (ev.target.closest("#nav a") || !ev.target.closest("#nav"))) {
      nav.classList.remove("open");
      var t = document.querySelector("[data-nav-toggle]");
      if (t) t.setAttribute("aria-expanded", "false");
    }
  });

  /* ---- Tabs (install chooser, screenshot gallery) — WAI-ARIA tabs pattern ---- */
  document.querySelectorAll("[data-tabs]").forEach(function (root) {
    var tabs = Array.prototype.slice.call(root.querySelectorAll('[role="tab"]'));
    function select(tab, focus) {
      tabs.forEach(function (t) {
        var on = t === tab;
        t.setAttribute("aria-selected", on ? "true" : "false");
        t.tabIndex = on ? 0 : -1;
        var panel = document.getElementById(t.getAttribute("aria-controls"));
        if (panel) panel.hidden = !on;
      });
      if (focus) tab.focus();
    }
    tabs.forEach(function (t, i) {
      t.addEventListener("click", function () { select(t, false); });
      t.addEventListener("keydown", function (e) {
        var j = e.key === "ArrowRight" ? i + 1 : e.key === "ArrowLeft" ? i - 1 : e.key === "Home" ? 0 : e.key === "End" ? tabs.length - 1 : null;
        if (j === null) return;
        e.preventDefault();
        select(tabs[(j + tabs.length) % tabs.length], true);
      });
    });
    // Default the install chooser to the visitor's platform.
    if (root.id === "install") {
      var ua = navigator.userAgent;
      var id = /Mac/.test(ua) ? "t-mac" : /Linux|X11/.test(ua) && !/Android/.test(ua) ? "t-linux" : null;
      var t = id && document.getElementById(id);
      if (t) select(t, false);
    }
  });

  /* ---- Copy buttons ---- */
  document.addEventListener("click", function (ev) {
    var b = ev.target.closest("[data-copy]");
    if (!b) return;
    var code = b.parentNode.querySelector("code");
    if (!code || !navigator.clipboard) return;
    navigator.clipboard.writeText(code.textContent).then(function () {
      var was = b.textContent; b.textContent = "Copied";
      setTimeout(function () { b.textContent = was; }, 1200);
    }).catch(function () {});
  });

  /* ---- The live query ----
     Every command and output here is verbatim what lakeleto 0.4.0 printed for
     examples/sample-data/orders.parquet. If the sample or the output format changes, re-run
     them and paste again; do not edit a number by hand. */
  var Q = [
    { label: "Revenue by city",
      cmd: 'lakeleto query "SELECT city, count(*) AS orders, round(sum(amount_usd), 2) AS revenue FROM t GROUP BY city ORDER BY revenue DESC LIMIT 5" --file orders.parquet',
      meta: "SQL · DataFusion",
      out: "| city      | orders | revenue  |\n|-----------|--------|----------|\n| Sydney    | 503    | 66834.05 |\n| Toronto   | 517    | 66242.49 |\n| Mumbai    | 515    | 65590.18 |\n| Singapore | 550    | 64419.91 |\n| London    | 493    | 62170.92 |\n\n5 row(s)" },
    { label: "Orders per month",
      cmd: "lakeleto query \"SELECT date_trunc('month', order_ts) AS month, count(*) AS orders FROM t GROUP BY 1 ORDER BY 1\" --file orders.parquet",
      meta: "SQL · DataFusion",
      out: "| month               | orders |\n|---------------------|--------|\n| 2024-01-01T00:00:00 | 818    |\n| 2024-02-01T00:00:00 | 823    |\n| 2024-03-01T00:00:00 | 869    |\n| 2024-04-01T00:00:00 | 844    |\n| 2024-05-01T00:00:00 | 857    |\n| 2024-06-01T00:00:00 | 789    |\n\n6 row(s)" },
    { label: "Big refunds",
      cmd: 'lakeleto query "SELECT order_id, customer, city, amount_usd FROM t WHERE is_refunded AND amount_usd > 150 ORDER BY amount_usd DESC LIMIT 5" --file orders.parquet',
      meta: "SQL · DataFusion",
      out: "| order_id | customer  | city      | amount_usd |\n|----------|-----------|-----------|------------|\n| 104001   | cust_0268 | Tokyo     | 735.25     |\n| 101456   | cust_0261 | Singapore | 574.49     |\n| 104004   | cust_0602 | Sydney    | 552.28     |\n| 102600   | cust_0511 | Toronto   | 551.74     |\n| 102702   | cust_0051 | Sydney    | 535.68     |\n\n5 row(s)" },
    { label: "What's in it?",
      cmd: "lakeleto schema orders.parquet",
      meta: "footer only · no rows read",
      out: "source : orders.parquet\nformat : parquet\nengine : local\nrows   : 5000\ncolumns: 11\n\ncolumn        type           null?\n------------  -------------  -----\norder_id      Int32          yes\ncustomer      Utf8           yes\ncity          Utf8           yes\ntier          Utf8           yes\namount_usd    Float64        yes\nquantity      Int32          yes\ndiscount_pct  Float64        yes\nis_refunded   Boolean        yes\nrating        Float64        yes\norder_ts      Timestamp(ns)  yes\nship_date     Date32         yes" },
    { label: "Nulls & ranges",
      cmd: "lakeleto profile --fast orders.parquet",
      meta: "footer statistics · no scan",
      out: "source : orders.parquet\nengine : local\nrows   : 5000 (footer stats — no scan)\n\n| column       | type          | nulls | null% | distinct | min                 | max                 |\n|--------------|---------------|-------|-------|----------|---------------------|---------------------|\n| order_id     | Int32         | 0     | 0.0%  | —        | 100000              | 104999              |\n| customer     | Utf8          | 242   | 4.8%  | —        | cust_0001           | cust_0900           |\n| city         | Utf8          | 0     | 0.0%  | —        | Berlin              | Toronto             |\n| tier         | Utf8          | 0     | 0.0%  | —        | enterprise          | pro                 |\n| amount_usd   | Float64       | 0     | 0.0%  | —        | 5.01                | 1172.35             |\n| quantity     | Int32         | 0     | 0.0%  | —        | 1                   | 19                  |\n| discount_pct | Float64       | 0     | 0.0%  | —        | -0.0                | 0.25                |\n| is_refunded  | Boolean       | 0     | 0.0%  | —        | false               | true                |\n| rating       | Float64       | 774   | 15.5% | —        | 1.0                 | 5.0                 |\n| order_ts     | Timestamp(ns) | 0     | 0.0%  | —        | 2024-01-01T00:59:00 | 2024-06-28T22:13:00 |\n| ship_date    | Date32        | 0     | 0.0%  | —        | 2024-01-01          | 2024-06-28          |" },
    { label: "First rows as CSV",
      cmd: "lakeleto head orders.parquet -n 5 -o csv",
      meta: "-o csv · pipe-friendly",
      out: "order_id,customer,city,tier,amount_usd,quantity,discount_pct,is_refunded,rating,order_ts,ship_date\n100000,cust_0332,Berlin,pro,23.29,13,0.05,false,3.0,2024-06-13T04:40:00,2024-01-22\n100001,cust_0155,Nairobi,enterprise,66.53,1,0.0,false,4.0,2024-03-10T08:12:00,2024-02-03\n100002,cust_0405,Toronto,free,164.86,12,0.1,false,1.0,2024-06-21T00:31:00,2024-06-12\n100003,cust_0667,New York,pro,59.14,16,0.25,false,4.0,2024-02-24T12:57:00,2024-01-11\n100004,cust_0050,New York,free,53.34,16,0.05,false,4.0,2024-04-02T18:46:00,2024-04-05" }
  ];

  var pick = document.getElementById("qpick");
  var cmdEl = document.getElementById("qcmd");
  var outEl = document.getElementById("qout");
  var metaEl = document.getElementById("qmeta");
  if (pick && cmdEl && outEl) {
    var timer = null, auto = true, current = 0;
    var buttons = Q.map(function (q, i) {
      var b = document.createElement("button");
      b.type = "button"; b.textContent = q.label;
      b.setAttribute("aria-pressed", i === 0 ? "true" : "false");
      b.addEventListener("click", function () { auto = false; play(i); });
      pick.appendChild(b);
      return b;
    });
    function esc(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;"); }
    function play(i) {
      clearTimeout(timer);
      current = i;
      buttons.forEach(function (b, j) { b.setAttribute("aria-pressed", j === i ? "true" : "false"); });
      var q = Q[i];
      metaEl.textContent = q.meta;
      if (still) { cmdEl.innerHTML = '<span class="p">$</span> ' + esc(q.cmd); outEl.textContent = q.out; return; }
      outEl.textContent = "";
      var n = 0, step = Math.max(1, Math.round(q.cmd.length / 55));
      (function type() {
        n = Math.min(q.cmd.length, n + step);
        cmdEl.innerHTML = '<span class="p">$</span> ' + esc(q.cmd.slice(0, n)) + '<span class="caret">&nbsp;</span>';
        if (n < q.cmd.length) { timer = setTimeout(type, 18); return; }
        timer = setTimeout(function () {
          cmdEl.innerHTML = '<span class="p">$</span> ' + esc(q.cmd);
          var lines = q.out.split("\n"), k = 0;
          outEl.innerHTML = "";
          (function line() {
            var s = document.createElement("span");
            s.className = "row"; s.style.display = "block";
            s.textContent = lines[k] || " ";
            outEl.appendChild(s);
            outEl.classList.add("printing");
            if (++k < lines.length) { timer = setTimeout(line, 45); return; }
            if (auto) timer = setTimeout(function () { if (auto) play((current + 1) % Q.length); }, 4200);
          })();
        }, 260);
      })();
    }
    // Start the loop only once the terminal is on screen, and stop when it leaves.
    var term = outEl.closest(".term");
    if ("IntersectionObserver" in window && !still) {
      var started = false;
      new IntersectionObserver(function (es) {
        es.forEach(function (e) {
          if (e.isIntersecting && !started) { started = true; play(0); }
          else if (!e.isIntersecting && started && auto) { clearTimeout(timer); started = false; }
        });
      }, { threshold: 0.35 }).observe(term);
    }
  }

  /* ---- Docs: mark the section being read in the table of contents ---- */
  var toc = document.querySelector(".toc");
  if (toc) {
    var links = {};
    toc.querySelectorAll('a[href^="#"]').forEach(function (a) { links[a.getAttribute("href").slice(1)] = a; });
    var heads = Array.prototype.slice.call(document.querySelectorAll(".doc h2[id], .doc h3[id]")).filter(function (h) { return links[h.id]; });
    var mark = function (id) {
      toc.querySelectorAll("a.on").forEach(function (a) { a.classList.remove("on"); });
      var a = links[id];
      if (!a) return;
      a.classList.add("on");
      // Keep it visible inside the TOC's own scroll box, without moving the page.
      var top = a.offsetTop - toc.offsetTop, bottom = top + a.offsetHeight;
      if (top < toc.scrollTop) toc.scrollTop = top - 40;
      else if (bottom > toc.scrollTop + toc.clientHeight) toc.scrollTop = bottom - toc.clientHeight + 40;
    };
    // The last heading above the upper third of the viewport is the one being read.
    var scan = function () {
      var cur = null;
      heads.forEach(function (h) { if (h.getBoundingClientRect().top < window.innerHeight * 0.33) cur = h.id; });
      if (cur) mark(cur);
    };
    var ticking = false;
    window.addEventListener("scroll", function () {
      if (ticking) return; ticking = true;
      requestAnimationFrame(function () { ticking = false; scan(); });
    }, { passive: true });
    scan();
  }

  /* ---- Colophon year ---- */
  var y = document.querySelector("[data-year]");
  if (y) y.textContent = new Date().getFullYear();
})();
