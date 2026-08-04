/* Lakeleto — progressive enhancement only.
   The page is fully printed in CSS; nothing here is required to read it. */
(function () {
  "use strict";

  /* ---- Mobile nav ---- */
  document.addEventListener("click", function (ev) {
    var btn = ev.target.closest("[data-nav-toggle]");
    var nav = document.getElementById("nav");
    if (btn && nav) {
      var open = nav.classList.toggle("open");
      btn.setAttribute("aria-expanded", open ? "true" : "false");
      return;
    }
    if (nav && nav.classList.contains("open")) {
      if (ev.target.closest("#nav a") || !ev.target.closest("#nav")) {
        nav.classList.remove("open");
        var t = document.querySelector("[data-nav-toggle]");
        if (t) t.setAttribute("aria-expanded", "false");
      }
    }
  });

  /* ---- The pull ----
     The page's single authored motion moment: the file registers layer by layer,
     keyblock then indigo then safflower, the way a print is actually made.

     It runs once, only if the press is already in the opening viewport, and only
     when the visitor has not asked for reduced motion. Replaying it on scroll
     would animate content the reader has already read, which reads as a glitch
     rather than as a press. The printed state is the CSS default, so a visitor
     with JS disabled sees the finished sheet. */
  var press = document.getElementById("press");
  var still = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (press && !still) {
    var box = press.getBoundingClientRect();
    if (box.top < window.innerHeight && box.bottom > 0) press.classList.add("replay");
  }

  /* ---- Colophon year ---- */
  var y = document.querySelector("[data-year]");
  if (y) y.textContent = new Date().getFullYear();
})();
