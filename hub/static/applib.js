/* nn hub webapp — pure helpers, extracted from app.js so they run (and
 * are unit-tested) under plain `node --test` as well as in the browser.
 * Browser: loaded before app.js, exposes window.NN.
 * Node:    require()d by tests/static_applib_test.mjs. */
(function (root, factory) {
  var lib = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = lib;
  else root.NN = lib;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c];
    });
  }

  /* Machine field names → operator labels ("_v" = settable variant). */
  function fieldLabel(n) {
    var m = /^(.*)_v$/.exec(n);
    var b = m ? m[1] : n;
    var lab = b === "led" ? "LED" : b.charAt(0).toUpperCase() + b.slice(1);
    return m ? lab + " (set)" : lab;
  }

  /* location.hash → sidebar section.  The Gateway page once fell through
   * to "devices" because this mapping lived inline and was never updated
   * for a new route — keep every route here. */
  function routeSection(hash) {
    var h = String(hash || "").replace(/^#/, "") || "/";
    return h.indexOf("/settings") === 0 ? "settings"
         : h.indexOf("/logging") === 0 ? "logging"
         : h.indexOf("/factory") === 0 ? "factory"
         : h.indexOf("/gateway") === 0 ? "gateway"
         : h.indexOf("/automation") === 0 ? "automation" : "devices";
  }

  /* journald tail line → {time, msg, lvl} ('' | 'e' | 'w').  The raw
   * prefix ("<iso-ts> host proc[pid]:") eats half the viewer width. */
  function parseLogLine(l) {
    var m = /^\S*T(\d\d:\d\d:\d\d)\S*\s+\S+\s+\S+?\[\d+\]:\s?(.*)$/.exec(l);
    var msg = m ? m[2] : l;
    var lvl = /(^|\s)E \(|\bERROR\b|\[E /.test(msg) ? "e"
            : /(^|\s)W \(|\bWARN\b|\[W /.test(msg) ? "w" : "";
    return {time: m ? m[1] : "", msg: msg, lvl: lvl};
  }

  /* Fleet row status: what each device runs vs the catalog target.
   * Zephyr reports "MAJOR.MINOR.PATCH+TWEAK"; targets are stored as
   * plain semver, so only the base version is compared. */
  /* Running vs the catalog target.  "behind" only when the running build is
     provably OLDER: numeric dotted versions (0.0.31 < 0.0.32) or same-length
     build stamps (20260915a < 20260921a).  A newer build is "ahead"; anything
     not comparable (git hashes) is "differs" — never a false "behind". */
  function fleetStatus(running, target) {
    var r = String(running || "").trim();
    if (!r || r === "-" || r === "—") return {st: "unknown", cls: "dim"};
    if (!target) return {st: "no target", cls: "dim"};
    var base = r.split("+")[0], t = String(target).trim();
    if (base === t) return {st: "up to date", cls: "ok"};
    var cmp = compareVersions(base, t);
    if (cmp < 0) return {st: "behind", cls: "warn"};
    if (cmp > 0) return {st: "ahead", cls: "dim"};
    return {st: "differs", cls: "warn"};
  }

  /* -1 / 1 when a and b are comparable versions, 0 when they are not. */
  function compareVersions(a, b) {
    var num = /^\d+(\.\d+)*$/;
    if (num.test(a) && num.test(b)) {
      var x = a.split(".").map(Number), y = b.split(".").map(Number);
      for (var i = 0; i < Math.max(x.length, y.length); i++) {
        var d = (x[i] || 0) - (y[i] || 0);
        if (d) return d < 0 ? -1 : 1;
      }
      return 0;
    }
    var stamp = /^\d{8}[a-z]?$/;
    if (stamp.test(a) && stamp.test(b)) return a < b ? -1 : a > b ? 1 : 0;
    return 0;
  }

  /* Compact "how long ago" for status chips. */
  function fmtAgo(sec) {
    if (sec < 0) sec = 0;
    if (sec < 90) return Math.round(sec) + "s";
    if (sec < 5400) return Math.round(sec / 60) + " min";
    if (sec < 129600) return Math.round(sec / 3600) + " h";
    return Math.round(sec / 86400) + " d";
  }

  /* ── pending writes on sensor cards ─────────────────────────────────
   * A card row shows the DEVICE's last report on its control and, while a
   * write or an automation is pending, the intent as a word.  `pend` is
   * {d, at, t0}: desired value, the hub's desired_at (or "local" for this
   * browser's own click before the hub echoes it) and the local ms the
   * row started waiting -- so browser/hub clock skew cannot age it. */
  var PEND_SLOW_S = 30, PEND_FAIL_S = 120, LOCAL_ECHO_MS = 5000;

  function same(a, b) { return Math.abs(a - b) < 1e-6; }

  /* Next pending state from one hub field-cache entry
   * ({v, ts, desired?, desired_at?}).  `real` is the value after this
   * entry (the entry's v, else the previous real). */
  function nextPending(pend, real, e, nowMs) {
    if (e && typeof e.desired === "number" && !(real !== null && same(real, e.desired))) {
      if (pend && same(pend.d, e.desired)) return {d: pend.d, at: e.desired_at, t0: pend.t0};
      return {d: e.desired, at: e.desired_at, t0: nowMs};
    }
    if (!pend) return null;
    if (real !== null && same(real, pend.d)) return null;          // confirmed
    if (pend.at !== "local") return null;   // hub cleared it: superseded / expired
    // our own click the hub has not echoed yet: give the PUT a moment
    return nowMs - pend.t0 > LOCAL_ECHO_MS ? null : pend;
  }

  /* The word shown for a pending row. */
  function pendingText(d, ageS, binary, fmt) {
    if (binary) {
      var on = !!(+d);
      if (ageS < PEND_SLOW_S) return on ? "turning on…" : "turning off…";
      if (ageS < PEND_FAIL_S) return on ? "on? not confirmed" : "off? not confirmed";
      return on ? "did not turn on" : "did not turn off";
    }
    var t = "→ " + (fmt ? fmt(d) : String(d));
    if (ageS < PEND_SLOW_S) return t + "…";
    if (ageS < PEND_FAIL_S) return t + "? not confirmed";
    return t + " failed";
  }

  return {esc: esc, fieldLabel: fieldLabel, routeSection: routeSection,
          parseLogLine: parseLogLine, fleetStatus: fleetStatus,
          fmtAgo: fmtAgo, nextPending: nextPending, pendingText: pendingText,
          PEND_SLOW_S: PEND_SLOW_S};
});
