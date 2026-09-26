// Unit tests for hub/static/applib.js — run with:  node --test tests/
// Plain node:test, no framework installs.  These are the pure webapp
// helpers; browser-level behavior stays with events_state_test.py.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const NN = require("../hub/static/applib.js");

test("esc escapes html metacharacters", () => {
  assert.equal(NN.esc('<b a="1">&'), "&lt;b a=&quot;1&quot;&gt;&amp;");
  assert.equal(NN.esc(123), "123");
});

test("fieldLabel maps firmware names to operator labels", () => {
  assert.equal(NN.fieldLabel("led"), "LED");
  assert.equal(NN.fieldLabel("led_v"), "LED (set)");
  assert.equal(NN.fieldLabel("temperature"), "Temperature");
  assert.equal(NN.fieldLabel("button_v"), "Button (set)");
});

test("routeSection covers every sidebar route", () => {
  // the Gateway page once highlighted 'devices' — the exact regression
  // this mapping is now tested against
  assert.equal(NN.routeSection("#/gateway"), "gateway");
  assert.equal(NN.routeSection("#/gateway/whatever"), "gateway");
  assert.equal(NN.routeSection("#/settings/debug"), "settings");
  assert.equal(NN.routeSection("#/logging/inferd"), "logging");
  assert.equal(NN.routeSection("#/factory/fleet"), "factory");
  assert.equal(NN.routeSection("#/automation/devices"), "automation");
  for (const h of ["", "#/", "#/live/cam0", "#/devstatus/c6-s1"])
    assert.equal(NN.routeSection(h), "devices");
});

test("parseLogLine strips the journald prefix and tints levels", () => {
  const p = NN.parseLogLine(
    "2026-08-28T19:43:04+0800 orangepiplus python3[3161335]: hello world");
  assert.deepEqual(p, {time: "19:43:04", msg: "hello world", lvl: ""});
  assert.equal(NN.parseLogLine(
    "2026-08-28T19:43:04+0800 h p[1]: E (123) boom").lvl, "e");
  assert.equal(NN.parseLogLine(
    "2026-08-28T19:43:04+0800 h p[1]: [W proto] odd").lvl, "w");
  // unparseable lines pass through whole rather than vanishing
  const raw = NN.parseLogLine("no prefix at all");
  assert.equal(raw.msg, "no prefix at all");
  assert.equal(raw.time, "");
});

test("fleetStatus compares base versions (zephyr +TWEAK)", () => {
  assert.deepEqual(NN.fleetStatus("0.0.32+0", "0.0.32"),
                   {st: "up to date", cls: "ok"});
  assert.deepEqual(NN.fleetStatus("0.0.31+0", "0.0.32"),
                   {st: "behind", cls: "warn"});
  assert.deepEqual(NN.fleetStatus("", "0.0.32"),
                   {st: "unknown", cls: "dim"});
  assert.deepEqual(NN.fleetStatus("abc1234", ""),
                   {st: "no target", cls: "dim"});
});

test("fleetStatus never calls a newer build 'behind'", () => {
  assert.deepEqual(NN.fleetStatus("0.0.47+0", "0.0.32"), {st: "ahead", cls: "dim"});
  assert.deepEqual(NN.fleetStatus("0.0.9", "0.0.32"), {st: "behind", cls: "warn"}, "numeric, not string, compare");
  assert.deepEqual(NN.fleetStatus("20260915a", "20260921a"), {st: "behind", cls: "warn"});
  assert.deepEqual(NN.fleetStatus("20260922a", "20260921a"), {st: "ahead", cls: "dim"});
  assert.deepEqual(NN.fleetStatus("b3df5b8", "c72017a"), {st: "differs", cls: "warn"});
  assert.deepEqual(NN.fleetStatus("-", "20260921a"), {st: "unknown", cls: "dim"});
});

test("fmtAgo picks sane units", () => {
  assert.equal(NN.fmtAgo(5), "5s");
  assert.equal(NN.fmtAgo(300), "5 min");
  assert.equal(NN.fmtAgo(7200), "2 h");
  assert.equal(NN.fmtAgo(200000), "2 d");
  assert.equal(NN.fmtAgo(-10), "0s");
});

// ── pending writes on sensor cards ─────────────────────────────────────
// The control shows only what the device reported; the word shows the
// intent until the device confirms.  These pin the transitions.
const T = 1_000_000;

test("hub intent becomes pending, keeps its age across re-syncs", () => {
  let p = NN.nextPending(null, 0, {v: 0, desired: 1, desired_at: 50}, T);
  assert.deepEqual(p, {d: 1, at: 50, t0: T});
  p = NN.nextPending(p, 0, {v: 0, desired: 1, desired_at: 51}, T + 9000);
  assert.equal(p.t0, T, "same intent must not restart the clock");
});

test("a click is pending locally, then adopts the hub's record", () => {
  let p = {d: 1, at: "local", t0: T};
  p = NN.nextPending(p, 0, {v: 0}, T + 1000);           // hub not echoed yet
  assert.equal(p.at, "local");
  p = NN.nextPending(p, 0, {v: 0, desired: 1, desired_at: 77}, T + 2000);
  assert.deepEqual(p, {d: 1, at: 77, t0: T});
});

test("confirmed when the device reports the desired value", () => {
  const p = {d: 1, at: 77, t0: T};
  assert.equal(NN.nextPending(p, 1, {v: 1}, T + 3000), null);
  assert.equal(NN.nextPending({d: 1, at: "local", t0: T}, 1, {v: 1}, T + 100), null);
});

test("hub cleared it (superseded / expired / rejected) -> not pending", () => {
  assert.equal(NN.nextPending({d: 1, at: 77, t0: T}, 0, {v: 0}, T + 3000), null);
});

test("an unechoed local click gives up after a few seconds", () => {
  assert.equal(NN.nextPending({d: 1, at: "local", t0: T}, 0, {v: 0}, T + 6000), null);
});

test("an intent already met is never pending", () => {
  assert.equal(NN.nextPending(null, 1, {v: 1, desired: 1, desired_at: 5}, T), null);
});

test("pending wording ages from 'turning on' to 'not confirmed' to 'did not'", () => {
  assert.equal(NN.pendingText(1, 2, true), "turning on…");
  assert.equal(NN.pendingText(0, 2, true), "turning off…");
  assert.equal(NN.pendingText(1, 45, true), "on? not confirmed");
  assert.equal(NN.pendingText(1, 150, true), "did not turn on");
  assert.equal(NN.pendingText(3, 2, false, String), "→ 3…");
  assert.equal(NN.pendingText(3, 150, false, String), "→ 3 failed");
});
