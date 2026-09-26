/* nn hub — /devices webapp.
 *
 * A tiny hash-routed SPA over the hub's single-origin camera API:
 *   #/                 device grid (snapshot thumbnails + Live / Events)
 *   #/live/<cam>       adaptive live view (jmuxer, else HLS — user can't tell)
 *   #/events/<cam>     recorded events, newest first, with the captured clip
 *
 * All video/snapshot/event traffic is proxied by the hub, so the browser only
 * ever talks to this origin.
 */
(function () {
  "use strict";

  var app = document.getElementById("app");
  var backBtn = document.getElementById("back");
  var titleEl = document.getElementById("title");
  var infoEl = document.getElementById("hdr-info");

  var teardown = null;   // cleanup for the current view (sockets, timers)

  function cleanup() {
    if (teardown) { try { teardown(); } catch (e) {} teardown = null; }
    infoEl.textContent = "";
  }

  // The drift check below compares a frame's capture time against "now", so a
  // viewer whose own clock is wrong would report every camera as broken.
  // Every hub response carries a Date header — use it to measure the
  // browser's offset and take it back out of the comparison.  1 s resolution
  // plus one network hop of error, against a threshold of tens of seconds.
  var SRV_SKEW_MS = 0;
  function noteServerDate(r) {
    try {
      var d = r.headers && r.headers.get && r.headers.get("Date");
      if (!d) return;
      var t = Date.parse(d);
      if (!isNaN(t)) SRV_SKEW_MS = t - Date.now();
    } catch (e) {}
  }
  function serverNow() { return Date.now() + SRV_SKEW_MS; }

  function api(path) {
    return fetch(path).then(function (r) {
      noteServerDate(r);
      if (!r.ok) throw new Error(path + " → " + r.status);
      return r.json();
    });
  }

  // ── stream time-drift watchdog ──────────────────────────────────────────
  // The capture-time badge is fed by the playlist's PROGRAM-DATE-TIME (HLS)
  // or the device timestamp (jmuxer).  Either can run far behind wall clock
  // while the picture itself is fine: ffmpeg anchors the playlist date ONCE
  // and then advances it by media time, so every stall pushes it permanently
  // back, and on the transcoded path it accumulates.  A stale badge over a
  // live picture is indistinguishable from a stale picture, so say so out
  // loud instead of leaving the viewer to guess.
  var DRIFT_MAX_S    = 20;        // hub's limit wins once it answers
  var DRIFT_CONFIRM  = 3;         // consecutive bad samples before reporting
  var DRIFT_SAMPLE_MS = 5000;     // showTs fires ~4x/s; sample far slower
  var DRIFT_GAP_MS   = 600000;    // at most one report per camera / 10 min
  var driftSt = {};
  function noteFrameTime(camId, frameMs, player, onState) {
    // Below 1e12 the device is reporting UPTIME, not epoch — it has never
    // timesynced, so there is no wall-clock drift to measure (showTs already
    // renders that case honestly as "up H:MM:SS").
    if (!camId || !(frameMs > 1e12)) return;
    var st = driftSt[camId] || (driftSt[camId] = {bad: 0, at: 0, sent: 0});
    var now = Date.now();
    if (now - st.at < DRIFT_SAMPLE_MS) return;
    st.at = now;
    var drift = (serverNow() - frameMs) / 1000;
    if (Math.abs(drift) <= DRIFT_MAX_S) {
      st.bad = 0;
      if (onState) onState(0);
      return;
    }
    // One bad sample is a hiccup (a tab that just woke, a segment boundary).
    // Only a sustained offset is worth an operator's attention.
    st.bad++;
    if (onState) onState(drift);
    if (st.bad < DRIFT_CONFIRM || now - st.sent < DRIFT_GAP_MS) return;
    st.sent = now;
    try {
      fetch("/api/v1/cameras/" + encodeURIComponent(camId) + "/stream-report", {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          drift_s: Math.round(drift * 10) / 10,
          player: player,
          clock_skew_s: Math.round(SRV_SKEW_MS / 100) / 10,
          ua: navigator.userAgent.slice(0, 120)
        })
      }).then(function (r) {
        noteServerDate(r);
        return r.ok ? r.json() : null;
      }).then(function (j) {
        // Adopt the hub's threshold so the two observers agree on "broken".
        if (j && j.limit_s) DRIFT_MAX_S = j.limit_s;
      }).catch(function () {});
    } catch (e) {}
  }

  // A WebSocket handshake carries no saved HTTP credentials, so behind basic
  // auth the browser would pop a login box for every stream.  Ask for a
  // short-lived single-use ticket over ordinary HTTP instead — that request
  // DOES authenticate — and spend it on the socket URL.
  // Tickets are reusable until they expire (server TTL) or the hub
  // restarts, so mint once and share across sockets; re-mint shortly
  // before expiry.  A hub restart invalidates the cache server-side —
  // the socket then closes, and the retry path mints a fresh one.
  var _wsTicket = null, _wsTicketExp = 0;
  function wsUrl(path) {
    var proto = location.protocol === "https:" ? "wss://" : "ws://";
    function withTicket(t) {
      var sep = path.indexOf("?") === -1 ? "?" : "&";
      return proto + location.host + path +
             (t ? sep + "ticket=" + encodeURIComponent(t) : "");
    }
    if (_wsTicket && Date.now() < _wsTicketExp - 5000) {
      return Promise.resolve(withTicket(_wsTicket));
    }
    return fetch("/api/v1/ws-ticket", {cache: "no-store"})
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        if (j && j.ticket) {
          _wsTicket = j.ticket;
          _wsTicketExp = Date.now() + (j.ttl || 60) * 1000;
        }
        return withTicket(j && j.ticket);
      });
  }

  var esc = NN.esc;                    // pure helpers live in applib.js
  function fmtBytes(b) {
    return b > 1048576 ? (b / 1048576).toFixed(1) + " MB" : (b / 1024).toFixed(0) + " KB";
  }

  function setHeader(title, showBack) {
    titleEl.textContent = title;
    backBtn.hidden = !showBack;
  }

  // Device-scoped pages title themselves with JUST the device name — the
  // active tab already says what you're looking at — and the name is a
  // dropdown: pick another device and you land on the SAME tab for it.
  // Lists are fetched per render; they're tiny and always current.
  function setDeviceHeader(kind, currentId, hashPrefix) {
    backBtn.hidden = false;
    var sel = document.createElement("select");
    sel.className = "devswitch";
    var cur = document.createElement("option");
    cur.value = currentId; cur.textContent = currentId;
    sel.appendChild(cur);
    titleEl.textContent = "";
    titleEl.appendChild(sel);
    var url = kind === "cam" ? "/api/v1/cameras" : "/api/v1/devices";
    api(url).then(function (list) {
      sel.innerHTML = "";
      (list || []).forEach(function (d) {
        var o = document.createElement("option");
        o.value = kind === "cam" ? d.id : (d.name || d.id);
        o.textContent = d.name || d.id;
        if (o.value === currentId) o.selected = true;
        sel.appendChild(o);
      });
      // current device gone from the list (rare): keep it visible
      if (sel.value !== currentId) {
        var o = document.createElement("option");
        o.value = currentId; o.textContent = currentId; o.selected = true;
        sel.appendChild(o);
      }
    }).catch(function () {});
    sel.onchange = function () {
      if (sel.value !== currentId)
        location.hash = hashPrefix + encodeURIComponent(sel.value);
    };
  }

  // ── device settings tabs ──────────────────────────────────────────────────
  // The tab set differs by device type, because the devices differ: a camera
  // has detection rules and a streaming strategy to configure, a sensor has
  // its card controls.  Each tab is its OWN ROUTE rather than in-page state,
  // so it survives a refresh, can be linked to, and the back button works.
  var TABS = {
    cam: [["events",    "Events",         "#/events/"],
          ["detection", "Detection Rule", "#/infer/"],
          ["streaming", "Streaming",      "#/stream/"],
          ["pipelines", "Pipelines",      "#/pipes/"],
          ["status",    "Device Status",  "#/camstatus/"]],
    dev: [["events",  "Events",        "#/sevents/"],
          ["control", "Control",       "#/ssettings/"],
          ["status",  "Device Status", "#/devstatus/"]]
  };
  function tabsHtml(kind, id, active) {
    return '<nav class="dtabs">' + TABS[kind].map(function (t) {
      return '<a class="dtab' + (t[0] === active ? " on" : "") + '" href="' +
             t[2] + encodeURIComponent(id) + '">' + esc(t[1]) + '</a>';
    }).join("") + '</nav>';
  }
  // Views replace app.innerHTML asynchronously (loading → content), so the
  // strip is mounted after the final render rather than baked into each
  // template; the guard keeps a re-render from stacking two strips.
  function mountTabs(kind, id, active) {
    if (app.querySelector(":scope > .dtabs")) return;
    app.insertAdjacentHTML("afterbegin", tabsHtml(kind, id, active));
  }

  // ── router ────────────────────────────────────────────────────────────────
  function router() {
    cleanup();
    var h = location.hash.replace(/^#/, "") || "/";
    var parts = h.split("/").filter(Boolean);      // e.g. ["live","cam0"]
    if (parts[0] === "live" && parts[1]) return viewLive(decodeURIComponent(parts[1]));
    if (parts[0] === "events" && parts[1]) return viewEvents(decodeURIComponent(parts[1]));
    if (parts[0] === "settings") return viewSettings(parts[1]);
    if (parts[0] === "logging") return viewLogging(parts[1]);
    if (parts[0] === "automation") return viewAutomation(parts[1]);
    if (parts[0] === "infer" && parts[1]) return viewInfer(decodeURIComponent(parts[1]));
    if (parts[0] === "stream" && parts[1]) return viewCamStreaming(decodeURIComponent(parts[1]));
    if (parts[0] === "camstatus" && parts[1]) return viewCamStatus(decodeURIComponent(parts[1]));
    if (parts[0] === "pipes" && parts[1]) return viewCamPipelines(decodeURIComponent(parts[1]));
    if (parts[0] === "sevents" && parts[1]) return viewSensorEvents(decodeURIComponent(parts[1]));
    if (parts[0] === "ssettings" && parts[1]) return viewSensorSettings(decodeURIComponent(parts[1]));
    if (parts[0] === "devstatus" && parts[1]) return viewSensorStatus(decodeURIComponent(parts[1]));
    if (parts[0] === "archived" && parts[1]) return viewArchivedDevice(decodeURIComponent(parts[1]));
    if (parts[0] === "add") return viewAddDevice();
    if (parts[0] === "gateway") return viewGateway();
    if (parts[0] === "factory") return viewFactory(parts[1]);
    return viewGrid();
  }

  // sidebar: highlight the active section, and act as a drawer when narrow
  var sidebar = document.getElementById("sidebar");
  var scrim = document.getElementById("nav-scrim");
  var navToggle = document.getElementById("nav-toggle");
  function closeNav() { if (sidebar) sidebar.classList.remove("open");
                        if (scrim) scrim.hidden = true; }
  if (navToggle) navToggle.onclick = function () {
    var open = sidebar.classList.toggle("open");
    scrim.hidden = !open;
  };
  if (scrim) scrim.onclick = closeNav;
  function markNav() {
    var sec = NN.routeSection(location.hash);
    [].forEach.call(document.querySelectorAll(".navitem"), function (a) {
      a.classList.toggle("active", a.getAttribute("data-nav") === sec);
    });
    closeNav();
  }
  window.addEventListener("hashchange", markNav);
  markNav();
  window.addEventListener("hashchange", router);
  backBtn.onclick = function () { location.hash = "#/"; };

  function findCam(cams, id) {
    var c = (cams || []).filter(function (x) { return x.id === id; })[0];
    return c || {
      id: id, name: id,
      snapshot_url: "/api/v1/cameras/" + id + "/snapshot.jpg",
      live_ws: "/api/v1/cameras/" + id + "/ws",
      hls_url: "/api/v1/cameras/" + id + "/hls/live.m3u8",
      detections: "/api/v1/cameras/" + id + "/detections",
      events_url: "/api/v1/cameras/" + id + "/events"
    };
  }

  // ── device grid ───────────────────────────────────────────────────────────
  function viewGrid() {
    setHeader("Devices", false);
    app.innerHTML = '<div class="empty">loading…</div>';
    Promise.all([
      api("/api/v1/cameras").catch(function () { return []; }),
      api("/api/v1/devices").catch(function () { return []; })
    ]).then(function (res) {
      var cams = res[0] || [], devs = res[1] || [];
      if (!cams.length && !devs.length) {
        app.innerHTML = '<div class="empty">no devices configured</div>';
        return;
      }
      var n = cams.length + devs.length;
      infoEl.textContent = n + (n === 1 ? " device" : " devices");
      app.innerHTML = "";

      var grid = null;
      if (cams.length) {
        if (devs.length) app.appendChild(sectionTitle("Cameras"));
        grid = document.createElement("div");
        grid.className = "grid";
        cams.forEach(function (c) { grid.appendChild(cardFor(c)); });
        app.appendChild(grid);
      }

      var cardTimers = [];
      if (devs.length) {
        if (cams.length) app.appendChild(sectionTitle("Sensors"));
        var sgrid = document.createElement("div");
        sgrid.className = "grid sensors";
        devs.forEach(function (d) {
          sgrid.appendChild(sensorCardFor(d, function (t) { cardTimers.push(t); }));
        });
        app.appendChild(sgrid);
      }

      // ── bottom of the device list: add-device, then the archive row ──
      var addRow = document.createElement("div");
      addRow.className = "grid-bottom";
      addRow.innerHTML =
        '<button class="btn addnew" onclick="location.hash=\'#/add\'">' +
        '+ Add new device</button>';
      app.appendChild(addRow);

      var archRow = document.createElement("div");
      archRow.className = "archive-row";
      archRow.innerHTML = '<span class="chev">▸</span> Archive';
      var archBody = document.createElement("div");
      archBody.className = "archive-body";
      archBody.hidden = true;
      app.appendChild(archRow);
      app.appendChild(archBody);
      var archLoaded = false;
      archRow.onclick = function () {
        archBody.hidden = !archBody.hidden;
        archRow.querySelector(".chev").textContent = archBody.hidden ? "▸" : "▾";
        if (archLoaded || archBody.hidden) return;
        archLoaded = true;
        archBody.innerHTML = '<div class="empty">loading…</div>';
        api("/api/v1/archive").then(function (list) {
          if (!list.length) {
            archBody.innerHTML = '<div class="empty">archive is empty</div>';
            return;
          }
          archBody.innerHTML = "";
          list.forEach(function (a) {
            var el = document.createElement("div");
            el.className = "archived-card";
            el.innerHTML =
              '<div class="meta"><b>' + esc(a.name) + '</b>' +
              '<small>' + esc(a.device_type || "?") + ' · archived ' +
              new Date(a.archived_at * 1000).toLocaleDateString() +
              '</small></div>' +
              '<button class="btn events">Settings</button>';
            el.querySelector("button").onclick = function () {
              location.hash = "#/archived/" + encodeURIComponent(a.device_id);
            };
            archBody.appendChild(el);
          });
        }).catch(function () {
          archBody.innerHTML = '<div class="empty err">archive unavailable</div>';
        });
      };

      function bust() {
        if (!grid) return;
        grid.querySelectorAll("img[data-src]").forEach(function (img) {
          img.src = img.getAttribute("data-src") + "?t=" + Date.now();
        });
      }
      // Re-poll the camera list to keep the online/offline pill + thumbnails
      // fresh, updating cards in place (no rebuild → no flicker).
      function refresh() {
        if (!grid) return;
        api("/api/v1/cameras").then(function (cs) {
          cs.forEach(function (c) {
            var card = grid.querySelector('.card[data-cam="' + c.id + '"]');
            if (card) setStatus(card, c.online, c.streaming, c);
          });
          bust();
        }).catch(function () {});
      }
      bust();
      var timer = setInterval(refresh, 6000);
      teardown = function () {
        clearInterval(timer);
        cardTimers.forEach(clearInterval);
      };
    }).catch(function (e) {
      app.innerHTML = '<div class="empty err">failed to load devices: ' + esc(e.message) + "</div>";
    });
  }

  function sectionTitle(t) {
    var h = document.createElement("div");
    h.className = "section-title";
    h.textContent = t;
    return h;
  }

  // ── sensor cards ──────────────────────────────────────────────────────────
  // One card per provisioned sensor.  Header = name only.  One row per
  // field descriptor from the device's config sync (INFO_REPLY `fields`):
  //   {n: name, t: 0 read-only | 1 writable, min, max}
  // Widget by shape: 0..1 → toggle; small integer range → dropdown;
  // anything else → slider.  Read-only fields render the same widget,
  // disabled, so the card reads uniformly.

  // fetch with a client-side deadline: a field op against a lossy sensor
  // can hold the HTTP request ~50 s server-side (relay retries) — without
  // an abort the card's sequential value loader stalls behind it.
  function fetchT(url, opts, ms) {
    var ctl = ("AbortController" in window) ? new AbortController() : null;
    var timer = ctl && setTimeout(function () { ctl.abort(); }, ms || 12000);
    opts = opts || {};
    if (ctl) opts.signal = ctl.signal;
    return fetch(url, opts).then(function (r) {
      if (timer) clearTimeout(timer);
      return r;
    }, function (e) {
      if (timer) clearTimeout(timer);
      throw e;
    });
  }

  // Fill a device's rows: what the hub already knows (its field cache,
  // fed by the device's own pushes) shows at once, and only the rest is
  // read live from the device -- PER_DEVICE_READS at a time, not one
  // after another.  A refresh used to be a full sequential mesh read per
  // visible field, which is where the seconds of "…" and the "—" came from.
  function loadRowValues(dev, rows) {
    api("/api/v1/devices/" + encodeURIComponent(dev) + "/fields/cache")
      .catch(function () { return {}; })
      .then(function (j) {
        var cache = (j && j.fields) || {};
        var pending = [];
        rows.forEach(function (row) {
          var e = cacheEntry(cache, row);
          if (e) row.applyLive(e);              // value and/or pending intent
          if (!e || typeof e.v !== "number") pending.push(row);
        });
        var i = 0;
        function worker() {
          if (i >= pending.length) return;
          var row = pending[i++];
          getField(dev, row.field)
            .then(function (j) { row.setValue(j.value); })
            .catch(function () { row.setError(); })
            .then(worker);
        }
        for (var k = 0; k < PER_DEVICE_READS; k++) worker();
      });
  }

  function putField(dev, field, value) {
    return fetchT("/api/v1/devices/" + encodeURIComponent(dev) +
                  "/field/" + encodeURIComponent(field), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value: value })
    }, 15000).then(function (r) {
      if (!r.ok) {
        var e = new Error("HTTP " + r.status);
        e.status = r.status;          // 400 = the device refused the value
        throw e;
      }
      return r.json();
    });
  }

  // A live field read is a hub -> gateway -> device round trip with the
  // hub's own retry schedule behind it (5 attempts, 1+2+4+8 s backoff);
  // giving up at 12 s meant the page showed "—" while the hub was still
  // about to succeed.  Wait for the hub's verdict instead.
  var FIELD_READ_MS = 40000;
  // Reads to ONE device in flight at once.  The device serves field ops
  // from a 4-deep queue and answers through 6 reliable-send slots that its
  // own automation notifies also need; two parallel reads keep a card
  // fast without starving those.  Cards for different devices are
  // independent and always load in parallel.
  var PER_DEVICE_READS = 2;
  // Pending-write rules and wording live in applib (NN.nextPending /
  // NN.pendingText, unit-tested): measured cascades take 11-26 s and
  // sometimes over a minute before the hub failsafe lands them, so "not
  // confirmed" is not "failed".  The hub drops an unconfirmed intent at 180 s.

  function cacheEntry(cache, row) {
    // twin rows (`X_v`) mirror the base field's cache entry
    var key = /_v$/.test(row.field) ? row.field.slice(0, -2) : row.field;
    return cache[key];
  }

  // Poll a device's field cache: every 4 s normally, every 1 s while any
  // row is waiting for a confirmation (it is hub-local, no mesh traffic).
  function watchFieldCache(dev, rows) {
    var last = 0;
    return setInterval(function () {
      var busy = rows.some(function (r) { return r.isPending(); });
      rows.forEach(function (r) { if (r.isPending()) r.tick(); });
      if (!busy && Date.now() - last < 4000) return;
      last = Date.now();
      api("/api/v1/devices/" + encodeURIComponent(dev) + "/fields/cache")
        .then(function (j) {
          var cache = (j && j.fields) || {};
          rows.forEach(function (row) { row.applyLive(cacheEntry(cache, row)); });
        }).catch(function () {});
    }, 1000);
  }

  function getField(dev, field) {
    return fetchT("/api/v1/devices/" + encodeURIComponent(dev) +
                  "/field/" + encodeURIComponent(field), {}, FIELD_READ_MS)
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      });
  }

  function sensorCardFor(d, regTimer) {
    var card = document.createElement("div");
    card.className = "card sensor-card";
    card.setAttribute("data-dev", d.name);
    var head = document.createElement("div");
    head.className = "namerow";
    head.innerHTML = '<span class="name">' + esc(d.name) + "</span>";
    card.appendChild(head);

    var body = document.createElement("div");
    body.className = "sensor-rows";
    card.appendChild(body);

    var caps = d.capabilities;
    if (typeof caps === "string") {
      try { caps = JSON.parse(caps); } catch (e) { caps = []; }
    }
    // Per-device card config decides which field controls appear on
    // the card face; the FULL list always lives in the Events view.
    var hiddenSet = {};
    var cfgReady = api("/api/v1/devices/" + encodeURIComponent(d.name) +
                       "/card-config")
      .then(function (c) {
        (c.hidden || []).forEach(function (n) { hiddenSet[n] = true; });
      }).catch(function () {});

    if (!caps || !caps.length) {
      // No config sync stored yet — offer to pull one from the device.
      var empty = document.createElement("div");
      empty.className = "sensor-empty";
      empty.innerHTML = '<span>no config sync yet</span>';
      var btn = document.createElement("button");
      btn.className = "btn";
      btn.textContent = "Sync";
      btn.onclick = function () {
        btn.disabled = true; btn.textContent = "syncing…";
        fetch("/api/v1/devices/" + encodeURIComponent(d.name) +
              "/info/refresh", { method: "POST" })
          .then(function (r) { return r.json(); })
          .then(function (doc) {
            if (doc && doc.fields && doc.fields.length) {
              body.innerHTML = "";
              renderRows(doc.fields);
            } else {
              btn.disabled = false; btn.textContent = "Sync";
            }
          })
          .catch(function () { btn.disabled = false; btn.textContent = "Sync"; });
      };
      empty.appendChild(btn);
      body.appendChild(empty);
    } else {
      renderRows(caps);
    }

    function renderRows(fields) {
      cfgReady.then(function () {
        var rows = [];
        var shown = fields.filter(function (f) { return !hiddenSet[f.n]; });
        shown.forEach(function (f) {
          var row = sensorRow(d.name, f);
          rows.push(row);
          body.appendChild(row.el);
        });
        if (!shown.length) {
          var none = document.createElement("div");
          none.className = "sensor-empty";
          none.innerHTML = "<span>all controls hidden — see Settings</span>";
          body.appendChild(none);
        }
        loadValues(rows);
        watchLive(rows);
      });
    }

    // Track automation-driven changes: the device pushes an AUTO_EVENT
    // whenever an actuator changes (any source — hub write, local rule,
    // D2D cascade), and the hub caches it.  Polling that cache is
    // hub-local (zero mesh traffic), so a short interval is fine.
    function watchLive(rows) {
      var t = watchFieldCache(d.name, rows);
      if (regTimer) regTimer(t); else clearInterval(t);
    }

    // Cache first, then bounded-parallel live reads (loadRowValues).
    // Interactivity never depends on these completing: writable widgets
    // are live from the moment the card renders.
    function loadValues(rows) { loadRowValues(d.name, rows); }

    var actions = document.createElement("div");
    actions.className = "actions";
    // same classes as the camera card buttons so the styling matches
    actions.innerHTML =
      '<button class="btn events sev">Events</button>' +
      '<button class="btn infer sset">Settings</button>';
    actions.querySelector(".btn.sev").onclick = function () {
      location.hash = "#/sevents/" + encodeURIComponent(d.name);
    };
    actions.querySelector(".btn.sset").onclick = function () {
      location.hash = "#/ssettings/" + encodeURIComponent(d.name);
    };
    card.appendChild(actions);

    return card;
  }

  // ── sensor Events: EVERY advertised input/output, live ─────────────────
  // Device Status tab (sensor): firmware version, downloaded version and the
  // scheduled OTA — the same panel the camera status tab shows.
  function viewSensorStatus(dev) {
    setDeviceHeader("dev", dev, "#/devstatus/");
    app.innerHTML = '<div class="empty">loading…</div>';
    api("/api/v1/devices/" + encodeURIComponent(dev)).then(function (d) {
      app.innerHTML = "";
      mountTabs("dev", dev, "status");
      renderUptime(app, function () {
        // Passive first: the 30 s heartbeat already carries uptime, so
        // showing it costs the mesh nothing.  Fall back to one live
        // INFO_QUERY only when the hub has not heard a heartbeat yet.
        return fetchT("/api/v1/devices/" + encodeURIComponent(dev) +
                      "/uptime", null, 8000)
          .then(function (r) { return r.json().then(function (j) {
            if (r.ok) return "up " + fmtUp(j.up_s) +
              "  (heartbeat, " + Math.round(j.age_s) + "s ago)";
            return fetchT("/api/v1/devices/" + encodeURIComponent(dev) +
                          "/info/refresh", {method: "POST"}, 15000)
              .then(function (r2) { return r2.json().then(function (i) {
                if (!r2.ok) throw new Error(i.error || "device did not answer");
                return "up " + fmtUp((i.uptime_ms || 0) / 1000) +
                       "  (live from device, fw " + (i.firmware || "?") + ")";
              }); });
          }); });
      });
      renderDeviceReboot(app, dev);
      renderFirmwarePanel(app, dev, d);
      renderUnregister(app, dev, "dev");
    }).catch(function () {
      app.innerHTML = '<div class="empty">unavailable</div>';
      mountTabs("dev", dev, "status");
    });
  }

  // ── device uptime ───────────────────────────────────────────────────────
  // Both device kinds already tell us this — the point is only to SHOW it.
  // Cameras: read off traffic the device already sends (heartbeat / status
  // record), no device round trip.  Sensors: a live INFO_QUERY — one small
  // mesh round trip, acceptable for an operator opening a status page.
  function fmtUp(s) {
    s = Math.max(0, Math.floor(s));
    var d = Math.floor(s / 86400), h = Math.floor(s % 86400 / 3600),
        m = Math.floor(s % 3600 / 60);
    return (d ? d + "d " : "") + h + "h " + String(m).padStart(2, "0") + "m";
  }
  function renderUptime(root, loader) {
    var box = document.createElement("div");
    box.innerHTML =
      '<div class="setting"><div class="meta"><h3>Uptime</h3>' +
      '<p class="up-line">reading…</p></div></div>';
    root.appendChild(box);
    var line = box.querySelector(".up-line");
    loader().then(function (t) { line.textContent = t; })
            .catch(function (e) { line.textContent = String(e && e.message || e); });
  }


  // ── gateway: service health + recovery ──────────────────────────────────
  // The mesh's single point of failure, and the one thing that needs
  // hands-on recovery after its NCP is replugged (the running process
  // keeps the old tty and wedges).  Surfacing it here means that fix no
  // longer requires SSH.
  // ── Factory: firmware sources + catalog ─────────────────────────────────
  // ── Factory: tabbed — Catalog · Flash device · Sources · Fleet ──────────
  var FACTORY_TABS = [["catalog", "Catalog"], ["flash", "Flash device"],
                      ["sources", "Sources"], ["fleet", "Fleet"]];

  function viewFactory(tab) {
    tab = tab || "catalog";
    setHeader("Factory", false);
    app.innerHTML = '<nav class="dtabs">' + FACTORY_TABS.map(function (t) {
      return '<a class="dtab' + (t[0] === tab ? " on" : "") +
             '" href="#/factory/' + t[0] + '">' + t[1] + "</a>";
    }).join("") + '</nav><div id="factory-host"><div class="empty">loading…</div></div>';
    var host = document.getElementById("factory-host");
    if (tab === "flash") factoryFlash(host);
    else if (tab === "sources") factorySources(host);
    else if (tab === "fleet") factoryFleet(host);
    else factoryCatalog(host);
  }

  function factoryCatalog(host) {
    api("/api/v1/firmware/catalog").then(function (cat) {
      var byimg = {};
      (cat || []).forEach(function (e) {
        (byimg[e.device_type] = byimg[e.device_type] || []).push(e);
      });
      var keys = Object.keys(byimg).sort();
      host.innerHTML = '<div class="setting"><div class="meta">' +
        '<h3>Catalog</h3><p>Every firmware the hub can serve, keyed by the ' +
        'name devices report.  The ✓ version is the active OTA target.</p>' +
        '</div></div>' +
        (keys.map(function (k) {
          var vs = byimg[k].sort(function (a, b) {
            return (b.version || "").localeCompare(a.version || "", undefined, {numeric: true});
          });
          return '<div class="setting"><div class="meta"><h3>' + esc(k) +
            ' <small>' + vs.length + " version" + (vs.length === 1 ? "" : "s") + '</small></h3>' +
            '<div class="fx-vers">' + vs.map(function (v) {
              return '<div class="fx-ver' + (v.is_active ? " act" : "") + '">' +
                     (v.is_active ? "✓ " : "") + esc(v.version) +
                     ' <small>' + esc(v.source_name || "") + '</small></div>';
            }).join("") + '</div></div></div>';
        }).join("") || '<div class="empty">catalog is empty — add a source</div>');
    }).catch(function (e) {
      host.innerHTML = '<div class="empty err">' + esc(e.message || e) + "</div>";
    });
  }

  function factorySources(host) {
    api("/api/v1/firmware/sources").then(function (srcs) {
      var rows = (srcs || []).map(function (sx) {
        var what = sx.kind === "GitHubReleasesSource"
          ? "GitHub" : sx.kind === "LocalDirSource" ? "local" : esc(sx.kind);
        return '<div class="setting"><div class="meta">' +
          '<h3>' + esc(sx.name) + ' <small>' + what + '</small></h3>' +
          '<p>' + (sx.last_error
                    ? 'last sync failed: ' + esc(sx.last_error)
                    : sx.last_sync_at
                      ? 'last synced ' + new Date(sx.last_sync_at * 1000).toLocaleString()
                      : 'never synced') +
          (sx.poll_seconds ? ' · polls every ' + sx.poll_seconds + 's' : ' · manual sync only') +
          '</p></div><div class="switch">' +
          '<button class="btn events fx-sync" data-src="' + esc(sx.name) + '">Sync</button>' +
          '<button class="btn danger fx-del" data-src="' + esc(sx.name) + '">Remove</button>' +
          '</div></div>';
      }).join("");
      host.innerHTML =
        '<div class="setting"><div class="meta"><h3>Firmware sources</h3>' +
        '<p>Where the hub discovers firmware.  A GitHub source watches a ' +
        'repo\'s releases: each release carries image.signed.bin + ' +
        'manifest.json (+ flash.sh to be flashable), and the manifest\'s ' +
        'device_type is the firmware name devices report.</p></div></div>' +
        (rows || '<div class="empty">no sources configured</div>') +
        '<div class="setting"><div class="meta"><h3>Add a GitHub catalog</h3>' +
        '<p>Repo as owner/name.  For a private repo, set a token in the ' +
        'hub\'s environment and give its variable name here — the token ' +
        'itself is never stored in config.</p></div></div>' +
        '<div class="setting fx-form"><div class="meta">' +
        '<input id="fx-name" placeholder="source name (e.g. my-github)">' +
        '<input id="fx-repo" placeholder="owner/repo">' +
        '<input id="fx-pat" placeholder="token env var (optional, e.g. NN_HUB_GH_PAT)">' +
        '</div><div class="switch">' +
        '<button class="btn live" id="fx-add">Add</button></div></div>' +
        '<div class="logbox" id="fx-msg" style="display:none"></div>';

      var msg = host.querySelector("#fx-msg");
      function say(t, isErr) {
        msg.style.display = "";
        msg.textContent = t;
        msg.classList.toggle("err", !!isErr);
      }
      [].forEach.call(host.querySelectorAll(".fx-sync"), function (b) {
        b.onclick = function () {
          b.disabled = true; b.textContent = "Syncing…";
          fetchT("/api/v1/firmware/sources/" + encodeURIComponent(b.dataset.src) + "/sync",
                 {method: "POST"}, 60000)
            .then(function (r) { return r.json(); })
            .then(function () { factorySources(host); })
            .catch(function (e) { say("sync failed: " + (e && e.message || e), true);
                                  b.disabled = false; b.textContent = "Sync"; });
        };
      });
      [].forEach.call(host.querySelectorAll(".fx-del"), function (b) {
        b.onclick = function () {
          if (!confirm("Remove source '" + b.dataset.src + "'?  Already-" +
                       "discovered versions stay in the catalog.")) return;
          fetchT("/api/v1/firmware/sources/" + encodeURIComponent(b.dataset.src),
                 {method: "DELETE"}, 30000)
            .then(function () { factorySources(host); })
            .catch(function (e) { say("remove failed: " + (e && e.message || e), true); });
        };
      });
      host.querySelector("#fx-add").onclick = function () {
        var body = {
          name: host.querySelector("#fx-name").value.trim(),
          kind: "github",
          repo: host.querySelector("#fx-repo").value.trim(),
          poll_seconds: 300
        };
        var pat = host.querySelector("#fx-pat").value.trim();
        if (pat) body.pat_env = pat;
        if (!body.name || !body.repo) { say("name and owner/repo are required", true); return; }
        var b = this; b.disabled = true; b.textContent = "Adding…";
        fetchT("/api/v1/firmware/sources",
               {method: "POST", headers: {"Content-Type": "application/json"},
                body: JSON.stringify(body)}, 60000)
          .then(function (r) { return r.json().then(function (j) { return {ok: r.ok, j: j}; }); })
          .then(function (x) {
            if (!x.ok) { say(x.j.error || "add failed", true);
                         b.disabled = false; b.textContent = "Add"; return; }
            factorySources(host);
          })
          .catch(function (e) { say("add failed: " + (e && e.message || e), true);
                                b.disabled = false; b.textContent = "Add"; });
      };
    }).catch(function (e) {
      host.innerHTML = '<div class="empty err">' + esc(e.message || e) + "</div>";
    });
  }

  // Two targets share this tab.  Serial: the firmware's own flash.sh over a
  // USB-serial port (ESP boards).  Card: a whole-disk image (format
  // sdcard.img.xz, e.g. byai_sdcard) written to a removable micro-SD in a
  // reader plugged into the hub — the card then goes into the BeagleY.
  function factoryFlash(host) {
    api("/api/v1/firmware/catalog").then(function (cat) {
      host.innerHTML =
        '<div class="setting"><div class="meta"><h3>Flash a device</h3>' +
        '<p>Plug a board into the hub over USB, pick it and a firmware ' +
        'from the catalog, and the hub runs that firmware\'s own flash ' +
        'script.  After flashing, the device boots into setup mode — add ' +
        'it with Add&nbsp;new&nbsp;device.  Ports the gateway uses are ' +
        'locked out.  For a <b>micro-SD camera image</b>, put the card in ' +
        'a USB reader on the hub: it is erased and rewritten, then read ' +
        'back to verify.</p></div></div>' +
        '<div class="setting fx-form"><div class="meta">' +
        '<select id="fl-fw"><option value="">choose firmware…</option></select>' +
        '<select id="fl-port"><option value="">no ports — plug a device ' +
        'into the hub</option></select>' +
        '<select id="fl-disk" style="display:none"><option value="">no card ' +
        'reader — plug one into the hub</option></select>' +
        '<input id="fl-confirm" style="display:none" placeholder="type ERASE to confirm">' +
        '</div><div class="switch">' +
        '<button class="btn" id="fl-rescan">Rescan</button>' +
        '<button class="btn danger" id="fl-go">Flash</button></div></div>' +
        '<div class="setting" id="fl-prog" style="display:none"><div class="meta">' +
        '<progress id="fl-bar" max="1000" value="0" style="width:100%"></progress>' +
        '<p id="fl-prog-txt"></p></div></div>' +
        '<div class="logbox" id="fl-log" style="display:none"></div>';

      var flPort = host.querySelector("#fl-port");
      var flDisk = host.querySelector("#fl-disk");
      var flConf = host.querySelector("#fl-confirm");
      var flFw   = host.querySelector("#fl-fw");
      var flLog  = host.querySelector("#fl-log");
      var flGo   = host.querySelector("#fl-go");
      var flProg = host.querySelector("#fl-prog");
      var byKey  = {};
      (cat || []).forEach(function (e) { byKey[e.device_type + "@" + e.version] = e; });
      function target() {
        var e = byKey[flFw.value];
        return e && e.flash_target === "sdcard" ? "sdcard" : "serial";
      }
      function showTarget() {
        var sd = target() === "sdcard";
        flPort.style.display = sd ? "none" : "";
        flDisk.style.display = sd ? "" : "none";
        flConf.style.display = sd ? "" : "none";
        flGo.textContent = sd ? "Erase card & write" : "Flash";
        if (sd) loadDisks();
      }
      // Auto-detect: while this tab is open, re-list the active target every
      // few seconds and re-render ONLY when the set changed (so a plugged
      // reader/board appears by itself, and the user's selection survives).
      var lastSig = {disks: null, ports: null};
      function loadDisks() {
        api("/api/v1/factory/disks").then(function (j) {
          var ds = (j && j.disks) || [];
          var sig = JSON.stringify(ds.map(function (d) { return [d.path, d.size_bytes, d.serial]; }))
                    + (j && j.helper_installed) + (j && j.trusted_origin);
          if (sig === lastSig.disks) return;
          lastSig.disks = sig;
          var keep = flDisk.value;
          var warn = "";
          if (j && !j.helper_installed) warn = "hub is missing nn-sdflash (run deploy/install-sdflash.sh)";
          else if (j && !j.trusted_origin) warn = "open the hub through its authenticated address to write cards";
          flDisk.innerHTML = ds.length
            ? ds.map(function (d) {
                var label = (d.model || d.vendor || d.name) + " · " +
                  (d.has_media ? fmtBytes(d.size_bytes) : "no card") +
                  (d.serial ? " · " + d.serial : "") + " · " + d.path;
                return '<option value="' + esc(d.path) + '"' + (d.has_media ? "" : " disabled") +
                       '>' + esc(label) + '</option>';
              }).join("")
            : '<option value="">no card reader — plug one into the hub</option>';
          if (keep && ds.some(function (d) { return d.path === keep && d.has_media; })) flDisk.value = keep;
          else { var first = ds.filter(function (d) { return d.has_media; })[0]; if (first) flDisk.value = first.path; }
          if (warn) { flLog.style.display = ""; flLog.textContent = warn; }
        }).catch(function () {});
      }
      function loadPorts() {
        api("/api/v1/factory/ports").then(function (j) {
          var ps = (j && j.ports) || [];
          var sig = JSON.stringify(ps.map(function (p) { return [p.path, p.busy]; }));
          if (sig === lastSig.ports) return;
          lastSig.ports = sig;
          var keepP = flPort.value;
          var anyFree = ps.some(function (px) { return !px.busy; });
          flPort.innerHTML = ps.length
            ? ((anyFree ? "" :
                '<option value="" selected>all ports are locked — plug a board in</option>') +
               ps.map(function (px) {
                return '<option value="' + esc(px.path) + '"' +
                       (px.busy ? " disabled" : "") + '>' + esc(px.id) +
                       (px.busy ? " — " + esc(px.holder || "busy") : "") +
                       '</option>';
              }).join(""))
            : '<option value="">no ports — plug a device into the hub</option>';
          var free = ps.filter(function (px) { return !px.busy; });
          if (keepP && free.some(function (px) { return px.path === keepP; })) flPort.value = keepP;
          else if (free.length) flPort.value = free[0].path;
        }).catch(function () {});
      }
      flFw.innerHTML = '<option value="">choose firmware…</option>' +
        (cat || []).filter(function (e) { return e.flashable; })
           .map(function (e) {
          return '<option value="' + esc(e.device_type) + "@" + esc(e.version) +
                 '">' + esc(e.device_type) + " · " + esc(e.version) + '</option>';
        }).join("");
      loadPorts();
      flFw.onchange = showTarget;
      host.querySelector("#fl-rescan").onclick = function () {
        if (target() === "sdcard") loadDisks(); else loadPorts();
      };
      function reset() { flGo.disabled = false; showTarget(); }
      function pollJob(id, sd) {
        (function poll() {
          api("/api/v1/factory/flash/" + id).then(function (jb) {
            flLog.textContent = jb.log || "…";
            flLog.scrollTop = flLog.scrollHeight;
            if (sd && jb.progress) {
              var p = jb.progress, tot = p.total || 0;
              flProg.style.display = "";
              host.querySelector("#fl-bar").value = tot ? Math.round(1000 * p.bytes / tot) : 0;
              host.querySelector("#fl-prog-txt").textContent =
                (p.phase || "") + (tot ? " — " + fmtBytes(p.bytes) + " of " + fmtBytes(tot) +
                " (" + Math.round(100 * p.bytes / tot) + "%)" : "") +
                (p.phase === "verify" ? " — reading the card back" : "");
            }
            if (jb.state === "running") { setTimeout(poll, sd ? 2000 : 1500); return; }
            reset();
            if (jb.state === "done") {
              flLog.textContent += sd
                ? "\n\n✓ card written" + (jb.result && jb.result.verified ? " and verified" : "") +
                  ".  Remove it from the reader, put it in the camera and power it on — " +
                  "it boots into setup mode; then go to Add new device."
                : "\n\n✓ flashed.  The device is booting into setup mode — go to " +
                  "Add new device to provision it.";
            } else {
              var err = jb.result && jb.result.error ? "\n" + jb.result.error : "";
              flLog.textContent += "\n\n✗ " + (sd ? "card write" : "flash") +
                " failed (rc=" + jb.rc + ")" + err;
            }
          }).catch(function () { setTimeout(poll, 3000); });
        })();
      }
      flGo.onclick = function () {
        var fw = flFw.value, sd = target() === "sdcard";
        var dv = fw.split("@");
        if (!fw) { flLog.style.display = ""; flLog.textContent = "pick a firmware"; return; }
        var url, body;
        if (sd) {
          if (!flDisk.value) { flLog.style.display = ""; flLog.textContent = "pick a card"; return; }
          if (flConf.value.trim() !== "ERASE") {
            flLog.style.display = "";
            flLog.textContent = "this erases everything on " + flDisk.options[flDisk.selectedIndex].text +
              " — type ERASE in the box to confirm";
            return;
          }
          url = "/api/v1/factory/flash-disk";
          body = {disk: flDisk.value, device_type: dv[0], version: dv[1], verify: true};
        } else {
          if (!flPort.value) { flLog.style.display = ""; flLog.textContent = "pick a port"; return; }
          url = "/api/v1/factory/flash";
          body = {port: flPort.value, device_type: dv[0], version: dv[1]};
        }
        flGo.disabled = true; flGo.textContent = sd ? "Writing…" : "Flashing…";
        flConf.value = "";
        flLog.style.display = ""; flLog.textContent = "starting…";
        fetchT(url, {method: "POST", headers: {"Content-Type": "application/json"},
                     body: JSON.stringify(body)}, 30000)
          .then(function (r) { return r.json().then(function (j) { return {ok: r.ok, j: j}; }); })
          .then(function (x) {
            if (!x.ok) { flLog.textContent = x.j.err || x.j.error || "rejected"; reset(); return; }
            pollJob(x.j.id, sd);
          })
          .catch(function (e) { flLog.textContent = "failed: " + (e && e.message || e); reset(); });
      };
      showTarget();
      // plug-and-see: poll the active target list while the tab is visible
      var tick = setInterval(function () {
        if (!document.body.contains(host)) { clearInterval(tick); return; }
        if (target() === "sdcard") loadDisks(); else loadPorts();
      }, 3000);
    }).catch(function (e) {
      host.innerHTML = '<div class="empty err">' + esc(e.message || e) + "</div>";
    });
  }

  // Fleet: what every device is RUNNING vs the active OTA target — the
  // "who is behind" view that previously existed nowhere.
  function factoryFleet(host) {
    // Cache-first: the hub persists every device's last self-reported
    // version, so the table renders complete in one round trip.  The
    // WebSocket then streams changes — connecting it also makes the hub
    // re-ask stale sensors in the background (serialized on the mesh).
    api("/api/v1/fleet").then(function (f) {
      var targets = (f && f.targets) || {};
      var rows = (f && f.rows) || [];
      var idx = {};                       // device name → row element id
      rows.forEach(function (x, i) { idx[x.name] = i; });

      function cells(x) {
        var tgt = targets[x.key] || "";
        var v = NN.fleetStatus(x.running, tgt);
        var st = v.st, cls = v.cls;
        var ago = x.seen_at ? NN.fmtAgo(Date.now() / 1000 - x.seen_at) : "";
        // running and target merged into one cell: "0.0.31 → 0.0.32", or the
        // running version alone when it matches or there is no target
        var run = x.running && x.running !== "-" ? x.running : "—";
        var ver = esc(run) + (tgt && v.st !== "up to date" ? ' <span class="fleet-arrow">→ ' + esc(tgt) + "</span>" : "");
        return '<span class="fleet-dev">' + esc(x.name) + "</span>" +
          '<span class="fleet-st ' + cls + '" data-label="status">' + st + "</span>" +
          '<span class="mono fleet-fwname" data-label="firmware" title="' + esc(x.key || "") + '">' + esc(x.key || "—") + "</span>" +
          '<span class="mono" data-label="version"' + (ago ? ' title="reported ' + esc(ago) + ' ago"' : "") + ">" + ver + "</span>";
      }

      host.innerHTML = '<div class="setting"><div class="meta">' +
        '<h3>Fleet</h3><p>What each device is running against the ' +
        'catalog\'s active target, from the hub\'s cache.  Live changes ' +
        'stream in; stale sensors are re-asked in the background.</p>' +
        '</div></div>' +
        '<div class="setting"><div class="meta"><div class="fleet-tbl fw-tbl">' +
        '<div class="fleet-row fleet-head"><span>device</span><span>status</span>' +
        '<span>firmware</span><span>running → target</span></div>' +
        rows.map(function (x, i) {
          return '<div class="fleet-row" id="fleet-r' + i + '">' +
                 cells(x) + "</div>";
        }).join("") + "</div></div></div>";

      wsUrl("/api/v1/fleet/ws").then(function (url) {
        var ws;
        try { ws = new WebSocket(url); } catch (e) { return; }
        ws.onmessage = function (ev) {
          var j = null;
          try { j = JSON.parse(ev.data); } catch (e) { return; }
          if (!j || !j.name || !(j.name in idx)) return;
          var i = idx[j.name], el = document.getElementById("fleet-r" + i);
          if (!el) return;
          rows[i].running = j.running || rows[i].running;
          rows[i].seen_at = j.at || rows[i].seen_at;
          el.innerHTML = cells(rows[i]);
        };
        teardown = function () { try { ws.close(); } catch (e) {} };
      });
    }).catch(function (e) {
      host.innerHTML = '<div class="empty err">failed to load: ' +
        esc(e && e.message || e) + "</div>";
    });
  }


  // ── radio health (RADIO_STATS, hub/radio_health.py) ──────────────────
  // Channel busy = share of transmit attempts the radio aborted because
  // the channel was occupied (interference: a channel change helps).
  // No ack = frames sent that the peer never acknowledged (weak link: a
  // channel change does NOT help).  Hub repeat = messages the device had
  // to resend because the hub's ack never reached it.
  function rhCell(v, warn, bad, label) {
    var dl = ' data-label="' + label + '"';
    if (v === null || v === undefined) return '<span class="mono fleet-st dim"' + dl + '>—</span>';
    var cls = (bad !== undefined && v >= bad) ? "rh-bad" : (v >= warn ? "warn" : "ok");
    return '<span class="mono fleet-st ' + cls + '"' + dl + '>' + v + "%</span>";
  }
  function renderRadioHealth(parent) {
    var intro = document.createElement("div");
    intro.className = "setting";
    intro.innerHTML = '<div class="meta"><h3>Radio health</h3><p>From each ' +
      'sensor\'s 5-minute radio report, last 30 min.  <b>Busy</b> high on ' +
      'several devices means interference, and a channel change helps; <b>no ack</b> ' +
      'high on one device means a weak link, and it does not.  <b>Hub repeat</b>: ' +
      'messages the device had to resend because the hub\'s ack never reached it.</p></div>';
    var card = document.createElement("div");
    card.className = "setting";
    card.innerHTML = '<div class="meta"><div class="fleet-tbl rh-tbl" id="rh-box">' +
      '<div class="empty">loading…</div></div></div>';
    parent.appendChild(intro);
    parent.appendChild(card);
    api("/api/v1/radio/health?minutes=30").then(function (j) {
      var box = card.querySelector("#rh-box");
      var devs = (j && j.devices) || [];
      if (!devs.length) {
        box.innerHTML = '<div class="empty">no radio reports yet — sensors send one ' +
          'every 5 min (firmware 0.0.47+)</div>';
        return;
      }
      box.innerHTML = '<div class="fleet-row fleet-head">' +
        "<span>device</span><span>link</span><span>busy</span><span>no ack</span>" +
        "<span>mac retry</span><span>hub repeat</span><span>reasm/h</span><span>report</span></div>" +
        devs.map(function (d) {
          var w = d.window || {};
          var age = d.report_age_s;
          var rssi = (d.parent_rssi !== null && d.parent_rssi !== undefined) ? d.parent_rssi + " dBm" : "";
          var lq = (d.parent_lq && d.parent_lq[0] !== null) ? "lq " + d.parent_lq.join("/") : "";
          var link1 = "ch" + (d.channel || "?") + " · " + esc(d.role || "?") +
                      (d.parent ? " → " + esc(d.parent) : "");
          var link2 = [rssi, lq].filter(Boolean).join(" · ");
          var pc = w.parent_changes;
          return '<div class="fleet-row"><span class="rh-dev">' + esc(d.device) + "</span>" +
            '<span class="mono rh-link" data-label="link" title="' + link1 + (link2 ? " · " + link2 : "") + '">' +
              link1 + (link2 ? '<small>' + link2 + "</small>" : "") + "</span>" +
            rhCell(w.busy_pct, 20, 50, "busy") + rhCell(w.no_ack_pct, 10, 30, "no ack") +
            rhCell(w.mac_retry_pct, 20, 40, "mac retry") + rhCell(w.hub_repeat_pct, 10, 30, "hub repeat") +
            '<span class="mono" data-label="reasm/h">' +
              (w.reassembly_fail_per_h !== undefined ? w.reassembly_fail_per_h : "—") + "</span>" +
            '<span class="fleet-st ' + (age > 900 || pc ? "warn" : "dim") + '" data-label="report">' +
              NN.fmtAgo(age) + " ago" + (d.window ? "" : " · 1 report") +
              (pc ? " · " + pc + " parent chg" : "") + "</span></div>";
        }).join("");
    }).catch(function (e) {
      card.querySelector("#rh-box").innerHTML = '<div class="empty err">' + esc(String(e && e.message || e)) + "</div>";
    });
  }

  // ── Thread channel: scan, vote, move (hub/channel_manager.py) ───────
  function fmtClock(t) {
    if (!t) return "—";
    var d = new Date(t * 1000);
    return (d.getMonth() + 1) + "/" + d.getDate() + " " +
      String(d.getHours()).padStart(2, "0") + ":" + String(d.getMinutes()).padStart(2, "0");
  }
  function renderChannel(parent) {
    var card = document.createElement("div");
    card.className = "setting ch-card";
    card.innerHTML = '<div class="meta"><h3>Thread channel</h3>' +
      '<p id="ch-now">loading…</p>' +
      '<div id="ch-scan"></div><div id="ch-hist"></div>' +
      '<details class="met-tablebox ch-auto"><summary>Automatic channel change</summary>' +
      '<div id="ch-auto"></div></details></div>' +
      '<div class="switch"><button class="btn events" id="ch-scan-btn">Scan</button></div>';
    parent.appendChild(card);
    var now = card.querySelector("#ch-now"), scanBox = card.querySelector("#ch-scan"),
        histBox = card.querySelector("#ch-hist"), autoBox = card.querySelector("#ch-auto"),
        scanBtn = card.querySelector("#ch-scan-btn");
    var status = null;

    function showStatus(st) {
      status = st;
      var txt = "The mesh is on channel <b>" + (st.channel || "?") + "</b>.";
      if (st.pending)
        txt += '  <span class="fleet-st warn">Moving to channel ' + st.pending.channel +
               " at " + fmtClock(st.pending.effective_at) + "</span>";
      txt += "  Scan asks every gateway and sensor, one at a time, how busy each channel " +
             "is where it stands; each ranks its " + st.settings.k + " quietest channels " +
             "(Borda vote).  A channel any device hears at " + st.settings.veto_dbm +
             " dBm or more is ruled out.";
      now.innerHTML = txt;
      scanBtn.disabled = !!st.job_running;
      var h = st.history || [];
      histBox.innerHTML = h.length ? '<div class="fleet-tbl ch-hist"><div class="fleet-row fleet-head">' +
        "<span>when</span><span>move</span><span>by</span><span>result</span></div>" +
        h.slice(0, 5).map(function (x) {
          return '<div class="fleet-row"><span class="mono" data-label="when">' + fmtClock(x.requested_at) +
            '</span><span class="mono" data-label="move">' + (x.from || "?") + " → " + x.channel +
            '</span><span data-label="by">' + esc(x.source) + '</span><span class="fleet-st ' +
            (x.ok ? "ok" : "warn") + '" data-label="result" title="' + esc(x.detail || "") + '">' +
            (x.ok ? "confirmed" : "not confirmed") + "</span></div>";
        }).join("") + "</div>" : "";
      showAuto(st);
    }

    function showAuto(st) {
      var s = st.settings, a = st.auto;
      var ev = a ? (a.breach ? "Last check " + fmtClock(a.at) + ": " + esc(a.over.join(", ")) +
                   " over the limit → " + esc(a.action || "") :
                   "Last check " + fmtClock(a.at) + ": below the limit") : "Not checked yet.";
      autoBox.innerHTML =
        '<p>When at least half of the sensors (and at least ' + s["auto.min_devices"] +
        ') show the metric at or above the limit over ' + s["auto.sustain_min"] + ' min, the hub ' +
        'scans and moves the mesh if the vote finds a clearly better channel.  After a move it ' +
        'waits ' + s.cooldown_h + ' h before another automatic move.</p>' +
        '<div class="ch-auto-row">' +
        '<label class="afield"><span>On</span><label class="switch"><input type="checkbox" id="ch-a-en"' +
        (Number(s["auto.enabled"]) ? " checked" : "") + '><span class="slider-knob"></span></label></label>' +
        '<label class="afield"><span>Metric</span><select id="ch-a-metric" class="sselect">' +
        '<option value="busy"' + (s["auto.metric"] === "busy" ? " selected" : "") + '>channel busy</option>' +
        '<option value="no_ack"' + (s["auto.metric"] === "no_ack" ? " selected" : "") + '>no ack</option></select></label>' +
        '<label class="afield"><span>Limit %</span><input type="number" id="ch-a-thr" min="1" max="100" value="' +
        s["auto.threshold_pct"] + '"></label>' +
        '<button class="btn events" id="ch-a-save">Save</button></div>' +
        '<p class="ch-note" id="ch-a-note">' + ev + "</p>";
      autoBox.querySelector("#ch-a-save").onclick = function () {
        var note = autoBox.querySelector("#ch-a-note");
        fetchT("/api/v1/radio/channel/settings", {method: "PUT",
          headers: {"content-type": "application/json"},
          body: JSON.stringify({"auto.enabled": autoBox.querySelector("#ch-a-en").checked ? 1 : 0,
                                "auto.metric": autoBox.querySelector("#ch-a-metric").value,
                                "auto.threshold_pct": Number(autoBox.querySelector("#ch-a-thr").value)})}, 10000)
          .then(function (r) { return r.json().then(function (j) { return {ok: r.ok, j: j}; }); })
          .then(function (res) { note.textContent = res.ok ? "Saved." : "Not saved: " + (res.j.err || "?"); })
          .catch(function (e) { note.textContent = "Not saved: " + (e && e.message || e); });
      };
    }

    function showJob(job) {
      if (job.state === "running") {
        scanBox.innerHTML = '<p class="ch-note">Scanning… ' + job.scans.length + " of " +
          ((job.targets && (job.targets.gateways.length + job.targets.sensors)) || "?") +
          " devices done.</p>";
        return;
      }
      if (job.state === "failed") {
        scanBox.innerHTML = '<p class="ch-note fleet-st warn">Scan failed: ' + esc(job.error || "") + "</p>";
        return;
      }
      var cur = status && status.channel, dec = job.decision || {};
      var rows = (job.ranking || []).filter(function (r, i) {
        return i < 5 || r.channel === cur || r.vetoed_by.length;
      });
      var voters = job.scans.map(function (x) {
        return x.dbm ? esc(x.voter) : '<span class="fleet-st warn" title="' + esc(x.error || "") + '">' +
          esc(x.voter) + " (no scan)</span>";
      }).join(", ");
      scanBox.innerHTML = '<p class="ch-note">Scanned ' + fmtClock(job.finished_at) + " by " + voters + ".</p>" +
        '<div class="fleet-tbl ch-tbl"><div class="fleet-row fleet-head"><span>ch</span><span>points</span>' +
        "<span>loudest</span><span>status</span></div>" +
        rows.map(function (r) {
          var st = r.vetoed_by.length ? '<span class="fleet-st rh-bad" data-label="status">busy at ' +
              esc(r.vetoed_by.join(", ")) + "</span>" :
            r.channel === dec.winner ? '<span class="fleet-st ok" data-label="status">best</span>' :
            '<span class="fleet-st dim" data-label="status">clear</span>';
          return '<div class="fleet-row' + (r.channel === cur ? " ch-cur" : "") + '"><span class="mono">' +
            r.channel + (r.channel === cur ? " ·now" : "") + '</span><span class="mono" data-label="points">' +
            (Math.round(r.points * 10) / 10) + '</span><span class="mono" data-label="loudest">' +
            (r.worst_dbm === null ? "—" : r.worst_dbm + " dBm") + "</span>" + st + "</div>";
        }).join("") + "</div>" +
        '<div class="ch-decide"><span>' + esc(dec.reason || "") + "</span>" +
        (dec.winner && dec.winner !== cur ?
          '<button class="btn ' + (dec.move ? "primary" : "events") + '" id="ch-move">Move to ' +
          dec.winner + "</button>" : "") + '</div><p class="ch-note" id="ch-move-note"></p>';
      var mv = scanBox.querySelector("#ch-move");
      if (mv) {
        var armed = false;
        mv.onclick = function () {
          var note = scanBox.querySelector("#ch-move-note");
          if (!armed) {
            armed = true;
            mv.textContent = "Really move to " + dec.winner + "?";
            note.textContent = "Every device follows after " + status.settings.delay_s +
              " s; the mesh is quiet for a moment then.";
            setTimeout(function () { armed = false; mv.textContent = "Move to " + dec.winner; }, 6000);
            return;
          }
          mv.disabled = true;
          fetchT("/api/v1/radio/channel/migrate", {method: "POST",
            headers: {"content-type": "application/json"},
            body: JSON.stringify({channel: dec.winner, confirm: dec.winner, reason: "webapp: " + (dec.reason || "")})}, 30000)
            .then(function (r) { return r.json().then(function (j) { return {ok: r.ok, j: j}; }); })
            .then(function (res) {
              note.textContent = res.ok ? "Moving to channel " + res.j.channel + " at " +
                fmtClock(res.j.effective_at) + "." : "Not moved: " + (res.j.err || "?");
              refresh();
            })
            .catch(function (e) { mv.disabled = false; note.textContent = "Not moved: " + (e && e.message || e); });
        };
      }
    }

    function poll(id) {
      api("/api/v1/radio/channel/scan/" + id).then(function (job) {
        showJob(job);
        if (job.state === "running") setTimeout(function () { poll(id); }, 2000);
        else { scanBtn.disabled = false; refresh(); }
      }).catch(function () { scanBtn.disabled = false; });
    }
    scanBtn.onclick = function () {
      scanBtn.disabled = true;
      fetchT("/api/v1/radio/channel/scan", {method: "POST"}, 10000)
        .then(function (r) { return r.json().then(function (j) { return {ok: r.ok, j: j}; }); })
        .then(function (res) {
          if (res.ok) poll(res.j.job_id);
          else { scanBtn.disabled = false; scanBox.innerHTML = '<p class="ch-note fleet-st warn">' + esc(res.j.err || "") + "</p>"; }
        }).catch(function () { scanBtn.disabled = false; });
    };
    function refresh() {
      api("/api/v1/radio/channel").then(function (st) {
        var first = !status;
        showStatus(st);
        if (first && st.last_job && st.last_job.id)
          api("/api/v1/radio/channel/scan/" + st.last_job.id).then(showJob).catch(function () {});
      }).catch(function (e) { now.textContent = "Channel status unavailable: " + (e && e.message || e); });
    }
    refresh();
  }

  function viewGateway() {
    setHeader("Gateway", false);
    app.innerHTML = '<div class="empty">loading…</div>';
    Promise.all([
      api("/api/v1/gateway/service").catch(function (e) { return {error: String(e && e.message || e)}; }),
      api("/api/v1/gateways").catch(function () { return []; })
    ]).then(function (r) {
      var svc = r[0] || {}, gws = r[1] || [];
      app.innerHTML = "";
      var box = document.createElement("div");
      var healthy = svc.active === "active" && !svc.link_errors;
      function seenText(t) {
        if (!t) return {txt: "never connected", cls: "dim"};
        var a = Math.round(Date.now() / 1000 - t);
        if (a < 90) return {txt: "online", cls: "ok"};
        return {txt: "seen " + NN.fmtAgo(a) + " ago", cls: "warn"};
      }
      // the hub's own gateway is the one no camera hosts
      var local = gws.filter(function (g) { return !g.host_camera; })[0];
      var role = local ? (local.role_name || "?") : (svc.role || "?");
      var gwRows = gws.map(function (g) {
        var st = seenText(g.last_seen);
        var r = g.role_name || (g.role !== undefined ? "role " + g.role : "?");
        return '<div class="fleet-row"><span class="gw-name">' + esc(g.name || g.id) + "</span>" +
          '<span class="fleet-st ' + (r === "detached" ? "warn" : "dim") + '" data-label="role">' + esc(r) + "</span>" +
          '<span data-label="runs on">' + (g.host_camera ? esc(g.host_camera) : "this hub (USB)") + "</span>" +
          '<span class="mono gw-rloc" data-label="rloc16">' +
            (g.rloc16 ? "0x" + g.rloc16.toString(16).padStart(4, "0") : "—") + "</span>" +
          '<span class="fleet-st ' + st.cls + '" data-label="status">' + st.txt + "</span></div>";
      }).join("");
      var ports = (svc.ncp_ports || []).map(function (p) { return esc(p.split("/").pop()); });
      var kv = function (k, v, cls) {
        return '<div class="fleet-row"><span>' + k + '</span><span class="' + (cls || "") + '">' + v + "</span></div>";
      };
      box.innerHTML =
        '<div class="setting"><div class="meta"><h3>Gateways</h3>' +
        '<p>Border routers that link the Thread mesh to the hub.  One runs on this ' +
        'hub over USB; cameras can host more.</p>' +
        (gws.length ? '<div class="fleet-tbl gw-tbl"><div class="fleet-row fleet-head">' +
          '<span>gateway</span><span>role</span><span>runs on</span><span class="gw-rloc">rloc16</span>' +
          "<span>status</span></div>" + gwRows + "</div>"
          : '<div class="empty">no gateways registered</div>') +
        "</div></div>" +
        '<div class="setting gw-svc"><div class="meta"><h3>Hub gateway service</h3>' +
        '<p>After replugging the NCP\'s USB the service keeps its old port open and ' +
        'stops routing.  Restarting makes it find the port again.</p>' +
        '<div class="fleet-tbl gw-kv" id="gw-kv">' +
        (svc.error
          ? kv("status", "unavailable: " + esc(svc.error), "fleet-st warn")
          : kv("service", esc(svc.unit || "?") + ' <span class="fleet-st ' +
                 (svc.active === "active" ? "ok" : "warn") + '">' + esc(svc.active || "?") + "</span>") +
            kv("since", esc(svc.since || "?"), "mono") +
            kv("thread role", esc(role) + (role === "detached" ? " · not routing" : ""),
               "fleet-st " + (role === "detached" ? "warn" : "dim")) +
            kv("link errors", (svc.link_errors || 0) +
                 (svc.link_errors ? " · stale port, restart to re-probe" : ""),
               "fleet-st " + (svc.link_errors ? "warn" : "ok")) +
            kv("NCP ports", ports.length ? ports.join("<br>") : "none found", "mono gw-ports")) +
        '</div><div class="gw-note" id="gw-note"></div>' +
        ((svc.log || []).length ? '<details class="met-tablebox gw-log"><summary>Service log · last ' +
          svc.log.length + ' lines</summary><div class="logbox" id="gw-info"></div></details>' : "") +
        '</div><div class="switch"><button class="btn ' +
        (healthy ? 'events' : 'danger') + '" id="gw-restart">Restart</button></div></div>';
      app.appendChild(box);
      renderRadioHealth(app);
      renderChannel(app);
      var logEl = box.querySelector("#gw-info");
      if (logEl) logEl.textContent = svc.log.join("\n");
      var info = box.querySelector("#gw-note");
      var btn = box.querySelector("#gw-restart");
      var armed = false;
      btn.onclick = function () {
        if (!armed) {
          armed = true; btn.textContent = "Really restart?";
          setTimeout(function () { armed = false; btn.textContent = "Restart"; }, 5000);
          return;
        }
        armed = false; btn.disabled = true; btn.textContent = "restarting…";
        fetchT("/api/v1/gateway/service/restart", {method: "POST"}, 40000)
          .then(function (rr) { return rr.json().then(function (j) { return {ok: rr.ok, j: j}; }); })
          .then(function (res) {
            btn.disabled = false; btn.textContent = "Restart";
            info.textContent = res.ok
              ? "restart issued — " + (res.j.note || "") +
                "\nreload this page in a minute to see the new state"
              : "restart failed: " + (res.j.err || res.j.error || "unknown");
          })
          .catch(function (e) {
            btn.disabled = false; btn.textContent = "Restart";
            info.textContent = "restart failed: " + (e && e.message || e);
          });
      };
    });
  }

  // ── device lifecycle: unregister overlay, archive, add-device wizard ─────

  function makeOverlay(title) {
    var ov = document.createElement("div");
    ov.className = "overlay";
    ov.innerHTML =
      '<div class="modal"><h3>' + esc(title) + '</h3>' +
      '<div class="modal-body"></div>' +
      '<div class="modal-btns"></div></div>';
    document.body.appendChild(ov);
    return {
      el: ov,
      body: ov.querySelector(".modal-body"),
      btns: ov.querySelector(".modal-btns"),
      close: function () { try { ov.remove(); } catch (e) {} }
    };
  }

  // checklist item: state = "spin" | "ok" | "fail" | "todo"
  function checkItem(label) {
    var li = document.createElement("li");
    li.innerHTML = '<span class="ck-ico todo"></span><span class="ck-txt">' +
                   esc(label) + '</span><small class="ck-note"></small>';
    return {
      el: li,
      set: function (state, note) {
        var ico = li.querySelector(".ck-ico");
        ico.className = "ck-ico " + state;
        ico.textContent = state === "ok" ? "✓" : state === "fail" ? "✗" : "";
        if (note !== undefined) li.querySelector(".ck-note").textContent = note;
      }
    };
  }

  function renderUnregister(root, id, kind) {
    var box = document.createElement("div");
    box.innerHTML =
      '<div class="setting"><div class="meta">' +
      '<h3>Unregister device</h3><p>Reset the device to factory setup ' +
      'mode and move its records to the Archive.</p></div>' +
      '<div class="switch"><button class="btn danger">Unregister</button></div></div>';
    root.appendChild(box);
    box.querySelector("button").onclick = function () {
      openUnregisterOverlay(id, kind);
    };
  }

  function openUnregisterOverlay(id, kind) {
    var ov = makeOverlay("Unregister " + id);
    ov.body.innerHTML =
      '<p class="warn">This device will be <b>RESET</b>: its network ' +
      'credentials, keys and settings are erased and it returns to setup ' +
      'mode.  Its data on the hub moves to the Archive.</p>';
    var ul = document.createElement("ul");
    ul.className = "check-list";
    ov.body.appendChild(ul);

    var items = {
      auto:  checkItem("checking if device is used in automation"),
      clear: checkItem("send sealed clear-user-data command"),
      ack:   checkItem("device armed clear-on-reboot"),
      boot:  checkItem("reboot device"),
      arch:  checkItem("archive device data on hub")
    };
    Object.keys(items).forEach(function (k) { ul.appendChild(items[k].el); });

    var confirmB = document.createElement("button");
    confirmB.className = "btn danger";
    confirmB.textContent = "Confirm";
    confirmB.disabled = true;
    var cancelB = document.createElement("button");
    cancelB.className = "btn events";
    cancelB.textContent = "Cancel";
    var forceB = document.createElement("button");
    forceB.className = "btn events";
    forceB.textContent = "Force archive (device unreachable)";
    forceB.hidden = true;
    ov.btns.appendChild(forceB);
    ov.btns.appendChild(cancelB);
    ov.btns.appendChild(confirmB);
    cancelB.onclick = ov.close;

    // 1. automation usage — cameras are never referenced by rules today,
    //    but run the same check so the flow reads identically.
    items.auto.set("spin");
    var usageUrl = kind === "cam"
      ? null   // cameras: not addressable from automation rules
      : "/api/v1/devices/" + encodeURIComponent(id) + "/automation-usage";
    (usageUrl ? api(usageUrl) : Promise.resolve({used: false}))
      .then(function (u) {
        if (u.used) {
          items.auto.set("fail", "used by: " + (u.rules || []).join(", ") +
            " — remove it from these rules first");
          var link = document.createElement("a");
          link.href = "#/automation"; link.textContent = "open Automation";
          link.className = "modal-link";
          link.onclick = ov.close;
          ov.body.appendChild(link);
        } else {
          items.auto.set("ok");
          confirmB.disabled = false;
        }
      })
      .catch(function () {
        items.auto.set("fail", "check failed — is the hub ok?");
      });

    function run(force) {
      confirmB.disabled = true;
      forceB.hidden = true;
      cancelB.disabled = true;
      var base = kind === "cam"
        ? "/api/v1/cameras/" + encodeURIComponent(id)
        : "/api/v1/devices/" + encodeURIComponent(id);
      items.clear.set("spin");
      fetchT(base + "/unregister",
             {method: "POST", headers: {"Content-Type": "application/json"},
              body: JSON.stringify({force: !!force})}, 30000)
        .then(function (r) {
          return r.json().then(function (j) { return {ok: r.ok, j: j}; });
        })
        .then(function (res) {
          if (!res.ok) {
            var msg = (res.j && (res.j.err || res.j.error)) || "failed";
            if (String(msg).indexOf("automation_in_use") === 0) {
              items.auto.set("fail", msg);
              items.clear.set("todo");
            } else {
              items.clear.set("fail", msg);
              forceB.hidden = false;
            }
            cancelB.disabled = false;
            return;
          }
          if (kind === "cam") {
            // camera unregister is synchronous on the hub side
            items.clear.set("ok"); items.ack.set("ok", "camera wipes on restart");
            items.boot.set("ok", "restarting"); items.arch.set("ok");
            confirmB.textContent = "Done";
            confirmB.disabled = false;
            confirmB.onclick = function () { ov.close(); location.hash = "#/"; };
            return;
          }
          poll();
        })
        .catch(function (e) {
          items.clear.set("fail", String(e && e.message || e));
          cancelB.disabled = false;
          forceB.hidden = false;
        });

      function poll() {
        fetchT("/api/v1/devices/" + encodeURIComponent(id) +
               "/unregister/status", null, 10000)
          .then(function (r) { return r.json(); })
          .then(function (st) {
            var state = st.state || "?";
            if (state === "starting" || state === "clearing") {
              items.clear.set("spin");
            } else if (state === "cleared") {
              items.clear.set("ok"); items.ack.set("ok");
              items.boot.set("spin");
            } else if (state === "rebooting") {
              items.clear.set("ok"); items.ack.set("ok");
              items.boot.set("ok", st.detail || "");
              items.arch.set("spin");
            } else if (state === "archived") {
              items.clear.set("ok"); items.ack.set("ok");
              items.boot.set("ok"); items.arch.set("ok");
              confirmB.textContent = "Done";
              confirmB.disabled = false;
              confirmB.onclick = function () { ov.close(); location.hash = "#/"; };
              return;
            } else if (state === "error") {
              items.clear.set("fail", st.detail || "error");
              cancelB.disabled = false;
              confirmB.textContent = "Retry";
              confirmB.disabled = false;
              forceB.hidden = false;
              return;
            }
            setTimeout(poll, 2000);
          })
          .catch(function () { setTimeout(poll, 3000); });
      }
    }
    confirmB.onclick = function () { run(false); };
    forceB.onclick = function () {
      if (forceB.textContent.indexOf("Really") !== 0) {
        forceB.textContent = "Really force? Device stays as-is";
        return;
      }
      run(true);
    };
  }

  // ── archived device settings ────────────────────────────────────────────
  function viewArchivedDevice(dev) {
    setHeader(dev + " (archived)", true);
    app.innerHTML = '<div class="empty">loading…</div>';
    api("/api/v1/archive/" + encodeURIComponent(dev)).then(function (a) {
      app.innerHTML = "";
      var box = document.createElement("div");
      var snap = a.snapshot || {};
      var pi = snap.provision_info || {};
      box.innerHTML =
        '<div class="setting"><div class="meta"><h3>' + esc(a.name) + '</h3>' +
        '<p>' + esc(a.device_type || "?") + ' · archived ' +
        new Date(a.archived_at * 1000).toLocaleString() +
        (snap.forced ? " · forced (device was not reset)" : "") +
        '</p></div></div>' +
        '<div class="logbox">' +
        'device_id  ' + esc(a.device_id) + "\n" +
        'gateway    ' + esc(pi.gateway_id || "-") + "\n" +
        'fields     ' + esc(String((pi.capabilities || []).length)) +
        '</div>' +
        '<div class="logbox arch-events" style="display:none"></div>' +
        '<div class="setting"><div class="meta"><h3>Delete history</h3>' +
        '<p>Permanently removes ALL of this device\'s historic data from ' +
        'the hub: telemetry, events, configuration and this archive ' +
        'entry — for a camera, including its recorded event videos.  It ' +
        'does not contact the device and does not re-admit anything; ' +
        're-admitting a camera id is the Adopt step on the Add-device page.</p></div>' +
        '<div class="switch"><button class="btn danger" id="arch-del">Delete</button></div></div>';
      app.appendChild(box);
      // camera lives own their recordings: list the window's events
      api("/api/v1/archive/" + encodeURIComponent(dev) + "/events")
        .then(function (evs) {
          if (!evs || !evs.length) return;
          var bx = box.querySelector(".arch-events");
          bx.style.display = "";
          bx.textContent = evs.length + " recorded event(s) in this life:\n" +
            evs.map(function (e) {
              return e.day + "  " + e.event + "  (" +
                     e.files.length + " files)";
            }).join("\n") +
            "\nDeleting this archive entry removes these videos.";
        }).catch(function () {});
      var btn = box.querySelector("#arch-del");
      var armed = false;
      btn.onclick = function () {
        if (!armed) {
          armed = true; btn.textContent = "Really delete everything?";
          setTimeout(function () {
            armed = false; btn.textContent = "Delete";
          }, 5000);
          return;
        }
        btn.disabled = true;
        fetchT("/api/v1/archive/" + encodeURIComponent(a.device_id),
               {method: "DELETE"}, 15000)
          .then(function (r) {
            if (r.ok) { location.hash = "#/"; }
            else { btn.disabled = false; btn.textContent = "Delete failed — retry"; }
          })
          .catch(function () { btn.disabled = false; });
      };
    }).catch(function () {
      app.innerHTML = '<div class="empty err">not found in archive</div>';
    });
  }

  // ── add-device wizard ───────────────────────────────────────────────────
  // ── add-device wizard: type → find → details → self-register ───────────
  // Four explicit steps, one visible at a time.  Everything the
  // deployment already knows (stream endpoint, keys, ports, which slot)
  // is resolved by the hub — the operator is only asked for what only
  // they can know: which device, what to call it, and the Wi-Fi secret.
  var ADD_STEPS = ["Device type", "Find the device", "Details", "Register"];

  function viewAddDevice() {
    setHeader("Add device", true);
    app.innerHTML =
      '<ol class="wiz-steps"></ol>' +
      '<div class="wiz-body"></div>';
    var st = {kind: null, addr: null, cand: null, at: 0};
    renderWizard(st);
  }

  function renderWizard(st) {
    var ol = app.querySelector(".wiz-steps");
    ol.innerHTML = "";
    ADD_STEPS.forEach(function (label, i) {
      var li = document.createElement("li");
      li.className = "wiz-step" + (i === st.at ? " on" : "") +
                     (i < st.at ? " done" : "");
      li.innerHTML = '<span class="n">' + (i < st.at ? "✓" : (i + 1)) +
                     '</span>' + esc(label);
      // completed steps are clickable — going back is normal, not an error
      if (i < st.at) li.onclick = function () { st.at = i; renderWizard(st); };
      ol.appendChild(li);
    });
    var b = app.querySelector(".wiz-body");
    b.innerHTML = "";
    [wizType, wizFind, wizDetails, wizRegister][st.at](b, st);
  }

  function wizNav(host, st, backLabel, nextFn, nextLabel, nextEnabled) {
    var row = document.createElement("div");
    row.className = "wiz-nav";
    if (st.at > 0) {
      var back = document.createElement("button");
      back.className = "btn events";
      back.textContent = backLabel || "Back";
      back.onclick = function () { st.at--; renderWizard(st); };
      row.appendChild(back);
    }
    if (nextFn) {
      var next = document.createElement("button");
      next.className = "btn live";
      next.textContent = nextLabel || "Next";
      next.disabled = nextEnabled === false;
      next.onclick = nextFn;
      row.appendChild(next);
    }
    host.appendChild(row);
    return row;
  }

  // step 1 — what kind of device
  function wizType(host, st) {
    host.innerHTML =
      '<div class="setting"><div class="meta"><h3>What are you adding?</h3>' +
      '<p>Sensors join the Thread mesh.  Cameras stream over Wi-Fi or ' +
      'Ethernet.  Both are set up over Bluetooth from this hub.</p></div></div>' +
      '<div class="add-kinds">' +
      '<button class="btn events" data-kind="sensor">Sensor' +
        '<br><small>Thread mesh · BLE setup</small></button>' +
      '<button class="btn events" data-kind="camera_esp">Wi-Fi camera' +
        '<br><small>ESP32 · BLE setup</small></button>' +
      '<button class="btn events" data-kind="camera_net">Network camera' +
        '<br><small>registers itself</small></button>' +
      '</div>';
    [].forEach.call(host.querySelectorAll(".add-kinds button"), function (btn) {
      btn.onclick = function () {
        st.kind = btn.getAttribute("data-kind");
        // a network camera has nothing to find or configure over BLE
        st.at = (st.kind === "camera_net") ? 3 : 1;
        renderWizard(st);
      };
    });
  }

  // step 2 — find it over BLE
  function wizFind(host, st) {
    host.innerHTML =
      '<div class="setting"><div class="meta"><h3>Find the device</h3>' +
      '<p>Put it in setup mode — factory-fresh and just-unregistered ' +
      'devices advertise automatically — then scan.</p></div>' +
      '<div class="switch"><button class="btn events" id="wz-scan">Scan</button></div></div>' +
      '<div class="add-candidates"></div>';
    var cands = host.querySelector(".add-candidates");
    var nav = wizNav(host, st, "Back", function () {
      st.at = 2; renderWizard(st);
    }, "Next", false);
    var nextBtn = nav.querySelector(".btn.live");

    function scan() {
      var b = host.querySelector("#wz-scan");
      b.disabled = true; b.textContent = "Scanning…";
      cands.innerHTML = "";
      nextBtn.disabled = true;
      fetchT("/api/v1/provision/scan", {method: "POST",
             headers: {"Content-Type": "application/json"},
             body: JSON.stringify({scan_time: 10})}, 30000)
        .then(function (r) { return r.json(); })
        .then(function (j) {
          b.disabled = false; b.textContent = "Rescan";
          var want = (st.kind === "camera_esp") ? "camera_esp" : "sensor";
          var list = (j.candidates || []).filter(function (c) { return c.kind === want; });
          if (!list.length) {
            cands.innerHTML = '<div class="empty">nothing in setup mode ' +
              'found.  A device that is already provisioned does not ' +
              'advertise — unregister it first.</div>';
            return;
          }
          list.forEach(function (c) {
            var btn = document.createElement("button");
            btn.className = "btn events cand";
            btn.innerHTML = esc(c.name || "(unnamed)") +
                            ' <small>' + esc(c.addr) + '</small>' +
                            (c.fw_image
                              ? '<br><small class="cand-fw">' + esc(c.fw_image) +
                                (c.fw_version ? ' · ' + esc(c.fw_version) : '') +
                                (c.label ? ' · ' + esc(c.label) : '') +
                                '</small>'
                              : '<br><small class="cand-fw dim">firmware: unknown ' +
                                '(pre-identity image)</small>');
            btn.onclick = function () {
              st.addr = c.addr; st.cand = c;
              [].forEach.call(cands.querySelectorAll(".cand"), function (o) {
                o.classList.remove("sel");
              });
              btn.classList.add("sel");
              nextBtn.disabled = false;
            };
            cands.appendChild(btn);
          });
          if (list.length === 1) list[0] && cands.querySelector(".cand").click();
        })
        .catch(function (e) {
          b.disabled = false; b.textContent = "Rescan";
          cands.innerHTML = '<div class="empty err">scan failed: ' +
            esc(String(e && e.message || e)) + '</div>';
        });
    }
    host.querySelector("#wz-scan").onclick = scan;
    scan();                       // scanning is why you came here
  }

  // step 3 — the only things the hub cannot know
  function wizDetails(host, st) {
    var isCam = st.kind === "camera_esp";
    host.innerHTML =
      '<div class="setting"><div class="meta"><h3>Details</h3><p>' +
      (st.addr ? 'Setting up <b>' + esc(st.addr) + '</b>.  ' : '') +
      (isCam ? 'The camera needs your Wi-Fi credentials; everything else ' +
               '(stream endpoint, keys) the hub fills in.'
             : 'The device joins the Thread mesh using the hub\'s network ' +
               'settings — only a name is needed.') +
      '</p></div></div>' +
      '<div class="add-form">' +
      '<label>Name <input id="wz-name" placeholder="' +
        (isCam ? "e.g. Front door" : "e.g. c6-s4") + '"></label>' +
      (isCam
        ? '<label>Wi-Fi network <input id="wz-ssid"></label>' +
          '<label>Wi-Fi password <input id="wz-pass" type="password"></label>' +
          '<label id="wz-slot-row" hidden>Camera slot <select id="wz-slot"></select></label>'
        : '<label>Type <input id="wz-type" value="sample_c6"></label>') +
      '</div>';
    if (isCam) {
      api("/api/v1/settings/last-wifi-ssid").catch(function () { return null; })
        .then(function (j) {
          if (j && j.value) host.querySelector("#wz-ssid").value = j.value;
        });
      api("/api/v1/camera-slots").then(function (slots) {
        var free = (slots || []).filter(function (sl) { return sl.free; });
        var sel = host.querySelector("#wz-slot");
        free.forEach(function (sl) {
          var o = document.createElement("option");
          o.value = sl.id;
          o.textContent = sl.id + (sl.name && sl.name !== sl.id ? "  (" + sl.name + ")" : "");
          sel.appendChild(o);
        });
        // only a real choice is worth showing
        if (free.length > 1) host.querySelector("#wz-slot-row").hidden = false;
        if (free.length === 0) {
          var w = document.createElement("div");
          w.className = "empty err";
          w.textContent = "every camera slot is in use — a new camera needs " +
            "its own video service on the media host first.";
          host.querySelector(".add-form").appendChild(w);
        }
      }).catch(function () {});
    }
    wizNav(host, st, "Back", function () {
      var name = (host.querySelector("#wz-name").value || "").trim();
      if (!name) { alert("give the device a name"); return; }
      st.name = name;
      if (isCam) {
        st.ssid = host.querySelector("#wz-ssid").value;
        st.pass = host.querySelector("#wz-pass").value;
        var row = host.querySelector("#wz-slot-row");
        st.slot = row.hidden ? "" : host.querySelector("#wz-slot").value;
        if (!st.ssid || !st.pass) { alert("Wi-Fi network and password are required"); return; }
      } else {
        st.dtype = host.querySelector("#wz-type").value || "sample_c6";
      }
      st.at = 3; renderWizard(st);
    }, "Start setup");
  }

  // step 4 — hand off, then wait for the device to register itself
  function wizRegister(host, st) {
    if (st.kind === "camera_net") {
      host.innerHTML =
        '<div class="setting"><div class="meta"><h3>Network camera</h3>' +
        '<p>Start the camera\'s media service and authorize its device key ' +
        'on the streaming host.  When it connects it registers itself — ' +
        'there is no pairing step.</p></div></div>' +
        '<div class="logbox" id="wz-out">waiting for a new camera to register…</div>';
      wizNav(host, st, "Back", null);
      watchNewCamera(host.querySelector("#wz-out"));
      return;
    }
    host.innerHTML = '<ul class="check-list" id="wz-steps"></ul>' +
                     '<div class="logbox" id="wz-out" style="display:none"></div>';
    var ul = host.querySelector("#wz-steps");
    var out = host.querySelector("#wz-out");
    var isCam = st.kind === "camera_esp";
    var itProv = checkItem("provisioning " + st.name);
    var itJoin = checkItem(isCam ? "camera joins Wi-Fi and registers"
                                 : "device joins the mesh");
    ul.appendChild(itProv.el); ul.appendChild(itJoin.el);
    itProv.set("spin");

    var body = {kind: st.kind, name: st.name, addr: st.addr};
    if (isCam) {
      body.ssid = st.ssid; body.password = st.pass;
      if (st.slot) body.target_cam = st.slot;
    } else {
      body.device_type = st.dtype;
    }
    fetchT("/api/v1/provision/jobs", {method: "POST",
           headers: {"Content-Type": "application/json"},
           body: JSON.stringify(body)}, 25000)
      .then(function (r) { return r.json(); })
      .then(function (j) {
        if (!j.id) { itProv.set("fail", j.err || "rejected"); retry(); return; }
        (function poll() {
          api("/api/v1/provision/jobs/" + j.id).then(function (s2) {
            if (s2.state === "running") {
              itProv.set("spin", s2.step || "");
              setTimeout(poll, 1500);
            } else if (s2.state === "done") {
              itProv.set("ok");
              itJoin.set("spin");
              waitAlive();
            } else if (s2.state === "unconfirmed") {
              // The device took the config but never confirmed.  Say so
              // plainly instead of claiming success: whether it worked is
              // decided by the device registering, below.
              itProv.set("fail", "device did not confirm — see below");
              out.style.display = "";
              out.textContent = s2.error || "";
              itJoin.set("spin");
              waitAlive();
            } else {
              itProv.set("fail", s2.error || "provisioning failed");
              out.style.display = ""; out.textContent = s2.error || "";
              retry();
            }
          }).catch(function () { setTimeout(poll, 3000); });
        })();
      })
      .catch(function (e) { itProv.set("fail", String(e && e.message || e)); retry(); });

    function retry() {
      wizNav(host, st, "Back", function () { renderWizard(st); }, "Try again");
    }
    function waitAlive() {
      var t0 = Date.now();
      (function chk() {
        if (Date.now() - t0 > 180000) {
          itJoin.set("fail", isCam
            ? "no registration in 3 min — a wrong Wi-Fi password is the usual cause"
            : "no contact in 3 min — check the gateway");
          retry();
          return;
        }
        var probe = isCam
          ? api("/api/v1/cameras").then(function (cs) {
              return (cs || []).some(function (c) {
                return c.name === st.name || c.id === (st.slot || "");
              });
            })
          : fetchT("/api/v1/devices/" + encodeURIComponent(st.name) + "/uptime",
                   null, 8000).then(function (r) { return r.ok; });
        probe.then(function (alive) {
          if (alive) {
            itJoin.set("ok");
            setTimeout(function () { location.hash = "#/"; }, 1200);
          } else { setTimeout(chk, 6000); }
        }).catch(function () { setTimeout(chk, 6000); });
      })();
    }
  }

  function watchNewCamera(outEl) {
    var known = {};
    api("/api/v1/cameras").then(function (cs) {
      (cs || []).forEach(function (c) { known[c.id] = 1; });
    });
    (function watch() {
      api("/api/v1/cameras").then(function (cs) {
        var fresh = (cs || []).filter(function (c) { return !known[c.id]; });
        if (fresh.length) {
          outEl.textContent = "registered: " + fresh[0].name + " ✓";
          setTimeout(function () { location.hash = "#/"; }, 1500);
          return;
        }
        setTimeout(watch, 5000);
      }).catch(function () { setTimeout(watch, 8000); });
    })();
  }

  // ── device reboot: operator-requested, two-step ─────────────────────────
  // Disruptive, so the button arms first ("Really reboot?") instead of
  // firing on a stray click.  A 200 means the device ACKNOWLEDGED and is
  // going down — it then re-attaches on its own, so the status line tracks
  // "rebooting… back when it answers" by polling a cheap read.
  function renderDeviceReboot(root, dev, opts) {
    opts = opts || {};
    var postUrl = opts.postUrl ||
      "/api/v1/devices/" + encodeURIComponent(dev) + "/reboot";
    var probeUrl = opts.probeUrl ||
      "/api/v1/devices/" + encodeURIComponent(dev) + "/field/led";
    var blurb = opts.blurb ||
      "Warm-reboots the device now (firmware 0.0.27+).  It drops off the " +
      "mesh and re-attaches by itself, typically within a minute.";
    var box = document.createElement("div");
    box.innerHTML =
      '<div class="setting"><div class="meta">' +
      '<h3>Reboot device</h3><p>' + blurb + '</p></div>' +
      '<div class="switch"><button class="dev-reboot">Reboot</button></div></div>' +
      '<div class="logbox reboot-out" style="display:none"></div>';
    root.appendChild(box);
    var btn = box.querySelector(".dev-reboot");
    var out = box.querySelector(".reboot-out");
    var armed = false, disarmT = null;
    btn.onclick = function () {
      if (!armed) {
        armed = true;
        btn.textContent = "Really reboot?";
        btn.style.background = "#ffb4a2";
        btn.style.color = "#2a0d0d";
        disarmT = setTimeout(function () {
          armed = false; btn.textContent = "Reboot";
          btn.style.background = ""; btn.style.color = "";
        }, 5000);
        return;
      }
      clearTimeout(disarmT);
      armed = false;
      btn.textContent = "Reboot"; btn.style.background = ""; btn.style.color = "";
      btn.disabled = true;
      out.style.display = "";
      out.textContent = "sending REBOOT to " + dev + "…";
      fetchT(postUrl, {method: "POST"}, 35000)
        .then(function (r) {
          return r.json().then(function (j) { return {ok: r.ok, j: j}; });
        })
        .then(function (res) {
          if (!res.ok) {
            btn.disabled = false;
            out.textContent = "reboot failed: " +
              (res.j && (res.j.error || res.j.message) || "unknown error");
            return;
          }
          var t0 = Date.now();
          out.textContent = (res.j && res.j.note) ?
            String(res.j.note) : "acknowledged — device is rebooting…";
          var grace = opts.graceMs || 0;
          // watch for it to answer again (cheap cache endpoint proves the
          // hub can still route; a fresh INFO would need another report)
          (function poll() {
            if (Date.now() - t0 < grace) { setTimeout(poll, 4000); return; }
            if (Date.now() - t0 > 180000) {
              btn.disabled = false;
              out.textContent += "\nstill not back after 3 min — check the device";
              return;
            }
            fetchT(probeUrl, null, 12000)
              .then(function (r) {
                if (r.ok) {
                  btn.disabled = false;
                  out.textContent = "device is back (answered a live read " +
                    Math.round((Date.now() - t0) / 1000) + "s after reboot)";
                } else { setTimeout(poll, 8000); }
              })
              .catch(function () { setTimeout(poll, 8000); });
          })();
        })
        .catch(function (e) {
          btn.disabled = false;
          out.textContent = "reboot failed: " + (e && e.message || e);
        });
    };
  }

  function viewSensorEvents(dev) {
    setDeviceHeader("dev", dev, "#/sevents/");
    app.innerHTML = '<div class="empty">loading…</div>';
    api("/api/v1/devices/" + encodeURIComponent(dev)).then(function (d) {
      var caps = d.capabilities;
      if (typeof caps === "string") {
        try { caps = JSON.parse(caps); } catch (e) { caps = []; }
      }
      if (!caps || !caps.length) {
        app.innerHTML = '<div class="empty">no config sync yet — open the ' +
                        'device card and press Sync</div>';
        mountTabs("dev", dev, "events");
        return;
      }
      infoEl.textContent = caps.length + " fields";
      app.innerHTML = "";
      var box = document.createElement("div");
      box.className = "card sensor-card sensor-page";
      var body = document.createElement("div");
      body.className = "sensor-rows";
      box.appendChild(body);
      app.appendChild(box);
      mountTabs("dev", dev, "events");

      var rows = [];
      caps.forEach(function (f) {
        var row = sensorRow(dev, f);
        rows.push(row);
        body.appendChild(row.el);
      });
      // cache first + bounded-parallel live reads, then live-cache polling
      loadRowValues(dev, rows);
      var timer = watchFieldCache(dev, rows);
      teardown = function () { clearInterval(timer); };
    }).catch(function (e) {
      app.innerHTML = '<div class="empty err">' + esc(e.message) + "</div>";
    });
  }

  // ── sensor Settings: which controls the card face shows ───────────────
  function viewSensorSettings(dev) {
    setDeviceHeader("dev", dev, "#/ssettings/");
    app.innerHTML = '<div class="empty">loading…</div>';
    Promise.all([
      api("/api/v1/devices/" + encodeURIComponent(dev)),
      api("/api/v1/devices/" + encodeURIComponent(dev) + "/card-config")
    ]).then(function (rs) {
      var d = rs[0], cfg = rs[1];
      var caps = d.capabilities;
      if (typeof caps === "string") {
        try { caps = JSON.parse(caps); } catch (e) { caps = []; }
      }
      if (!caps || !caps.length) {
        app.innerHTML = '<div class="empty">no config sync yet</div>';
        return;
      }
      var hidden = {};
      (cfg.hidden || []).forEach(function (n) { hidden[n] = true; });

      app.innerHTML =
        '<div class="setting"><div class="meta">' +
        '<h3>Card controls</h3><p>Choose which inputs/outputs appear on ' +
        "this device's card on the Devices page.  Everything stays " +
        'visible in Events regardless.</p></div></div>';
      mountTabs("dev", dev, "control");
      var box = document.createElement("div");
      box.className = "card sensor-card sensor-page";
      var body = document.createElement("div");
      body.className = "sensor-rows";
      box.appendChild(body);
      app.appendChild(box);

      function save() {
        var list = Object.keys(hidden).filter(function (k) { return hidden[k]; });
        fetch("/api/v1/devices/" + encodeURIComponent(dev) + "/card-config", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ hidden: list })
        }).then(function () {
          infoEl.textContent = "saved";
        }).catch(function () { infoEl.textContent = "save failed"; });
      }

      var hd = document.createElement("div");
      hd.className = "srow cardcfg cardcfg-head";
      hd.innerHTML = '<span class="slabel">field</span>' +
                     '<span class="sval">access</span>' +
                     '<span class="sctl">show on card</span>';
      body.appendChild(hd);
      caps.forEach(function (f) {
        var el = document.createElement("div");
        el.className = "srow cardcfg";
        el.innerHTML =
          '<span class="slabel" title="' + esc(f.n) + '">' + esc(fieldLabel(f.n)) + "</span>" +
          '<span class="sval">' + (f.t === 1 ? "writable" : "read-only") +
          "</span>";
        var ctl = document.createElement("span");
        ctl.className = "sctl";
        var sw = document.createElement("label");
        sw.className = "switch";
        var input = document.createElement("input");
        input.type = "checkbox";
        input.checked = !hidden[f.n];
        var knob = document.createElement("span");
        knob.className = "slider-knob";
        sw.appendChild(input); sw.appendChild(knob);
        ctl.appendChild(sw);
        el.appendChild(ctl);
        input.addEventListener("change", function () {
          hidden[f.n] = !input.checked;
          save();
        });
        body.appendChild(el);
      });
    }).catch(function (e) {
      app.innerHTML = '<div class="empty err">' + esc(e.message) + "</div>";
    });
  }

  // ── firmware panel (device Settings): versions, download, schedule ──────
  function renderFirmwarePanel(root, dev, d) {
    var box = document.createElement("div");
    box.className = "setting fwpanel";
    box.innerHTML =
      '<div class="meta"><h3>Firmware</h3>' +
      '<p class="fw-info">loading…</p></div>' +
      '<div class="fw-ctl">' +
      '<select class="fw-sel sselect"><option>loading…</option></select>' +
      '<button class="btn fw-dl" title="Download to device storage (arms, does not apply)">Download</button>' +
      '<button class="btn fw-go" title="Schedule the OTA apply">Schedule…</button>' +
      "</div>" +
      '<div class="fw-sched-pop" hidden>' +
      '<label>Apply at <input type="datetime-local" class="fw-when"></label>' +
      '<button class="btn primary fw-ok">Schedule</button>' +
      '<button class="btn fw-cancel">Cancel</button>' +
      "</div>";
    root.appendChild(box);

    var info = box.querySelector(".fw-info");
    var sel = box.querySelector(".fw-sel");
    var pop = box.querySelector(".fw-sched-pop");
    var whenInput = box.querySelector(".fw-when");
    var pollTimer = null;

    function fmtWhen(unix) {
      return new Date(unix * 1000).toLocaleString();
    }

    function refresh() {
      Promise.all([
        api("/api/v1/devices/" + encodeURIComponent(dev) + "/ota").catch(function () { return {}; }),
        api("/api/v1/devices/" + encodeURIComponent(dev) + "/ota/schedule").catch(function () { return {}; })
      ]).then(function (rs) {
        var ota = rs[0] || {}, sched = rs[1] || {};
        var cur = ota.running_version || "unknown";
        var dl = ota.armed ? (ota.armed_version + " (armed, ready to apply)")
               : (ota.state === "downloading"
                  ? ((ota.target_version || "?") + " downloading " +
                     Math.round(ota.progress_percent || 0) + "%")
                  : "none");
        var lines = "current: <b>" + esc(cur) + "</b>" +
                    (d && d.image ? ' · <span class="fw-image">' + esc(d.image) + "</span>" : "") +
                    "<br>downloaded: " + esc(dl);
        if (sched.at) {
          lines += "<br>scheduled OTA: <b>" + esc(sched.version) + "</b> at " +
                   esc(fmtWhen(sched.at)) +
                   ' <a href="#" class="fw-unsched">cancel</a>';
        }
        info.innerHTML = lines;
        var un = info.querySelector(".fw-unsched");
        if (un) un.onclick = function (ev) {
          ev.preventDefault();
          fetch("/api/v1/devices/" + encodeURIComponent(dev) + "/ota/schedule",
                { method: "DELETE" }).then(refresh);
        };
        // keep polling while a download is in flight
        if (ota.state === "downloading" && !pollTimer) {
          pollTimer = setInterval(refresh, 5000);
        } else if (ota.state !== "downloading" && pollTimer) {
          clearInterval(pollTimer); pollTimer = null;
        }
      });
    }

    // dropdown: catalog entries for THIS device's type, minus what it runs
    api("/api/v1/firmware/catalog?device_type=" +
        encodeURIComponent(d.image || d.device_type || d.type || "")).then(function (es) {
      api("/api/v1/devices/" + encodeURIComponent(dev) + "/ota")
        .catch(function () { return {}; })
        .then(function (ota) {
          var running = String(ota.running_version || "").split("+")[0];
          sel.innerHTML = "";
          var usable = (es || []).filter(function (e) {
            return e.version !== running;
          });
          if (!usable.length) {
            sel.innerHTML = "<option>no other versions</option>";
            sel.disabled = true;
            return;
          }
          usable.forEach(function (e) {
            var o = document.createElement("option");
            o.value = e.version;
            o.textContent = e.version + "  (" + e.source_name + ")";
            sel.appendChild(o);
          });
        });
    }).catch(function () {
      sel.innerHTML = "<option>catalog unavailable</option>";
      sel.disabled = true;
    });

    box.querySelector(".btn.fw-dl").onclick = function () {
      if (sel.disabled) return;
      var v = sel.value;
      info.innerHTML = "starting download of " + esc(v) + "…";
      fetch("/api/v1/devices/" + encodeURIComponent(dev) + "/ota/download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ version: v })
      }).then(function (r) { return r.json(); })
        .then(function (j) {
          if (j.err) info.innerHTML = '<span class="err">' + esc(j.err) + "</span>";
          setTimeout(refresh, 1500);
        })
        .catch(function (e) { info.textContent = "download request failed: " + e; });
    };

    box.querySelector(".btn.fw-go").onclick = function () {
      if (sel.disabled) return;
      // default: 5 minutes from now, in the input's local format
      var t = new Date(Date.now() + 5 * 60000);
      t.setSeconds(0, 0);
      whenInput.value = new Date(t.getTime() - t.getTimezoneOffset() * 60000)
                          .toISOString().slice(0, 16);
      pop.hidden = false;
    };
    box.querySelector(".btn.fw-cancel").onclick = function () { pop.hidden = true; };
    box.querySelector(".btn.fw-ok").onclick = function () {
      var at = Math.floor(new Date(whenInput.value).getTime() / 1000);
      if (!at || isNaN(at)) return;
      pop.hidden = true;
      fetch("/api/v1/devices/" + encodeURIComponent(dev) + "/ota/schedule", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ version: sel.value, at: at })
      }).then(refresh);
    };

    refresh();
    var old = teardown;
    teardown = function () {
      if (old) old();
      if (pollTimer) clearInterval(pollTimer);
    };
  }

  // Machine field names -> operator labels ("button_v" is firmware
  // naming, not a label).  Raw name stays in the tooltip.
  var fieldLabel = NN.fieldLabel;

  function sensorRow(dev, f) {
    var writable = f.t === 1;
    var min = typeof f.min === "number" ? f.min : 0;
    var max = typeof f.max === "number" ? f.max : 1;
    var isBinary = min === 0 && max === 1;
    var span = max - min;
    var isChoice = !isBinary && span > 0 && span <= 6 &&
                   Number.isInteger(min) && Number.isInteger(max);

    var el = document.createElement("div");
    el.className = "srow" + (writable ? "" : " ro");
    var lbl = document.createElement("span");
    lbl.className = "slabel";
    lbl.textContent = fieldLabel(f.n);
    lbl.title = f.n;
    el.appendChild(lbl);

    var val = document.createElement("span");
    val.className = "sval";
    val.textContent = "…";

    var ctl = document.createElement("span");
    ctl.className = "sctl";
    var input, setVal, getVal;

    if (isBinary) {
      var swWrap = document.createElement("label");
      swWrap.className = "switch";
      input = document.createElement("input");
      input.type = "checkbox";
      var knob = document.createElement("span");
      knob.className = "slider-knob";
      swWrap.appendChild(input); swWrap.appendChild(knob);
      ctl.appendChild(swWrap);
      setVal = function (v) { input.checked = !!(+v); val.textContent = (+v) ? "on" : "off"; val.classList.toggle("dim", !(+v)); };
      getVal = function () { return input.checked ? 1 : 0; };
    } else if (isChoice) {
      input = document.createElement("select");
      input.className = "sselect";
      for (var v = min; v <= max; v++) {
        var o = document.createElement("option");
        o.value = v; o.textContent = v;
        input.appendChild(o);
      }
      ctl.appendChild(input);
      setVal = function (v) { input.value = String(Math.round(+v)); val.textContent = fmtNum(v); };
      getVal = function () { return +input.value; };
    } else {
      input = document.createElement("input");
      input.type = "range";
      input.className = "srange";
      input.min = min; input.max = max;
      input.step = span > 20 ? (span / 100) : 0.5;
      ctl.appendChild(input);
      // live preview while dragging; commit on release
      input.addEventListener("input", function () {
        val.textContent = fmtNum(input.value);
      });
      setVal = function (v) { input.value = +v; val.textContent = fmtNum(v); };
      getVal = function () { return +input.value; };
    }

    // Writable widgets are interactive IMMEDIATELY — gating them on the
    // initial value read froze the whole card whenever one field's read
    // stalled on a lossy mesh link.  Read-only widgets stay disabled.
    input.disabled = !writable;

    // ── desired state ──────────────────────────────────────────────
    // The control shows only what the DEVICE reported; the word next to
    // it shows what was asked for until the device confirms it.  The
    // hub keeps the desired state (it survives a refresh and is the same
    // for every viewer, and automation cascades set it too); `pend` is
    // this row's copy, with a LOCAL start time so browser/hub clock skew
    // cannot age it.
    var real = null;          // last value the device reported
    var pend = null;          // {d, at (hub epoch), t0 (local ms)}
    function render() {
      if (real !== null) setVal(real);         // the control = device truth
      else if (pend && isBinary) input.checked = !(+pend.d);   // unknown: undo the click
      if (!pend) { el.classList.remove("pending", "late"); return; }
      var age = (Date.now() - pend.t0) / 1000;
      val.textContent = NN.pendingText(pend.d, age, isBinary, fmtNum);
      val.classList.remove("dim");
      el.classList.add("pending");
      el.classList.toggle("late", age >= NN.PEND_SLOW_S);
    }

    if (writable) {
      input.addEventListener("change", function () {
        var v = getVal();
        pend = { d: v, at: "local", t0: Date.now() };
        render();                 // switch snaps back; the word says "turning on…"
        putField(dev, f.n, v).then(function (j) {
          // the device answered: that IS its report
          real = +j.value; pend = null; render();
        }).catch(function (e) {
          if (e && e.status === 400) {             // device refused the value
            pend = null; render();
            val.textContent = "refused";
            return;
          }
          // timeout / relay error: the write may still land (the hub and
          // its failsafe keep trying) -- stay pending, the cache decides
        });
      });
    }

    el.appendChild(ctl);
    el.appendChild(val);

    return {
      el: el,
      field: f.n,
      setValue: function (v) {
        if (v === null || v === undefined) v = min;
        real = +v;
        render();
        input.disabled = !writable;   // read-only stays disabled
      },
      setError: function () {
        if (!pend) val.textContent = "—";
        input.disabled = !writable;
      },
      // One entry of the hub's field cache: {v, ts} and, while a write or
      // an automation is pending, {desired, desired_at, desired_by}.
      applyLive: function (e) {
        if (!e) return;
        if (input.type === "range" && document.activeElement === input) return; // mid-drag
        if (typeof e.v === "number") real = e.v;
        pend = NN.nextPending(pend, real, e, Date.now());
        render();
      },
      isPending: function () { return !!pend; },
      tick: render                       // re-age the pending text
    };
  }

  function fmtNum(v) {
    var n = +v;
    if (isNaN(n)) return String(v);
    return (Math.round(n * 10) / 10).toString();
  }

  // ── automation editor ─────────────────────────────────────────────────────
  // Two columns: Helper (form that appends rule YAML into the editor) and
  // Raw (the automations.yaml working copy).  Save & Compile POSTs the text
  // to /api/v1/auto/compile; a console at the bottom logs compile results,
  // and Push sends the compiled configs to the devices.

  var AUTO_OPS = ["above", "below", "equals", "not_equals"];

  var AUTOMATION_TABS = [["rules", "Modify rules"], ["devices", "On devices"]];

  function viewAutomation(tab) {
    tab = tab || "rules";
    setHeader("Automation", false);
    app.innerHTML = '<nav class="dtabs">' + AUTOMATION_TABS.map(function (t) {
      return '<a class="dtab' + (t[0] === tab ? " on" : "") +
             '" href="#/automation/' + t[0] + '">' + t[1] + "</a>";
    }).join("") + '</nav><div id="auto-host"><div class="empty">loading…</div></div>';
    var host = document.getElementById("auto-host");
    if (tab === "devices") { autoDevices(host); return; }
    Promise.all([
      api("/api/v1/auto/yaml").catch(function () { return { yaml: "" }; }),
      api("/api/v1/devices").catch(function () { return []; })
    ]).then(function (res) {
      renderAutomation(host, res[0].yaml || "", res[1] || []);
    }).catch(function (e) {
      host.innerHTML = '<div class="empty err">failed to load: ' + esc(e.message) + "</div>";
    });
  }

  // What each device is ACTUALLY running: the compiled config the hub
  // stored at the last successful push (version bumps on every compile).
  function autoDevices(host) {
    api("/api/v1/devices").then(function (devs) {
      devs = devs || [];
      if (!devs.length) {
        host.innerHTML = '<div class="empty">no devices</div>';
        return;
      }
      return Promise.all(devs.map(function (d) {
        var id = d.name || d.id;
        return api("/api/v1/auto/" + encodeURIComponent(id))
          .then(function (c) { return {name: id, cfg: c}; })
          .catch(function () { return {name: id, cfg: null}; });
      })).then(function (rows) {
        host.innerHTML = '<div class="setting"><div class="meta">' +
          '<h3>Compiled rules on devices</h3><p>The rule set stored for ' +
          'each device from the last compile.  A device only runs it after ' +
          'a push — "none" means this device has never had rules ' +
          'compiled for it.</p></div></div>' +
          '<div class="setting"><div class="meta"><div class="fleet-tbl">' +
          '<div class="fleet-row fleet-head"><span>device</span>' +
          '<span>rules version</span><span>compiled</span><span>size</span>' +
          '<span></span></div>' +
          rows.map(function (x) {
            if (!x.cfg) {
              return '<div class="fleet-row"><span>' + esc(x.name) + "</span>" +
                '<span class="mono">—</span><span class="mono">—</span>' +
                '<span class="mono">—</span>' +
                '<span class="fleet-st dim">none</span></div>';
            }
            var when = x.cfg.updated_at ?
              new Date(x.cfg.updated_at * 1000).toLocaleString() : "—";
            var blob = (x.cfg.payload && x.cfg.payload.auto_bin) || "";
            var bytes = Math.round(blob.length * 3 / 4);
            return '<div class="fleet-row"><span>' + esc(x.name) + "</span>" +
              '<span class="mono">v' + esc(String(x.cfg.version)) + "</span>" +
              '<span class="mono">' + esc(when) + "</span>" +
              '<span class="mono">' + bytes + " B</span>" +
              '<span class="fleet-st ok">compiled</span></div>';
          }).join("") + "</div></div></div>";
      });
    }).catch(function (e) {
      host.innerHTML = '<div class="empty err">failed to load: ' + esc(e.message) + "</div>";
    });
  }

  function renderAutomation(host, yamlText, devs) {
    infoEl.textContent = "";
    host.innerHTML =
      '<div class="auto-wrap">' +
      '  <div class="auto-col helper">' +
      '    <div class="auto-col-title">Helper <small>builds one rule, then adds it to the YAML on the right</small></div>' +
      '    <div class="auto-form">' +
      '      <label class="afield"><span>Rule id</span>' +
      '        <input type="text" id="ah-id" class="atext" placeholder="my_rule"></label>' +
      '      <div class="agroup">Trigger</div>' +
      '      <label class="afield"><span>Device</span><select id="ah-tdev" class="sselect"></select></label>' +
      '      <label class="afield"><span>Field</span><select id="ah-tfield" class="sselect"></select></label>' +
      '      <label class="afield"><span>Condition</span><select id="ah-top" class="sselect"></select></label>' +
      '      <label class="afield"><span>Threshold</span>' +
      '        <input type="number" id="ah-tval" class="atext" value="0.5" step="any"></label>' +
      '      <div class="agroup">Actions</div>' +
      '      <div id="ah-actions"></div>' +
      '      <button class="btn" id="ah-addact">+ action</button>' +
      '      <div class="auto-form-foot">' +
      '        <button class="btn primary" id="ah-add">Add rule to editor</button>' +
      '      </div>' +
      '    </div>' +
      '  </div>' +
      '  <div class="auto-col raw">' +
      '    <div class="auto-col-title">Raw <small>automations.yaml — what actually runs on the fleet</small></div>' +
      '    <textarea id="auto-editor" class="auto-editor" spellcheck="false" ' +
      '     placeholder="automations: []"></textarea>' +
      '    <div class="auto-actions">' +
      '      <button class="btn primary" id="auto-save">Save &amp; Compile</button>' +
      '      <button class="btn" id="auto-push" disabled title="Save &amp; Compile first — push sends the last compiled rules">Push to devices</button>' +
      '    </div>' +
      '  </div>' +
      '</div>' +
      '<div class="auto-console-wrap">' +
      '  <div class="auto-col-title">Console</div>' +
      '  <div id="auto-console" class="auto-console" data-empty="compile output appears here"></div>' +
      '</div>';

    var editor = document.getElementById("auto-editor");
    var consoleEl = document.getElementById("auto-console");
    var saveBtn = document.getElementById("auto-save");
    var pushBtn = document.getElementById("auto-push");
    editor.value = yamlText;

    var lastCompiled = null;   // device names from the last good compile

    function clog(msg, cls) {
      var line = document.createElement("div");
      line.className = "cline " + (cls || "");
      var ts = new Date().toTimeString().slice(0, 8);
      line.textContent = "[" + ts + "] " + msg;
      consoleEl.appendChild(line);
      consoleEl.scrollTop = consoleEl.scrollHeight;
    }

    // ── helper column ──
    var tdev = document.getElementById("ah-tdev");
    var tfield = document.getElementById("ah-tfield");
    var topSel = document.getElementById("ah-top");
    var actsEl = document.getElementById("ah-actions");

    function caps(d) {
      var c = d.capabilities;
      if (typeof c === "string") { try { c = JSON.parse(c); } catch (e) { c = []; } }
      return c || [];
    }
    function fillDevices(sel) {
      sel.innerHTML = "";
      devs.forEach(function (d) {
        var o = document.createElement("option");
        o.value = d.name; o.textContent = d.name;
        sel.appendChild(o);
      });
    }
    function fillFields(sel, devName, writableOnly) {
      sel.innerHTML = "";
      var d = devs.filter(function (x) { return x.name === devName; })[0];
      (d ? caps(d) : []).forEach(function (f) {
        if (writableOnly && f.t !== 1) return;
        var o = document.createElement("option");
        o.value = f.n; o.textContent = f.n + (f.t === 1 ? "" : " (ro)");
        sel.appendChild(o);
      });
      if (!sel.options.length) {
        var o = document.createElement("option");
        o.value = ""; o.textContent = "(no config sync)";
        sel.appendChild(o);
      }
    }
    AUTO_OPS.forEach(function (op) {
      var o = document.createElement("option");
      o.value = op; o.textContent = op.replace("_", " ");
      topSel.appendChild(o);
    });
    fillDevices(tdev);
    fillFields(tfield, tdev.value, false);
    tdev.onchange = function () { fillFields(tfield, tdev.value, false); };

    function addActionRow() {
      var row = document.createElement("div");
      row.className = "arow";
      var adev = document.createElement("select"); adev.className = "sselect";
      var afield = document.createElement("select"); afield.className = "sselect";
      var aval = document.createElement("input");
      aval.type = "number"; aval.className = "atext aval"; aval.value = "1"; aval.step = "any";
      var del = document.createElement("button");
      del.className = "btn adel"; del.textContent = "✕";
      del.onclick = function () { row.remove(); };
      fillDevices(adev);
      fillFields(afield, adev.value, true);
      adev.onchange = function () { fillFields(afield, adev.value, true); };
      row.appendChild(adev); row.appendChild(afield); row.appendChild(aval);
      row.appendChild(del);
      actsEl.appendChild(row);
    }
    addActionRow();
    document.getElementById("ah-addact").onclick = addActionRow;

    document.getElementById("ah-add").onclick = function () {
      var id = document.getElementById("ah-id").value.trim() ||
               "rule_" + Date.now().toString(36);
      var op = topSel.value;
      var thr = document.getElementById("ah-tval").value || "0";
      if (!tfield.value) { clog("trigger device has no fields — Sync it on the Devices page first", "err"); return; }
      var y = "";
      // Seed the document if the editor is empty / has no automations key.
      if (!/^\s*automations\s*:/m.test(editor.value)) {
        y += (editor.value.trim() ? "\n" : "") + "automations:\n";
      }
      y += "  - id: " + id + "\n" +
           "    trigger:\n" +
           "      - device: " + tdev.value + "\n" +
           "        field: " + tfield.value + "\n" +
           "        " + op + ": " + thr + "\n" +
           "    action:\n";
      var rows = actsEl.querySelectorAll(".arow");
      var ok = true;
      [].forEach.call(rows, function (row) {
        var sels = row.querySelectorAll("select");
        var val = row.querySelector(".aval").value || "0";
        if (!sels[1].value) { ok = false; return; }
        y += "      - device: " + sels[0].value + "\n" +
             "        field: " + sels[1].value + "\n" +
             "        value: " + val + "\n";
      });
      if (!ok) { clog("an action device has no writable fields", "err"); return; }
      if (!rows.length) { clog("add at least one action", "err"); return; }
      editor.value = editor.value.replace(/\s*$/, "") + "\n" + y;
      editor.scrollTop = editor.scrollHeight;
      clog("rule '" + id + "' added to editor — not saved yet");
    };

    // ── save & compile ──
    saveBtn.onclick = function () {
      saveBtn.disabled = true;
      clog("compiling…");
      fetch("/api/v1/auto/compile", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ yaml: editor.value })
      }).then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
        .then(function (res) {
          saveBtn.disabled = false;
          if (!res.ok) {
            lastCompiled = null;
            pushBtn.disabled = true;
            clog("error: " + (res.j.err || "compile failed"), "err");
            return;
          }
          var per = res.j.compiled || {};
          var names = Object.keys(per);
          lastCompiled = names;
          pushBtn.disabled = !names.length;
          if (!names.length) {
            clog("updated: compiled clean — no device configs (empty ruleset)", "ok");
            return;
          }
          var parts = names.map(function (n) {
            var p = per[n];
            return n + " (" + p.triggers + "t/" + p.conditions + "c/" +
                   p.actions + "a, " + p.binary_bytes + "B)";
          });
          clog("updated: " + res.j.device_count + " device(s) — " +
               parts.join(", "), "ok");
        })
        .catch(function (e) {
          saveBtn.disabled = false;
          lastCompiled = null;
          pushBtn.disabled = true;
          clog("error: " + e.message, "err");
        });
    };

    // ── push ──
    pushBtn.onclick = function () {
      if (!lastCompiled || !lastCompiled.length) return;
      pushBtn.disabled = true;
      var i = 0;
      (function next() {
        if (i >= lastCompiled.length) {
          pushBtn.disabled = false;
          clog("push complete");
          return;
        }
        var dev = lastCompiled[i++];
        clog("pushing to " + dev + "…");
        fetch("/api/v1/auto/" + encodeURIComponent(dev) + "/push",
              { method: "POST" })
          .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
          .then(function (res) {
            if (res.ok && res.j.status === 0) {
              clog("✓ " + dev + ": v" + res.j.version + " " +
                   res.j.blob_bytes + "B applied", "ok");
            } else {
              clog("✗ " + dev + ": " +
                   (res.j.err || ("status=" + res.j.status)), "err");
            }
            next();
          })
          .catch(function (e) { clog("✗ " + dev + ": " + e.message, "err"); next(); });
      })();
    };
  }

  // The API's `status` is the one word that is actually true for the slot:
  //   streaming  video is flowing
  //   no_video   a camera is connected but video is not flowing
  //   no_camera  the media-host is up but NO camera is connected to it
  //   checking   hub just started, no reading yet
  //   offline    the media-host itself is not answering
  // `online` alone was the media-host's heartbeat, not the camera's: a slot
  // whose service ran for weeks with no camera ever connecting showed a green
  // "online" while its player reconnect-looped (cam0, Sept 2026, unpowered).
  var STATUS_PILL = {
    streaming: ["● online",     "online",   "video is flowing"],
    no_video:  ["● no video",   "novideo",  "camera connected, but no new video is arriving"],
    no_camera: ["● no camera",  "nocamera", "media server is up, but no camera is connected to it — check the camera's power and Wi-Fi"],
    checking:  ["● checking…",  "checking", "hub just started; waiting for the first reading"],
    offline:   ["● offline",    "offline",  "the media server for this slot is not answering"]
  };
  function statusOf(c) {
    if (c && c.status && STATUS_PILL[c.status]) return c.status;
    // older API without `status`: keep the previous behaviour
    if (!c || !c.online) return "offline";
    if (c.streaming === false) return "no_video";
    return "streaming";
  }
  function setStatus(card, online, streaming, c) {
    var st = card.querySelector(".status");
    if (!st) return;
    var key = statusOf(c || {online: online, streaming: streaming});
    var p = STATUS_PILL[key];
    st.textContent = p[0];
    st.className = "status " + p[1];
    st.title = p[2];
  }

  function cardFor(c) {
    var card = document.createElement("div");
    card.className = "card";
    card.setAttribute("data-cam", c.id);
    card.innerHTML =
      '<a class="thumb" href="#/live/' + encodeURIComponent(c.id) + '">' +
      '<div class="noimg">no snapshot</div>' +
      '<img data-src="' + esc(c.snapshot_url) + '" alt="" ' +
      "onload=\"this.previousElementSibling.style.display='none'\" " +
      "onerror=\"this.style.display='none';this.previousElementSibling.style.display='flex'\">" +
      "</a>" +
      '<div class="namerow">' +
      '<span class="name">' + esc(c.name) + "</span>" +
      '<span class="status"></span>' +
      "</div>" +
      '<div class="sub">' + esc(c.id) + "</div>" +
      '<div class="actions">' +
      // no "Live view" button: the thumbnail is already the link, and two
      // controls doing the same thing just crowd the card
      '<button class="btn events">Events</button>' +
      '<button class="btn infer">Settings</button>' +
      "</div>";
    setStatus(card, c.online, c.streaming, c);
    card.querySelector(".btn.events").onclick = function () {
      location.hash = "#/events/" + encodeURIComponent(c.id);
    };
    card.querySelector(".btn.infer").onclick = function () {
      location.hash = "#/infer/" + encodeURIComponent(c.id);
    };
    return card;
  }

  // ── per-camera inference policy ───────────────────────────────────────────
  // Streaming tab: how the camera behaves UNDER CONGESTION (keep motion vs
  // keep quality, and the level within that choice).
  function viewCamStreaming(camId) {
    setDeviceHeader("cam", camId, "#/stream/");
    app.innerHTML = "";
    mountTabs("cam", camId, "streaming");
    renderStreamPolicy(app, camId);
  }

  // Pipelines tab: the media host runs every camera in ONE process as six
  // pipelines; here each one shows its health for this camera and its
  // settings, which the hub writes to the media host (live on the next
  // request, no restart).
  var PIPE_HELP = {
    ingest:  "records from the camera, ordered by device time",
    stream:  "the GStreamer graph: HLS, live view, snapshots",
    detect:  "motion detection on decoded frames",
    event:   "motion-gated recording and upload",
    control: "adaptive bitrate and device config over the back-channel",
    hub:     "registration heartbeat and policy fetch"
  };
  var PIPE_HIDDEN = {enabled: 1, workers: 1, keydir: 1, legacy_ingest_port: 1, legacy_control_port: 1,
                     ingest_port: 1, host_key_id: 1, hls_dir: 1, snapshot_dir: 1, state_dir: 1, hub_url: 1};

  function viewCamPipelines(camId) {
    setDeviceHeader("cam", camId, "#/pipes/");
    app.innerHTML = "";
    mountTabs("cam", camId, "pipelines");
    var root = document.createElement("div");
    root.className = "pipes";
    root.innerHTML = '<p class="empty">loading…</p>';
    app.appendChild(root);
    var base = "/api/v1/cameras/" + encodeURIComponent(camId);

    function fmtState(st) {
      if (!st) return '<span class="muted">no runs yet</span>';
      var bad = (st.failures_recent || st.stalled);
      return '<span class="' + (bad ? "bad" : "ok") + '">' +
        "runs " + st.runs + " · failures " + st.failures_recent + " · stalls " + st.stalls +
        " · resets " + st.resets + (st.last_latency_ms != null ? " · last " + st.last_latency_ms + " ms" : "") +
        (st.last_error ? ' · <span class="bad">' + esc(String(st.last_error).slice(0, 80)) + "</span>" : "") +
        "</span>";
    }

    function inputFor(key, val) {
      var t = typeof val;
      if (t === "boolean") return '<input type="checkbox" data-key="' + esc(key) + '" data-type="bool"' + (val ? " checked" : "") + ">";
      if (t === "number") return '<input type="number" step="any" data-key="' + esc(key) + '" data-type="num" value="' + esc(String(val)) + '">';
      return '<input type="text" data-key="' + esc(key) + '" data-type="str" value="' + esc(val == null ? "" : String(val)) + '">';
    }

    function render(d) {
      root.innerHTML = "";
      var head = document.createElement("div");
      head.className = "setting";
      var conn = d.connection;
      head.innerHTML = '<div class="meta"><h3>Media host</h3><p>' + esc(d.media_host || "") +
        " · up " + Math.round((d.uptime_s || 0) / 3600) + " h · " + Math.round((d.rss_kb || 0) / 1024) + " MB" +
        (conn ? " · connected from " + esc(String(conn.peer || "")) + " for " + Math.round((conn.age_s || 0) / 60) + " min"
              : ' · <span class="bad">camera not connected</span>') + "</p></div>";
      root.appendChild(head);
      Object.keys(d.pipelines || {}).forEach(function (name) {
        var p = d.pipelines[name];
        var box = document.createElement("div");
        box.className = "setting fwpanel pipe";
        var keys = Object.keys(p.settings || {}).filter(function (k) { return !PIPE_HIDDEN[k]; }).sort();
        box.innerHTML =
          '<div class="meta"><h3>' + esc(name) + ' <small class="muted">' + esc(PIPE_HELP[name] || "") + "</small></h3>" +
          "<p>" + fmtState(p.state) + " · queue depth " + (p.queue.depth || 0) + ", dropped " + (p.queue.dropped || 0) +
          ", wait " + (p.queue.avg_wait_ms || 0) + " ms · " + esc(p.mode || "") + ", " + (p.workers || 1) + " worker(s)</p></div>" +
          '<div class="pipe-form">' +
            keys.map(function (k) {
              return '<label class="pipe-row"><span>' + esc(k) + "</span>" + inputFor(k, p.settings[k]) + "</label>";
            }).join("") +
            '<div class="pipe-actions"><button class="btn pipe-save">Save</button>' +
            '<button class="btn pipe-reset" title="rebuild this camera\'s state in this pipeline">Reset</button>' +
            '<span class="pipe-msg muted"></span></div>' +
          "</div>";
        root.appendChild(box);
        var msg = box.querySelector(".pipe-msg");
        box.querySelector(".pipe-save").onclick = function () {
          var changed = {};
          [].forEach.call(box.querySelectorAll("input[data-key]"), function (inp) {
            var k = inp.getAttribute("data-key"), t = inp.getAttribute("data-type"), was = p.settings[k];
            var v = t === "bool" ? inp.checked : t === "num" ? Number(inp.value) : inp.value;
            if (t === "num" && isNaN(v)) return;
            if (v !== was) changed[k] = v;
          });
          if (!Object.keys(changed).length) { msg.textContent = "nothing changed"; return; }
          msg.textContent = "saving…";
          fetch(base + "/pipeline-settings/" + encodeURIComponent(name), {
            method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(changed)
          }).then(function (r) { return r.json().then(function (j) { return [r.ok, j]; }); })
            .then(function (x) {
              msg.textContent = x[0] ? "saved (version " + x[1].version + "), live on the next request" : "failed: " + (x[1].err || x[1].error || "");
              if (x[0]) load();
            }).catch(function (e) { msg.textContent = "failed: " + e; });
        };
        box.querySelector(".pipe-reset").onclick = function () {
          if (!confirm("Reset " + name + " for " + camId + "? Its state (graph, detector, engine or session controller) is rebuilt on the next request.")) return;
          msg.textContent = "resetting…";
          fetch(base + "/pipeline-reset/" + encodeURIComponent(name), {method: "POST"})
            .then(function (r) { return r.json(); })
            .then(function (j) { msg.textContent = j.ok ? "reset (" + j.resets + " so far)" : "failed: " + (j.err || j.error || ""); load(); })
            .catch(function (e) { msg.textContent = "failed: " + e; });
        };
      });
    }

    function load() {
      api(base + "/pipelines").then(render).catch(function (e) {
        root.innerHTML = '<p class="empty err">media host unavailable: ' + esc(String(e.message || e)) + "</p>";
      });
    }
    load();
  }

  // Device Status tab: firmware/bundle version, what is downloaded, and the
  // scheduled OTA.
  function viewCamStatus(camId) {
    setDeviceHeader("cam", camId, "#/camstatus/");
    app.innerHTML = "";
    mountTabs("cam", camId, "status");
    renderUptime(app, function () {
      return fetchT("/api/v1/cameras/" + encodeURIComponent(camId) + "/uptime",
                    null, 8000)
        .then(function (r) { return r.json().then(function (j) {
          if (!r.ok) throw new Error(j.error || "unavailable");
          return "up " + fmtUp(j.up_s) + "  (" + j.source +
                 ", " + Math.round(j.age_s) + "s ago)";
        }); });
    });
    renderDeviceReboot(app, camId, {
      postUrl: "/api/v1/cameras/" + encodeURIComponent(camId) + "/reboot",
      probeUrl: "/api/v1/cameras/" + encodeURIComponent(camId) + "/snapshot.jpg",
      // the device drops its session with no ack; give it a moment to
      // actually go down before polling, or the old state reads as "back"
      graceMs: 15000,
      blurb: "Restarts the camera now.  The stream drops and comes back " +
             "by itself, typically within a minute.  (Cameras whose " +
             "firmware predates this command will show no effect.)"
    });
    var host = document.createElement("div");
    app.appendChild(host);
    renderCameraFirmware(host, camId);
    renderCameraGateway(host, camId);
    renderUnregister(app, camId, "cam");
  }

  function viewInfer(camId) {
    setDeviceHeader("cam", camId, "#/infer/");
    app.innerHTML = '<div class="empty">loading…</div>';
    fetch("/api/v1/cameras/" + encodeURIComponent(camId) + "/inference")
      .then(function (r) { return r.json(); })
      .then(function (d) { renderInfer(camId, d); })
      .catch(function () { app.innerHTML = '<div class="empty">unavailable</div>'; });
  }

  // ── stream strategy: what to sacrifice when the link gets tight ────────
  function renderStreamPolicy(root, camId) {
    var box = document.createElement("div");
    box.className = "setting fwpanel";
    box.innerHTML =
      '<div class="meta"><h3>When the link is congested</h3>' +
      '<p class="sp-info">loading…</p></div>' +
      '<div class="sp-ctl">' +
        '<div class="sp-modes">' +
          '<button class="btn sp-mode" data-mode="fps">Keep motion</button>' +
          '<button class="btn sp-mode" data-mode="quality">Keep quality</button>' +
        '</div>' +
        '<input type="range" class="sp-slider srange" min="0" max="5" step="1">' +
        '<div class="sp-ends"><span>smooth motion</span><span>sharp image</span></div>' +
        '<div class="sp-value"></div>' +
      "</div>";
    root.appendChild(box);

    var info = box.querySelector(".sp-info");
    var slider = box.querySelector(".sp-slider");
    var valEl = box.querySelector(".sp-value");
    var stops = [], unit = "", mode = "fps";

    function describe() {
      var v = stops[+slider.value];
      if (v === undefined) { valEl.textContent = ""; return; }
      if (unit === "fps") {
        valEl.textContent = v + " fps";
        info.innerHTML = "Holding <b>picture quality</b>: the camera drops to " +
          "<b>" + v + " fps</b> and spends the whole link on image detail.";
      } else {
        valEl.textContent = (v / 1000).toFixed(1) + " Mbps";
        info.innerHTML = "Holding <b>motion</b>: full frame rate, bitrate falls " +
          "as far as <b>" + (v / 1000).toFixed(1) + " Mbps</b> before the camera " +
          "starts dropping frames.";
      }
    }

    function paintModes() {
      var bs = box.querySelectorAll(".sp-mode");
      for (var i = 0; i < bs.length; i++) {
        bs[i].classList.toggle("on", bs[i].getAttribute("data-mode") === mode);
      }
    }

    function load() {
      api("/api/v1/cameras/" + encodeURIComponent(camId) + "/stream-policy")
        .then(function (d) {
          mode = d.mode; stops = d.stops || []; unit = d.unit;
          slider.max = String(Math.max(1, stops.length - 1));
          slider.value = String(Math.min(d.level, stops.length - 1));
          paintModes(); describe();
        })
        .catch(function () { info.textContent = "unavailable"; });
    }

    function put() {
      return fetch("/api/v1/cameras/" + encodeURIComponent(camId) + "/stream-policy", {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({mode: mode, level: +slider.value})
      });
    }

    var bs = box.querySelectorAll(".sp-mode");
    for (var i = 0; i < bs.length; i++) {
      bs[i].onclick = function () {
        var m = this.getAttribute("data-mode");
        if (mode === m) return;
        mode = m; paintModes();
        // the slider means something different per mode, so re-read its
        // stops from the hub rather than reusing the old scale
        put().then(load);
      };
    }
    slider.addEventListener("input", describe);
    slider.addEventListener("change", function () { put().then(load); });
    load();
  }

  // ── camera firmware panel: bundle version + promote (agent pulls) ──────
  // ── camera-as-gateway: capability + operator toggle ─────────────────────
  function renderCameraGateway(root, camId) {
    var box = document.createElement("div");
    box.className = "setting";
    box.innerHTML = '<div class="meta"><h3>Gateway</h3>' +
      '<p class="gw-info">loading…</p></div>' +
      '<div class="switch"><label class="switch"><input type="checkbox" class="gw-en" disabled>' +
      '<span class="slider-knob"></span></label></div>';
    root.appendChild(box);
    var info = box.querySelector(".gw-info");
    var en = box.querySelector(".gw-en");
    function refresh() {
      api("/api/v1/cameras/" + encodeURIComponent(camId) + "/gateway").then(function (g) {
        en.checked = !!g.enabled;
        // The toggle is the OPERATOR'S intent; it stays usable even while
        // unsupported so a deliberate "off" survives adding an NCP later.
        en.disabled = false;
        info.innerHTML = g.supported
          ? 'This camera can host the Thread gateway (NCP detected).' +
            (g.enabled ? " Role <b>enabled</b>." : " Role <b>disabled</b> by operator.")
          : 'Not supported: ' + esc(g.reason || "unknown") + "." +
            (g.has_creds
              ? ' Gateway identity is stored (dormant) — enabling needs no re-provision.'
              : ' No gateway identity stored yet — it is delivered at the next provision.');
      }).catch(function () { info.textContent = "gateway state unavailable"; });
    }
    en.addEventListener("change", function () {
      fetchT("/api/v1/cameras/" + encodeURIComponent(camId) + "/gateway",
             {method: "PUT", headers: {"Content-Type": "application/json"},
              body: JSON.stringify({enabled: en.checked})}, 15000)
        .then(refresh)
        .catch(function () { en.checked = !en.checked; });
    });
    refresh();
  }

  function renderCameraFirmware(root, camId) {
    api("/api/v1/cameras/" + encodeURIComponent(camId) + "/bundle")
      .then(function (b) {
        if (!b || !b.device_type) return;    // no OTA agent on this camera
        var box = document.createElement("div");
        box.className = "setting fwpanel";
        box.innerHTML =
          '<div class="meta"><h3>Firmware</h3><p class="fw-info">loading…</p></div>' +
          '<div class="fw-ctl">' +
          '<select class="fw-sel sselect"><option>loading…</option></select>' +
          '<button class="btn fw-dl" title="Make this the target — the camera pulls it within ~10 min and self-confirms">Set target</button>' +
          "</div>";
        root.appendChild(box);
        var info = box.querySelector(".fw-info");
        var sel = box.querySelector(".fw-sel");

        function refresh() {
          Promise.all([
            api("/api/v1/cameras/" + encodeURIComponent(camId) + "/bundle").catch(function () { return {}; }),
            api("/api/v1/firmware").catch(function () { return []; })
          ]).then(function (rs) {
            var cur = rs[0] || {};
            var tgt = (rs[1] || []).filter(function (t) {
              return t.device_type === b.device_type;
            })[0];
            // whole-system (A/B slot) target, reported by nn-sysupd as "system"
            var sysTgt = (rs[1] || []).filter(function (t) {
              return t.device_type === "byai_system";
            })[0];
            var layers = "";
            if (cur.platform) layers += "<br>platform: <b>" + esc(cur.platform) + "</b>";
            if (cur.system || sysTgt) {
              layers += "<br>system: <b>" + esc(cur.system || "unknown") + "</b>" +
                (cur.slot ? " on slot " + esc(cur.slot) : "") +
                (sysTgt ? " · target " + esc(sysTgt.target_version || sysTgt.version || "?") : "");
            }
            var age = cur.reported_at
              ? Math.round((Date.now() / 1000 - cur.reported_at) / 60) : null;
            info.innerHTML =
              "running: <b>" + esc(cur.version || "unknown") + "</b>" +
              (b.device_type ? ' · <span class="fw-image">' + esc(b.device_type) + "</span>" : "") +
              (age !== null ? " (reported " + age + " min ago)" : "") +
              (b.wdt_rc !== undefined
                ? "<br>watchdog resets: <b>" + b.wdt_rc + "</b> · last boot: " +
                  esc(b.rst || "?") +
                  (b.rst === "WDT" ? ' <span class="err">← recovered by watchdog</span>' : "")
                : "") +
              layers +
              "<br>target: " +
              esc(tgt ? (tgt.target_version || tgt.version || "?") : "none") +
              " · applies on the camera's next 10-min tick, self-confirms or rolls back";
          });
        }

        api("/api/v1/firmware/catalog?device_type=" +
            encodeURIComponent(b.device_type)).then(function (es) {
          sel.innerHTML = "";
          if (!es || !es.length) {
            sel.innerHTML = "<option>no bundles in catalog</option>";
            sel.disabled = true;
            return;
          }
          es.forEach(function (e) {
            var o = document.createElement("option");
            o.value = e.version;
            o.textContent = e.version + "  (" + e.source_name + ")";
            sel.appendChild(o);
          });
        });

        box.querySelector(".btn.fw-dl").onclick = function () {
          if (sel.disabled) return;
          var v = sel.value;
          info.innerHTML = "promoting " + esc(v) + "…";
          fetch("/api/v1/firmware/catalog/" +
                encodeURIComponent(b.device_type) + "/" +
                encodeURIComponent(v) + "/promote", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}"
          }).then(function (r) { return r.json(); })
            .then(function () { refresh(); })
            .catch(function (e) { info.textContent = "promote failed: " + e; });
        };

        refresh();
      }).catch(function () {});
  }

  function renderInfer(camId, d) {
    var pol = d.policy || {engines: {}};
    var names = d.classes || [];
    var engines = ["edge", "service"];
    var hasEdge = !!(d.caps && d.caps.infer);
    var edgeErr = (d.caps && d.caps.infer_error) || "";
    var hasSvc = !(d.caps && d.caps.service_infer === null);   // service unless told otherwise
    var html =
      '<div class="setting"><div class="meta">' +
        '<h3>Detection policy</h3><p>A class captures video when its ' +
        'aggregated confidence (mean of the last <em>N</em> inferences) ' +
        'reaches <em>start</em>, and stops below <em>stop</em>. ' +
        'Aggregating is what stops a single lucky frame from recording.</p>' +
        '</div><div class="switch"><button id="pol-save">Save</button></div>' +
      '</div>' +
      // An edge engine that FAILED to start is not the same as hardware that
      // never had one, but both used to render as a missing section — so a
      // broken accelerator looked like a design choice.  Say which it is.
      (edgeErr ?
        '<div class="setting"><div class="meta"><h3>Edge engine unavailable</h3>' +
        '<p>This camera has an edge accelerator, but its engine failed to ' +
        'start, so detection is running on the <em>service</em> engine only. ' +
        'The camera reported: <code>' + esc(edgeErr) + '</code></p></div></div>'
        : '') +
      '<div class="logbox" id="pol-status">policy v' + (pol.version || 0) +
        (hasEdge || edgeErr ? '' : ' · this camera has no edge engine (service inference only)') +
        (names.length ? '' : ' · no class list advertised yet — is the camera connected?') +
        '</div>';

    engines.forEach(function (en) {
      if (en === "edge" && !hasEdge) return;
      if (en === "service" && !hasSvc) return;
      var e = (pol.engines || {})[en] || {enabled: true, classes: {}};
      html += '<div class="setting" style="display:block">' +
        '<h3 style="text-transform:capitalize">' + en + ' engine</h3>' +
        '<p style="margin-bottom:10px">' +
          (en === "edge" ? "runs on the camera (nn_infer)"
                         : "runs in the media service") + '</p>' +
        '<div class="fpsrow">Run at most ' +
          '<input type="number" class="p-fps" data-engine="' + en + '" ' +
            'min="1" max="15" step="1" value="' + (e.fps || 5) + '"> fps' +
          '<small> · one inference per 1000/N ms; a slower camera simply ' +
          'stays under the cap</small></div>' +
        '<table class="poltab" data-engine="' + en + '"><thead><tr>' +
          '<th>class</th><th>capture</th><th>agg</th><th>start</th><th>stop</th>' +
        '</tr></thead><tbody>';
      // configured classes first, then a picker to add more
      Object.keys(e.classes || {}).sort().forEach(function (cn) {
        html += polRow(cn, e.classes[cn]);
      });
      // "motion" pseudo-class: service engine only — it gates the motion
      // detector itself through the same aggregation/hysteresis
      var opts = (en === "service" ? ["motion"] : []).concat(names);
      html += '</tbody></table>' +
        '<div style="margin-top:10px"><select class="pol-add" data-engine="' + en + '">' +
          '<option value="">add class…</option>' +
          opts.map(function (n) { return '<option>' + n + '</option>'; }).join("") +
        '</select></div></div>';
    });
    app.innerHTML = html;
    mountTabs("cam", camId, "detection");

    [].forEach.call(document.querySelectorAll(".pol-add"), function (sel) {
      sel.onchange = function () {
        if (!sel.value) return;
        var tb = document.querySelector('.poltab[data-engine="' +
                   sel.getAttribute("data-engine") + '"] tbody');
        if (tb.querySelector('tr[data-cls="' + sel.value + '"]')) { sel.value = ""; return; }
        // motion is a frame-change RATIO, not a confidence — sane defaults
        // are two orders of magnitude below an object class's
        tb.insertAdjacentHTML("beforeend",
          polRow(sel.value, sel.value === "motion"
            ? {capture: true, agg: 5, start: 0.02, stop: 0.01}
            : {capture: true, agg: 5, start: 0.6, stop: 0.4}));
        sel.value = "";
      };
    });
    document.getElementById("pol-save").onclick = function () { savePolicy(camId); };
  }

  function polRow(cn, c) {
    return '<tr data-cls="' + cn + '">' +
      '<td>' + cn + '</td>' +
      '<td><input type="checkbox" class="p-cap"' + (c.capture ? " checked" : "") + '></td>' +
      '<td><input type="number" class="p-agg" min="1" max="30" value="' + (c.agg || 5) + '"></td>' +
      '<td><input type="number" class="p-start" min="0" max="1" step="0.05" value="' + (c.start != null ? c.start : 0.6) + '"></td>' +
      '<td><input type="number" class="p-stop" min="0" max="1" step="0.05" value="' + (c.stop != null ? c.stop : 0.4) + '"></td>' +
      '<td><button class="p-del">✕</button></td></tr>';
  }

  function savePolicy(camId) {
    var engines = {};
    [].forEach.call(document.querySelectorAll(".poltab"), function (tab) {
      var classes = {};
      [].forEach.call(tab.querySelectorAll("tbody tr"), function (tr) {
        var start = parseFloat(tr.querySelector(".p-start").value) || 0;
        var stop = parseFloat(tr.querySelector(".p-stop").value) || 0;
        classes[tr.getAttribute("data-cls")] = {
          capture: tr.querySelector(".p-cap").checked,
          agg: parseInt(tr.querySelector(".p-agg").value, 10) || 5,
          start: start,
          stop: Math.min(stop, start)      // the invariant, enforced here too
        };
      });
      var en = tab.getAttribute("data-engine");
      var fpsEl = document.querySelector('.p-fps[data-engine="' + en + '"]');
      var fps = fpsEl ? parseInt(fpsEl.value, 10) : 5;
      if (!(fps >= 1)) fps = 1;
      if (fps > 15) fps = 15;
      engines[en] = {enabled: true, fps: fps, classes: classes};
    });
    var st = document.getElementById("pol-status");
    st.textContent = "saving…";
    fetch("/api/v1/cameras/" + encodeURIComponent(camId) + "/inference", {
      method: "PUT", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({policy: {engines: engines}})
    }).then(function (r) { return r.json().then(function (j) { return {ok: r.ok, j: j}; }); })
      .then(function (res) {
        st.textContent = res.ok
          ? "saved · policy v" + res.j.policy.version +
            " (the camera picks it up within 30 s)"
          : "error: " + (res.j.error || "rejected");
      })
      .catch(function () { st.textContent = "save failed"; });
  }

  // delegated: row delete
  document.addEventListener("click", function (ev) {
    if (ev.target && ev.target.classList.contains("p-del")) {
      var tr = ev.target.closest("tr"); if (tr) tr.remove();
    }
  });

  // ── settings view ─────────────────────────────────────────────────────────
  var SETTINGS_TABS = [["general", "General"], ["debug", "Debug"]];

  function viewSettings(tab) {
    tab = tab || "general";
    setHeader("Settings", false);
    app.innerHTML = '<nav class="dtabs">' + SETTINGS_TABS.map(function (t) {
      return '<a class="dtab' + (t[0] === tab ? " on" : "") +
             '" href="#/settings/' + t[0] + '">' + t[1] + "</a>";
    }).join("") + '</nav><div id="settings-host"></div>';
    var host = document.getElementById("settings-host");
    if (tab === "debug") settingsDebug(host);
    else settingsGeneral(host);
  }

  function settingsDebug(host) {
    host.innerHTML =
      '<div class="setting">' +
        '<div class="meta">' +
          '<h3>Client debug reporting</h3>' +
          '<p>Browsers report player errors and stalls back to the hub so a ' +
          'playback problem can be diagnosed without reproducing it here. ' +
          'Batched and rate-limited; safe to leave on briefly. Reports appear ' +
          'under <a href="#/logging/reports">Logging</a>.</p>' +
        '</div>' +
        '<div class="switch"><button id="dbg-btn">…</button></div>' +
      '</div>';

    var btn = document.getElementById("dbg-btn");
    function render(j) {
      var on = !!(j && j.debug && j.debug.enabled);
      btn.textContent = on ? "On" : "Off";
      btn.classList.toggle("on", on);
    }
    function load() {
      fetch("/api/v1/clientlog/debug", {cache: "no-store"})
        .then(function (r) { return r.json(); }).then(render)
        .catch(function () { btn.textContent = "?"; });
    }
    btn.onclick = function () {
      var turnOn = btn.textContent !== "On";
      btn.textContent = "…";
      fetch("/api/v1/clientlog/debug", {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({enabled: turnOn})
      }).then(function () { DBG_ON = turnOn; load(); })
        .catch(function () { load(); });
    };
    load();
  }

  function settingsGeneral(host) {
    host.innerHTML =
      '<div class="setting">' +
        '<div class="meta">' +
          '<h3>Device-log store cap</h3>' +
          '<p>Sensor logs the hub keeps on disk (hourly rotation files). ' +
          'Oldest files are deleted past this size. ' +
          '<span id="devlog-use" class="mono"></span></p>' +
        '</div>' +
        '<div class="switch" style="gap:8px;display:flex;align-items:center">' +
          '<input id="devlogcap-mb" type="number" min="16" max="65536" step="16" ' +
            'style="width:110px;padding:8px;border-radius:8px;border:1px solid var(--line);' +
            'background:transparent;color:inherit;font:inherit"> MB ' +
          '<button id="devlogcap-save">Save</button>' +
        '</div>' +
      '</div>' +
      '<div class="setting">' +
        '<div class="meta">' +
          '<h3>Camera app-log cap</h3>' +
          '<p>Linux cameras keep a local app log (<code>/opt/nn/log/cam.log</code>). ' +
          'Their update agent trims it past this size on each 10-minute tick, ' +
          'keeping one rotated copy.</p>' +
        '</div>' +
        '<div class="switch" style="gap:8px;display:flex;align-items:center">' +
          '<input id="logcap-kb" type="number" min="64" max="1048576" step="64" ' +
            'style="width:110px;padding:8px;border-radius:8px;border:1px solid var(--line);' +
            'background:transparent;color:inherit;font:inherit"> KB ' +
          '<button id="logcap-save">Save</button>' +
        '</div>' +
      '</div>';

    var dIn = document.getElementById("devlogcap-mb");
    var dBtn = document.getElementById("devlogcap-save");
    var dUse = document.getElementById("devlog-use");
    function devlogRender(j) {
      if (j.cap_mb) dIn.value = j.cap_mb;
      dUse.textContent = (j.used_mb != null) ?
        "now " + j.used_mb + " MB in " + j.files + " files" : "";
    }
    fetch("/api/v1/settings/device-log-cap", {cache: "no-store"})
      .then(function (r) { return r.json(); }).then(devlogRender)
      .catch(function () {});
    dBtn.onclick = function () {
      dBtn.textContent = "…";
      fetch("/api/v1/settings/device-log-cap", {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({cap_mb: +dIn.value})
      }).then(function (r) { return r.json(); })
        .then(function (j) {
          dBtn.textContent = "Saved";
          devlogRender(j);
          setTimeout(function () { dBtn.textContent = "Save"; }, 1500);
        })
        .catch(function () { dBtn.textContent = "Save"; });
    };

    var capIn = document.getElementById("logcap-kb");
    var capBtn = document.getElementById("logcap-save");
    fetch("/api/v1/settings/camera-log-cap", {cache: "no-store"})
      .then(function (r) { return r.json(); })
      .then(function (j) { capIn.value = j.cap_kb; })
      .catch(function () {});
    capBtn.onclick = function () {
      capBtn.textContent = "…";
      fetch("/api/v1/settings/camera-log-cap", {
        method: "PUT", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({cap_kb: +capIn.value})
      }).then(function (r) { return r.json(); })
        .then(function (j) {
          capBtn.textContent = "Saved";
          if (j.cap_kb) capIn.value = j.cap_kb;
          setTimeout(function () { capBtn.textContent = "Save"; }, 1500);
        })
        .catch(function () { capBtn.textContent = "Save"; });
    };
  }

  // ── logging ───────────────────────────────────────────────────────────────
  // Everything you READ lives here (Settings is for things you change):
  // a device's journal, the shared inference daemon's counters, and the
  // reports browsers send back — one tab each, so the live journal gets
  // the full page instead of sharing it with two more boxes.
  var LOGGING_TABS = [["device", "Device log"], ["metrics", "Metrics"], ["inferd", "Inference"],
                      ["reports", "Client reports"]];

  function viewLogging(tab) {
    tab = tab || "device";
    setHeader("Logging", false);
    app.innerHTML = '<nav class="dtabs">' + LOGGING_TABS.map(function (t) {
      return '<a class="dtab' + (t[0] === tab ? " on" : "") +
             '" href="#/logging/' + t[0] + '">' + t[1] + "</a>";
    }).join("") + '</nav><div id="logging-host"></div>';
    var host = document.getElementById("logging-host");
    if (tab === "inferd") loggingInferd(host);
    else if (tab === "metrics") loggingMetrics(host);
    else if (tab === "reports") loggingReports(host);
    else loggingDevice(host);
  }

  // Metrics: every camera's pipeline metrics, collected from the media host
  // each minute, rolled into one report at the top of every hour and reset.
  function loggingMetrics(host) {
    host.innerHTML =
      '<div class="setting">' +
        '<div class="meta"><h3>Camera metrics</h3>' +
        '<p>Collected from the media host every minute per camera; at the top of each hour ' +
        'they become one report and reset.</p></div>' +
        '<div class="switch"><button id="met-refresh">Refresh</button>' +
        '<button id="met-rollup" title="close the hour now">Roll up now</button></div>' +
      '</div>' +
      '<div class="setting met-charts-card"><div class="meta"><h3>Hourly reports</h3></div>' +
        '<div class="met-filters" id="met-filters"></div>' +
        '<div class="met-charts" id="met-charts"><p class="muted">loading…</p></div>' +
        '<details class="met-tablebox"><summary>Show raw</summary><div class="tablewrap"><div class="fleet-tbl met-raw" id="met-table"></div></div></details>' +
      '</div>' +
      '<div class="setting" id="met-current"><div class="meta"><h3>This hour so far</h3></div><div class="logbox" id="met-now">—</div></div>';
    var nowBox = document.getElementById("met-now");
    var table = document.getElementById("met-table");
    var chartsBox = document.getElementById("met-charts");
    var filtersBox = document.getElementById("met-filters");

    function hm(ts) { var d = new Date(ts * 1000); return d.toLocaleDateString() + " " + String(d.getHours()).padStart(2, "0") + ":00"; }
    function pipeSummary(p) {
      var parts = [];
      Object.keys(p || {}).forEach(function (n) {
        var a = p[n];
        if (a.failure_minutes || a.stalls || a.resets) parts.push(n + ": " + (a.failure_minutes ? a.failure_minutes + " min failing " : "") + (a.stalls ? a.stalls + " stalls " : "") + (a.resets ? a.resets + " resets" : ""));
      });
      return parts.length ? parts.join("; ") : "clean";
    }
    function camLine(c, a) {
      return c + ":  up " + a.connected_minutes + "/" + a.minutes + " min" + (a.session_drops ? ", " + a.session_drops + " drop(s)" : "") +
        ", HLS stale " + a.hls_stale_minutes + " min, no video " + a.no_video_minutes + " min, " +
        Math.round((a.bytes || 0) / 1048576) + " MB, " + a.inferences + " inferences" + (a.inference_ms_max ? " (max " + a.inference_ms_max + " ms)" : "") +
        ", " + a.events + " events, uploads " + a.uploads_ok + "/" + (a.uploads_ok + a.uploads_failed) + "  [" + pipeSummary(a.pipelines) + "]";
    }
    function hostLine(h) {
      if (!h) return "";
      var drops = Object.keys(h.queue_drops || {}).filter(function (k) { return h.queue_drops[k]; }).map(function (k) { return k + " " + h.queue_drops[k]; }).join(", ");
      return "media host: " + h.samples + " samples, RSS " + h.rss_mb_min + "-" + h.rss_mb_max + " MB (avg " + h.rss_mb_avg + "), CPU avg " + h.cpu_pct_avg + "% max " + h.cpu_pct_max + "%" +
        (h.restarts ? ", " + h.restarts + " restart(s)" : "") + (h.unreachable_minutes ? ", unreachable " + h.unreachable_minutes + " min" : "") + (drops ? ", queue drops: " + drops : "");
    }
    function loadCurrent() {
      fetch("/api/v1/metrics/current", {cache: "no-store"}).then(function (r) { return r.ok ? r.json() : null; }).then(function (d) {
        if (!d) { nowBox.textContent = "collector not running"; return; }
        var lines = ["since " + hm(d.period_start) + (d.last_error ? "   last error: " + d.last_error : "")];
        Object.keys(d.cameras || {}).sort().forEach(function (c) { lines.push(camLine(c, d.cameras[c])); });
        lines.push(hostLine(d.host));
        nowBox.textContent = lines.join("\n");
      }).catch(function () { nowBox.textContent = "hub unreachable"; });
    }
    // ── small multiples: one row per metric, every camera a line, one shared
    // hourly timeline.  Colour follows the camera (fixed slot by id order),
    // never its rank, so hiding a camera does not repaint the others.
    // Dark categorical slots validated against the panel surface (#17171e):
    // all checks pass, worst adjacent CVD dE 8.4.
    var MET_COLORS = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"];
    var MET_KEY = "nnMetricsView";
    var metView = {hidden: [], range: 48};
    try { var sv = JSON.parse(localStorage.getItem(MET_KEY) || "null"); if (sv) { metView.hidden = sv.hidden || []; metView.range = sv.range || 48; } } catch (e) {}
    function saveView() { try { localStorage.setItem(MET_KEY, JSON.stringify(metView)); } catch (e) {} }
    var lastReps = [];

    function pipeProblems(a) {
      var n = 0;
      Object.keys(a.pipelines || {}).forEach(function (k) { var p = a.pipelines[k]; n += (p.resets || 0) + (p.stalls || 0) + (p.failure_minutes || 0); });
      return n;
    }
    var MET_ROWS = [
      {key: "up",    label: "Connected",        unit: "%",  max: 100, cam: function (a) { return a.uptime_pct; }},
      {key: "hls",   label: "HLS fresh",        unit: "%",  max: 100, cam: function (a) { return a.hls_fresh_pct; }},
      {key: "drops", label: "Session drops",    unit: "",   cam: function (a) { return a.session_drops; }},
      {key: "mb",    label: "Data received",    unit: "MB", cam: function (a) { return Math.round((a.bytes || 0) / 1048576); }},
      {key: "inf",   label: "Inferences",       unit: "",   cam: function (a) { return a.inferences; }},
      {key: "ev",    label: "Events recorded",  unit: "",   cam: function (a) { return a.events; }},
      {key: "pp",    label: "Pipeline problems", unit: "",  hint: "resets + stalls + minutes with failures", cam: pipeProblems},
      {key: "cpu",   label: "Media host CPU",   unit: "%",  host: function (h) { return h.cpu_pct_avg; }},
      {key: "rss",   label: "Media host memory", unit: "MB", host: function (h) { return h.rss_mb_avg; }}
    ];

    function fmtHour(ts, withDate) {
      var d = new Date(ts * 1000), hh = String(d.getHours()).padStart(2, "0") + ":00";
      return withDate ? (d.getMonth() + 1) + "/" + d.getDate() + " " + hh : hh;
    }
    function niceMax(v) {
      if (v <= 0) return 1;
      var p = Math.pow(10, Math.floor(Math.log10(v))), m = v / p;
      return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
    }

    function renderFilters(cams) {
      filtersBox.innerHTML = cams.map(function (c, i) {
        var on = metView.hidden.indexOf(c) < 0;
        return '<label class="met-chip' + (on ? "" : " off") + '"><input type="checkbox" data-cam="' + esc(c) + '"' + (on ? " checked" : "") + ">" +
          '<span class="met-sw" style="background:' + MET_COLORS[i % MET_COLORS.length] + '"></span>' + esc(c) + "</label>";
      }).join("") +
        '<select id="met-range" title="time range">' + [[24, "last 24 h"], [48, "last 48 h"], [168, "last 7 days"], [720, "last 30 days"]].map(function (o) {
          return '<option value="' + o[0] + '"' + (metView.range === o[0] ? " selected" : "") + ">" + o[1] + "</option>";
        }).join("") + "</select>";
      [].forEach.call(filtersBox.querySelectorAll("input[data-cam]"), function (cb) {
        cb.onchange = function () {
          var c = cb.getAttribute("data-cam");
          metView.hidden = metView.hidden.filter(function (x) { return x !== c; });
          if (!cb.checked) metView.hidden.push(c);
          saveView(); renderCharts(); renderRaw();
        };
      });
      document.getElementById("met-range").onchange = function () {
        metView.range = +this.value; saveView(); loadReports();
      };
    }

    function renderCharts() {
      var reps = lastReps.slice().sort(function (a, b) { return a.period_start - b.period_start; });
      if (!reps.length) { chartsBox.innerHTML = '<p class="muted">no hourly report yet</p>'; filtersBox.innerHTML = ""; return; }
      var all = {};
      reps.forEach(function (r) { Object.keys((r.report || {}).cameras || {}).forEach(function (c) { all[c] = 1; }); });
      var cams = Object.keys(all).sort();                   // colour slot = position in this list
      renderFilters(cams);
      var shown = cams.filter(function (c) { return metView.hidden.indexOf(c) < 0; });
      var t0 = reps[0].period_start, t1 = reps[reps.length - 1].period_start;
      var W = Math.max(320, chartsBox.clientWidth || 600), L = 150, R = 12, H = 64, T = 8, B = 6;
      var pw = W - L - R, span = Math.max(3600, t1 - t0);
      function x(t) { return L + (t - t0) / span * pw; }
      // vertical gridlines + time ticks at a readable step
      var stepH = [1, 2, 3, 6, 12, 24, 48, 96].filter(function (h) { return pw / (span / 3600 / h) >= 70; })[0] || 168;
      var step = 3600 * stepH;
      var ticks = []; for (var tt = Math.ceil(t0 / step) * step; tt <= t1; tt += step) ticks.push(tt);
      var html = "";
      MET_ROWS.forEach(function (row, ri) {
        var series = [];
        if (row.host) {
          series.push({name: "host", color: "var(--text)", pts: reps.map(function (r) { var h = (r.report || {}).host; return h ? [r.period_start, row.host(h)] : null; })});
        } else {
          shown.forEach(function (c) {
            series.push({name: c, color: MET_COLORS[cams.indexOf(c) % MET_COLORS.length],
                         pts: reps.map(function (r) { var a = ((r.report || {}).cameras || {})[c]; return a ? [r.period_start, row.cam(a)] : null; })});
          });
        }
        var vmax = 0; series.forEach(function (s) { s.pts.forEach(function (p) { if (p && p[1] > vmax) vmax = p[1]; }); });
        var ymax = row.max || niceMax(vmax);
        function y(v) { return T + (1 - v / ymax) * (H - T - B); }
        var svg = '<svg class="met-svg" width="' + W + '" height="' + H + '" data-row="' + ri + '">';
        ticks.forEach(function (tk) { svg += '<line class="met-grid" x1="' + x(tk) + '" x2="' + x(tk) + '" y1="' + T + '" y2="' + (H - B) + '"/>'; });
        svg += '<line class="met-base" x1="' + L + '" x2="' + (W - R) + '" y1="' + y(0) + '" y2="' + y(0) + '"/>';
        svg += '<text class="met-lbl" x="0" y="' + (H / 2 + 4) + '">' + esc(row.label) + "</text>";
        svg += '<text class="met-ax" x="' + (L - 6) + '" y="' + (T + 4) + '" text-anchor="end">' + ymax + (row.unit ? " " + row.unit : "") + "</text>";
        svg += '<text class="met-ax" x="' + (L - 6) + '" y="' + (y(0) + 3) + '" text-anchor="end">0</text>';
        series.forEach(function (s) {
          var d = "", pen = false;
          s.pts.forEach(function (p) {
            if (!p || p[1] == null) { pen = false; return; }
            d += (pen ? "L" : "M") + x(p[0]).toFixed(1) + "," + y(p[1]).toFixed(1); pen = true;
          });
          if (d) svg += '<path class="met-line" d="' + d + '" stroke="' + s.color + '"/>';
          if (s.pts.filter(Boolean).length === 1) {                 // a lone point still shows
            var p1 = s.pts.filter(Boolean)[0];
            svg += '<circle cx="' + x(p1[0]) + '" cy="' + y(p1[1]) + '" r="3" fill="' + s.color + '"/>';
          }
        });
        svg += '<line class="met-cross" x1="0" x2="0" y1="' + T + '" y2="' + (H - B) + '" visibility="hidden"/>';
        svg += '<g class="met-dots"></g>';
        svg += '<rect class="met-hit" x="' + L + '" y="0" width="' + pw + '" height="' + H + '"/></svg>';
        html += '<div class="met-row" title="' + esc(row.hint || "") + '">' + svg + "</div>";
        row._series = series; row._y = y;
      });
      // the shared timeline, once, under the last row
      var ax = '<svg class="met-svg" width="' + W + '" height="18">';
      ticks.forEach(function (tk, i) {
        ax += '<text class="met-ax" x="' + x(tk) + '" y="13" text-anchor="middle">' + fmtHour(tk, i === 0 || new Date(tk * 1000).getHours() === 0) + "</text>";
      });
      html += ax + "</svg>";
      html += '<div class="met-tip" id="met-tip" hidden></div>';
      chartsBox.innerHTML = html;

      // hover: one crosshair across every row, the tooltip lists the hovered row's values
      var tip = document.getElementById("met-tip");
      var hours = reps.map(function (r) { return r.period_start; });
      function nearest(px) {
        var t = t0 + (px - L) / pw * span, best = 0;
        hours.forEach(function (h, i) { if (Math.abs(h - t) < Math.abs(hours[best] - t)) best = i; });
        return best;
      }
      [].forEach.call(chartsBox.querySelectorAll(".met-hit"), function (hit) {
        var svgEl = hit.parentNode, ri = +svgEl.getAttribute("data-row"), row = MET_ROWS[ri];
        hit.onmousemove = function (ev) {
          var box = svgEl.getBoundingClientRect(), i = nearest(ev.clientX - box.left), cx = x(hours[i]);
          [].forEach.call(chartsBox.querySelectorAll("svg[data-row]"), function (sv) {
            var r2 = MET_ROWS[+sv.getAttribute("data-row")], cr = sv.querySelector(".met-cross"), g = sv.querySelector(".met-dots");
            cr.setAttribute("x1", cx); cr.setAttribute("x2", cx); cr.setAttribute("visibility", "visible");
            g.innerHTML = r2._series.map(function (s) {
              var p = s.pts[i];
              return p ? '<circle cx="' + cx + '" cy="' + r2._y(p[1]) + '" r="4" fill="' + s.color + '" stroke="var(--panel)" stroke-width="2"/>' : "";
            }).join("");
          });
          var rep = reps[i].report || {};
          tip.innerHTML = '<div class="met-tip-h">' + esc(fmtHour(hours[i], true)) + (rep.reason === "operator" ? " (closed early)" : "") + " · " + esc(row.label) + "</div>" +
            row._series.map(function (s) {
              var p = s.pts[i];
              return '<div><span class="met-sw" style="background:' + s.color + '"></span>' + esc(s.name) + ' <b>' + (p ? p[1] : "—") + "</b>" + (p && row.unit ? " " + row.unit : "") + "</div>";
            }).join("");
          tip.hidden = false;
          var cb = chartsBox.getBoundingClientRect();
          var left = ev.clientX - cb.left + 14; if (left + 190 > cb.width) left = ev.clientX - cb.left - 200;
          tip.style.left = left + "px"; tip.style.top = (box.top - cb.top + 4) + "px";
        };
        hit.onmouseleave = function () {
          tip.hidden = true;
          [].forEach.call(chartsBox.querySelectorAll(".met-cross"), function (c) { c.setAttribute("visibility", "hidden"); });
          [].forEach.call(chartsBox.querySelectorAll(".met-dots"), function (g) { g.innerHTML = ""; });
        };
      });
    }
    var metResizeT = null;
    window.addEventListener("resize", function () {
      if (!document.getElementById("met-charts")) return;          // left the tab
      clearTimeout(metResizeT); metResizeT = setTimeout(renderCharts, 150);
    });

    function loadReports() {
      fetch("/api/v1/metrics/reports?limit=" + metView.range, {cache: "no-store"}).then(function (r) { return r.json(); }).then(function (j) {
        var reps = (j && j.reports) || [];
        lastReps = reps;
        renderCharts();
        renderRaw();
      }).catch(function () { table.innerHTML = '<div class="empty err">hub unreachable</div>'; });
    }
    // "Show raw": the same numbers the charts draw, in the fleet table style
    // used by Automation › On devices.  Newest hour first; follows the camera
    // chips above, so hidden cameras are hidden here too.
    function renderRaw() {
      var reps = lastReps;
      if (!reps.length) { table.innerHTML = '<div class="empty">no hourly report yet</div>'; return; }
      var head = ["hour", "camera", "up", "drops", "HLS", "data", "infer", "events", "uploads", "pipelines"];
      var html = '<div class="fleet-row fleet-head">' + head.map(function (h) { return "<span>" + h + "</span>"; }).join("") + "</div>";
      reps.forEach(function (r) {
        var rep = r.report || {}, cams = rep.cameras || {};
        var ids = Object.keys(cams).sort().filter(function (c) { return metView.hidden.indexOf(c) < 0; });
        ids.forEach(function (c, i) {
          var a = cams[c], ps = pipeSummary(a.pipelines);
          var hour = i === 0 ? esc(fmtHour(r.period_start, true)) + (rep.reason === "operator" ? ' <span class="fleet-st dim">closed early</span>' : "") : "";
          function cell(label, val, warn) {
            return '<span class="mono' + (warn ? " fleet-st warn" : "") + '" data-label="' + label + '">' + val + "</span>";
          }
          html += '<div class="fleet-row' + (i === 0 ? " met-first" : "") + '">' +
            '<span class="mono met-hour">' + hour + "</span>" +
            '<span class="met-cam">' + esc(c) + "</span>" +
            cell("up", a.uptime_pct + "%", a.uptime_pct < 100) +
            cell("drops", a.session_drops, a.session_drops) +
            cell("HLS", a.hls_fresh_pct + "%", a.hls_fresh_pct < 100) +
            cell("data", Math.round((a.bytes || 0) / 1048576) + " MB") +
            cell("infer", a.inferences) +
            cell("events", a.events) +
            cell("uploads", a.uploads_ok + "/" + (a.uploads_ok + a.uploads_failed), a.uploads_failed) +
            '<span class="fleet-st met-pipes ' + (ps === "clean" ? "ok" : "warn") + '" data-label="pipelines" title="' + esc(ps) + '">' + esc(ps) + "</span>" +
            "</div>";
        });
        html += '<div class="fleet-row met-raw-host"><span class="fleet-st dim">' + esc(hostLine(rep.host)) + "</span></div>";
      });
      table.innerHTML = html;
    }

    function load() { loadCurrent(); loadReports(); }
    document.getElementById("met-refresh").onclick = load;
    document.getElementById("met-rollup").onclick = function () {
      if (!confirm("Close the current hour now and store its report?")) return;
      fetch("/api/v1/metrics/rollup", {method: "POST"}).then(load);
    };
    load();
  }

  function loggingDevice(host) {
    host.innerHTML =
      '<div class="setting">' +
        '<div class="meta"><h3>Device log</h3>' +
        '<p>Last lines from a device\u2019s service journal on the hub host.</p></div>' +
        '<div class="switch">' +
          '<select id="log-src"><option value="">loading…</option></select>' +
          '<select id="log-pipe" hidden><option value="">all pipelines</option></select>' +
          '<select id="log-n">' +
            '<option>100</option><option selected>200</option>' +
            '<option>500</option><option>1000</option></select>' +
          '<span class="loglines-lbl">lines</span>' +
          '<button id="log-wrap">No wrap</button>' +
          '<button id="log-refresh">Reload</button>' +
          '<button id="log-pause">Pause</button>' +
        '</div>' +
      '</div>' +
      '<div class="logstatus-row"><span class="logstatus" id="log-status">…</span></div>' +
      '<div class="logbox logbox-tall" id="log-box"></div>';

    // ── device journal (live) ──
    // A log is a stream, so it is streamed: the socket sends the tail first
    // and then each new line as it is written.  Switching device clears the
    // box and opens a new socket — an empty box means that device has
    // written nothing, not that something failed.
    var sel = document.getElementById("log-src");
    var nsel = document.getElementById("log-n");
    var psel = document.getElementById("log-pipe");
    function pipeParam() { return psel && !psel.hidden && psel.value ? "&pipeline=" + encodeURIComponent(psel.value) : ""; }
    function syncPipe() {
      var isCam = sel.value.indexOf("nn-video:") === 0;
      psel.hidden = !isCam;
      if (!isCam) psel.value = "";
    }
    var lbox = document.getElementById("log-box");
    var pauseBtn = document.getElementById("log-pause");
    var wrapBtn = document.getElementById("log-wrap");
    wrapBtn.onclick = function () {
      var nw = lbox.classList.toggle("nowrap");
      wrapBtn.textContent = nw ? "Wrap" : "No wrap";
    };
    var statusEl = document.getElementById("log-status");
    var sock = null, paused = false, pending = [];
    var MAX_LINES = 4000;      // keep the DOM bounded on a chatty device

    function setStatus(t, cls) {
      statusEl.textContent = t;
      statusEl.className = "logstatus" + (cls ? " " + cls : "");
    }

    function atBottom() {
      return lbox.scrollTop + lbox.clientHeight >= lbox.scrollHeight - 24;
    }

    // journald repeats "<iso-ts> host proc[pid]:" on every line — half the
    // width gone before the message starts.  Show HH:MM:SS + message; the
    // full line stays in the tooltip.  E/W lines get tinted.
    var rawLines = [];
    function fmtLine(l) {
      var p = NN.parseLogLine(l);
      return '<span class="ll' + (p.lvl ? " " + p.lvl : "") +
             '" title="' + esc(l) + '">' +
             (p.time ? '<span class="lt">' + p.time + "</span> " : "") +
             esc(p.msg) + "</span>";
    }
    function renderLog() {
      lbox.innerHTML = rawLines.map(fmtLine).join("\n");
    }
    function appendLines(arr) {
      if (!arr.length) return;
      var stick = atBottom();          // don't yank the view if scrolled up
      rawLines = rawLines.concat(arr);
      if (rawLines.length > MAX_LINES) rawLines = rawLines.slice(rawLines.length - MAX_LINES);
      renderLog();
      if (stick) lbox.scrollTop = lbox.scrollHeight;
    }

    function closeSock() {
      if (sock) {
        try { sock.onclose = null; sock.close(); } catch (e) {}
        sock = null;
      }
    }

    function openSock() {
      closeSock();
      lbox.textContent = "";           // empty until this device says something
      pending = [];
      if (!sel.value) { setStatus("no device", ""); return; }
      setStatus("connecting…", "");
      var want = sel.value;
      wsUrl("/api/v1/logs/ws?source=" + encodeURIComponent(want) +
            "&lines=" + encodeURIComponent(nsel.value) + pipeParam()).then(function (url) {
        if (sel.value !== want) return;      // switched again while fetching
        var ws;
        try { ws = new WebSocket(url); }
        catch (e) { setStatus("unavailable", "bad"); return; }
        sock = ws;
        wire(ws);
      });
    }

    function wire(ws) {
      ws.onopen = function () { setStatus("live", "ok"); };
      ws.onmessage = function (ev) {
        if (ev.data && ev.data.charAt(0) === "{") {
          var j = null;
          try { j = JSON.parse(ev.data); } catch (e) {}
          if (j && j.error) { setStatus(j.error, "bad"); return; }
          if (j && j.started) return;    // header, not a log line
        }
        if (paused) {
          pending.push(ev.data);
          if (pending.length > MAX_LINES) pending.splice(0, pending.length - MAX_LINES);
          setStatus("paused (" + pending.length + " buffered)", "");
          return;
        }
        appendLines([ev.data]);
      };
      ws.onerror = function () { setStatus("connection error", "bad"); };
      ws.onclose = function () {
        if (sock !== ws) return;        // superseded by a newer socket
        setStatus("disconnected — retrying", "bad");
        setTimeout(function () { if (sock === ws) openSock(); }, 5000);
      };
    }

    sel.onchange = function () {
      try { localStorage.setItem("nnLogSrc", sel.value); } catch (e) {}
      syncPipe();                       // the pipeline filter only applies to a camera view
      openSock();                       // switching device restarts the stream
    };
    nsel.onchange = openSock;
    document.getElementById("log-refresh").onclick = openSock;
    pauseBtn.onclick = function () {
      paused = !paused;
      pauseBtn.classList.toggle("on", paused);
      pauseBtn.textContent = paused ? "Resume" : "Pause";
      if (!paused) {
        appendLines(pending); pending = [];
        setStatus(sock && sock.readyState === 1 ? "live" : "disconnected",
                  sock && sock.readyState === 1 ? "ok" : "bad");
      }
    };

    fetch("/api/v1/logs/sources", {cache: "no-store"})
      .then(function (r) { return r.json(); })
      .then(function (j) {
        var srcs = (j && j.sources) || [];
        if (!srcs.length) {
          sel.innerHTML = '<option value="">none</option>';
          setStatus("no sources", "bad");
          return;
        }
        sel.innerHTML = srcs.map(function (x) {
          return '<option value="' + esc(x.id) + '">' + esc(x.label) + "</option>";
        }).join("");
        var last = null;
        try { last = localStorage.getItem("nnLogSrc"); } catch (e) {}
        if (last && srcs.some(function (x) { return x.id === last; })) sel.value = last;
        psel.innerHTML = '<option value="">all pipelines</option>' + ((j && j.pipelines) || []).map(function (pn) {
          return '<option value="' + esc(pn) + '">' + esc(pn) + "</option>";
        }).join("");
        psel.onchange = openSock;
        syncPipe();
        openSock();
      })
      .catch(function () {
        sel.innerHTML = '<option value="">unavailable</option>';
        setStatus("hub unreachable", "bad");
      });

    teardown = function () { closeSock(); };
  }

  function loggingInferd(host) {
    host.innerHTML =
      '<div class="setting">' +
        '<div class="meta"><h3>Shared inference daemon</h3>' +
        '<p>One model instance serves every camera. Runs and drops count since ' +
        'the daemon started, so the daemon total also holds cameras that no ' +
        'longer report. Queue is the last wait before a run. Refreshes every ' +
        '10 s.</p></div>' +
        '<div class="switch"><button id="inferd-refresh">Refresh</button></div>' +
      '</div>' +
      '<div id="inferd-box" class="inf-box"><p class="inf-empty">loading…</p></div>';
    var ibox = document.getElementById("inferd-box");
    var KINDS = {1: "CPU", 2: "NPU", 3: "C7x DSP", 4: "Coral", 5: "Hailo"};
    function num(n) { return (Number(n) || 0).toLocaleString("en-US"); }
    function queue(us) {
      us = Number(us) || 0;
      return us >= 1000 ? (us / 1000).toFixed(1) + " ms" : us + " µs";
    }
    function drops(d, runs) {
      d = Number(d) || 0;
      if (!d) return '<span class="fleet-st dim">0</span>';
      var pct = runs ? " (" + (100 * d / (d + runs)).toFixed(1) + "%)" : "";
      return '<span class="fleet-st warn">' + num(d) + pct + "</span>";
    }
    function ago(s) {
      return s < 90 ? s + " s ago" : s < 5400 ? Math.round(s / 60) + " min ago"
                                               : Math.round(s / 3600) + " h ago";
    }
    function cell(label, html, cls) {
      return '<span' + (cls ? ' class="' + cls + '"' : "") +
             ' data-label="' + label + '">' + html + "</span>";
    }
    function render(j) {
      if (!j || !j.hosts || !j.hosts.length) {
        ibox.innerHTML = '<p class="inf-empty">No daemon is reporting. Cameras ' +
                         "fall back to their own detectors.</p>";
        return;
      }
      ibox.innerHTML = j.hosts.map(function (h) {
        var age = Math.max(0, Math.round(Date.now() / 1000 - (h.ts || 0)));
        var t = h.total || {};
        var st = h.stale ? '<span class="fleet-st warn">⚠ stale</span>'
                         : '<span class="fleet-st ok">● live</span>';
        var head =
          '<div class="inf-host"><h4>' + esc(h.host || "?") + "</h4>" + st +
          '<span class="inf-meta">reported ' + ago(age) +
          (h.local ? " · this hub" : "") +
          (h.queue_dropped ? ' · <span class="fleet-st warn">' +
             num(h.queue_dropped) + " dropped at the queue</span>" : "") +
          "</span></div>";
        var devs = h.devices || [];
        var devTbl = !devs.length ? "" :
          '<h5 class="inf-sub">Accelerators</h5>' +
          '<div class="fleet-tbl inf-dev">' +
            '<div class="fleet-row fleet-head"><span>device</span><span>type</span>' +
              "<span>parallel</span><span>health</span></div>" +
            devs.map(function (d) {
              return '<div class="fleet-row">' +
                cell("device", esc(d.id), "mono inf-name") +
                cell("type", esc(KINDS[d.kind] || String(d.kind))) +
                cell("parallel", "×" + esc(String(d.parallelism || 1))) +
                cell("health", d.healthy ? '<span class="fleet-st ok">✓ healthy</span>'
                                         : '<span class="fleet-st warn">✗ unhealthy</span>') +
                "</div>";
            }).join("") +
          "</div>";
        var cams = h.cameras || {};
        var names = Object.keys(cams).sort();
        var camRows = names.map(function (c) {
          var x = cams[c] || {};
          return '<div class="fleet-row">' +
            cell("camera", esc(c), "inf-name") +
            cell("runs", num(x.runs), "mono") +
            cell("dropped", drops(x.dropped, x.runs)) +
            cell("queue", queue(x.queue_us), "mono") +
            "</div>";
        }).join("");
        var camTbl =
          '<h5 class="inf-sub">Cameras</h5>' +
          '<div class="fleet-tbl inf-cam">' +
            '<div class="fleet-row fleet-head"><span>camera</span><span>runs</span>' +
              "<span>dropped</span><span>queue</span></div>" +
            (camRows || '<div class="fleet-row"><span class="fleet-st dim">' +
                        "no camera has used it yet</span></div>") +
            '<div class="fleet-row inf-total" title="Every run since the daemon ' +
                'started, including cameras that no longer report; queue is the ' +
                'most recent wait">' +
              cell("", "daemon total", "inf-name") +
              cell("runs", num(t.runs), "mono") +
              cell("dropped", drops(t.dropped, t.runs)) +
              cell("queue", queue(t.queue_us), "mono") +
            "</div>" +
          "</div>";
        return '<div class="inf-card">' + head + devTbl + camTbl + "</div>";
      }).join("");
    }
    function loadInferd() {
      fetch("/api/v1/inferd/stats", {cache: "no-store"})
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(render)
        .catch(function () {
          ibox.innerHTML = '<p class="inf-empty">Hub unreachable.</p>';
        });
    }
    document.getElementById("inferd-refresh").onclick = loadInferd;
    loadInferd();
    var timer = setInterval(loadInferd, 10000);
    teardown = function () { clearInterval(timer); };
  }

  function loggingReports(host) {
    host.innerHTML =
      '<div class="setting">' +
        '<div class="meta"><h3>Client reports</h3>' +
        '<p>Player errors browsers sent back. Only collected while debug ' +
        'reporting is on (<a href="#/settings">Settings</a>).</p></div>' +
        '<div class="switch"><button id="dbg-refresh">Refresh</button></div>' +
      '</div>' +
      '<div class="logbox" id="dbg-log">—</div>';
    var cbox = document.getElementById("dbg-log");
    function loadReports() {
      fetch("/api/v1/clientlog", {cache: "no-store"})
        .then(function (r) { return r.json(); })
        .then(function (j) {
          var items = (j && j.entries) || [];
          if (!items.length) { cbox.textContent = "no reports"; return; }
          cbox.textContent = items.map(function (e) {
            var when = new Date(e.at * 1000).toLocaleTimeString();
            var what = (e.items || []).map(function (i) {
              return i.kind + " " + JSON.stringify(i.d) + (i.n > 1 ? " x" + i.n : "");
            }).join("; ");
            return when + "  " + e.ip + "  " + (e.page || "") + "\n    " + what +
                   "\n    " + (e.ua || "");
          }).join("\n");
        })
        .catch(function () { cbox.textContent = "hub unreachable"; });
    }
    document.getElementById("dbg-refresh").onclick = loadReports;
    loadReports();
  }

  // ── client debug reporting (opt-in) ────────────────────────────────────────
  // Enabled with ?debug=1 (sticky in localStorage; ?debug=0 clears).  Sends
  // only significant playback events, batched every 10 s, ≤10 events and
  // ≤2 KB per POST, duplicates collapsed with a count — a stuck player
  // cannot flood the hub.
  // Local override (?debug=1, sticky) OR the hub-wide flag
  // (POST /api/v1/clientlog/debug) — the latter turns reporting on for
  // every browser at once, which is what you want when a user reports
  // "it broke" from a device you can't touch.
  var DBG_ON = (function () {
    try {
      var m = /[?&]debug=([01])/.exec(location.search);
      if (m) localStorage.setItem("nnDebug", m[1]);
      return localStorage.getItem("nnDebug") === "1";
    } catch (e) { return false; }
  })();
  (function fetchDebugFlag() {
    // flag ONLY (tiny) — the report list is fetched by the Settings view
    fetch("/api/v1/clientlog/debug", {cache: "no-store"})
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        if (j && j.debug && j.debug.enabled) {
          if (!DBG_ON) console.info("nn debug reporting ON (hub-wide)");
          DBG_ON = true;
        }
      })
      .catch(function () {});
  })();
  var dbgQ = {}, dbgTimer = null;
  function dbg(kind, data) {
    if (!DBG_ON) return;
    var key = kind + "|" + JSON.stringify(data);
    if (dbgQ[key]) { dbgQ[key].n++; return; }        // collapse duplicates
    if (Object.keys(dbgQ).length >= 10) return;      // hard cap per batch
    dbgQ[key] = {t: Date.now(), kind: kind, d: data, n: 1};
    if (!dbgTimer) dbgTimer = setTimeout(dbgFlush, 10000);
  }
  function dbgFlush() {
    dbgTimer = null;
    var items = Object.keys(dbgQ).map(function (k) { return dbgQ[k]; });
    dbgQ = {};
    if (!items.length) return;
    var body = JSON.stringify({
      ua: navigator.userAgent.slice(0, 120),
      page: location.hash.slice(0, 60),
      items: items
    }).slice(0, 2048);
    try {
      if (navigator.sendBeacon)
        navigator.sendBeacon("/api/v1/clientlog",
          new Blob([body], {type: "application/json"}));
      else
        fetch("/api/v1/clientlog", {method: "POST", body: body,
          headers: {"Content-Type": "application/json"}, keepalive: true});
    } catch (e) {}
  }
  window.addEventListener("beforeunload", dbgFlush);
  if (DBG_ON) console.info("nn debug reporting ON (localStorage nnDebug=0 to disable)");

  // ── live view (adaptive: jmuxer when MSE is available, else native HLS) ─────
  function viewLive(camId) {
    setHeader("…", true);
    api("/api/v1/cameras").catch(function () { return []; }).then(function (cams) {
      var cam = findCam(cams, camId);
      setHeader(cam.name, true);

      var stage = document.createElement("div");
      stage.className = "stage";
      stage.innerHTML =
        '<video id="live" autoplay muted playsinline></video>' +
        '<canvas class="ov"></canvas>' +
        '<div class="status">connecting…</div>' +
        '<div class="mode"></div>' +
        '<div class="tstamp"></div>' +
        '<div class="det"></div>';
      app.innerHTML = "";
      app.appendChild(stage);

      var video = stage.querySelector("video");
      var statusEl = stage.querySelector(".status");
      var modeEl = stage.querySelector(".mode");
      var canvas = stage.querySelector(".ov");
      var tsEl = stage.querySelector(".tstamp");
      var stops = [];
      // capture-time badge: fed by /ws text frames (jmuxer) or by the HLS
      // playlist's PROGRAM-DATE-TIME (native Safari exposes getStartDate())
      function showTs(ms) {
        // A device that hasn't timesynced yet reports UPTIME ms, not epoch —
        // show it honestly as "up H:MM:SS" instead of a bogus 1970 wall clock.
        if (ms < 1e12) {
          var t = Math.floor(ms / 1000);
          tsEl.textContent = "up " + Math.floor(t / 3600) + ":" +
            String(Math.floor(t / 60) % 60).padStart(2, "0") + ":" +
            String(t % 60).padStart(2, "0");
          return;
        }
        var d = new Date(ms);
        // YYYY-MM-DD hh:mm:ss — the date matters (a stale stream can show
        // yesterday), milliseconds never did.
        var p2 = function (n) { return String(n).padStart(2, "0"); };
        tsEl.textContent =
          d.getFullYear() + "-" + p2(d.getMonth() + 1) + "-" + p2(d.getDate()) +
          " " + p2(d.getHours()) + ":" + p2(d.getMinutes()) + ":" + p2(d.getSeconds());
        // Both players land here — HLS via the playlist date, jmuxer via the
        // device timestamp — so this one hook covers every live view.
        noteFrameTime(cam.id, ms, modeEl.textContent || "?", function (drift) {
          if (drift) {
            tsEl.classList.add("stale");
            tsEl.title = "This capture time is " + Math.abs(Math.round(drift)) +
              "s " + (drift > 0 ? "behind" : "ahead of") + " the hub clock — " +
              "reported.  The picture may still be live.";
          } else {
            tsEl.classList.remove("stale");
            tsEl.title = "";
          }
        });
      }
      teardown = function () { stops.forEach(function (fn) { try { fn(); } catch (e) {} }); };

      // Transport is chosen by capability (same <video>/chrome either way), and
      // shown in the badge.  jmuxer needs a REAL MediaSource; iPhone Safari only
      // exposes ManagedMediaSource (which jmuxer can't drive — it would hang at
      // "connecting…"), so anything without full MediaSource gets native HLS.
      var hasMSE = ("MediaSource" in window);
      // Native HLS is Safari/iOS only — Chrome/Firefox report
      // MEDIA_ERR_SRC_NOT_SUPPORTED(4) instantly.  So HLS is a fallback ONLY
      // where the browser can actually play it; elsewhere a missing
      // jmuxer.min.js (script load hiccup) must be RETRIED, not fallen
      // through — that dead path is what made a healthy stream look broken
      // until the user refreshed.
      var canNativeHLS = !!video.canPlayType &&
        (video.canPlayType("application/vnd.apple.mpegurl") !== "" ||
         video.canPlayType("application/x-mpegURL") !== "");
      function goJmuxer() {
        modeEl.textContent = "passthrough";
        modeEl.title = "raw H.264, no transcode (jMuxer / MSE)";
        startJmuxer(cam, video, statusEl, stops, showTs);
      }
      function goHLS() {
        modeEl.textContent = "HLS";
        modeEl.title = "HLS (WAVE5 re-encode)";
        startHLS(cam, video, statusEl, stops, showTs);
      }
      if (hasMSE && window.JMuxer) {
        goJmuxer();
      } else if (hasMSE && !canNativeHLS) {
        // MSE is there but the jMuxer script isn't — reload it once.
        statusEl.textContent = "loading player…";
        dbg("player", {ev: "jmuxer_missing", cam: cam.id});
        (function loadJmuxer(attempt) {
          var sc = document.createElement("script");
          sc.src = "/app/jmuxer.min.js?r=" + Date.now();
          sc.onload = function () {
            if (window.JMuxer) { goJmuxer(); return; }
            dbg("player", {ev: "jmuxer_broken", cam: cam.id, try: attempt});
            retry(attempt);
          };
          sc.onerror = function () {
            dbg("player", {ev: "jmuxer_load_fail", cam: cam.id, try: attempt});
            retry(attempt);
          };
          document.head.appendChild(sc);
          function retry(n) {
            if (n >= 3) { statusEl.textContent = "player unavailable — reload the page";
                          return; }
            statusEl.textContent = "loading player… (" + (n + 1) + ")";
            setTimeout(function () { loadJmuxer(n + 1); }, 400 * (n + 1));
          }
        })(0);
      } else {
        goHLS();
      }

      startOverlay(cam, video, canvas, stage, stops);
    });
  }

  function startJmuxer(cam, video, statusEl, stops, showTs) {
    var jm = null, n = 0, t0 = Date.now(), ws, closed = false, retry, restartT = null;
    var restarts = 0;

    // jMuxer reports MSE failures through onError (a SourceBuffer append or a
    // MediaSource teardown).  That is FATAL for the muxer instance: feeding it
    // more data does nothing, so the picture freezes with no <video> error.
    // Rebuild muxer + socket instead of leaving a dead player.
    function makeMuxer() {
      try { if (jm) jm.destroy(); } catch (e) {}
      jm = new JMuxer({
        node: video, mode: "video", flushingTime: 0, fps: 30,
        clearBuffer: true, debug: false,
        onError: function (err) {
          dbg("jmuxer", {ev: "mse_error", cam: cam.id,
                         msg: String(err && err.message || err).slice(0, 60)});
          restart("mse");
        }
      });
    }
    // "reconnecting…" forever tells the viewer nothing.  Ask the hub what the
    // slot's real state is and say that instead -- a slot with no camera
    // attached is not going to come back by retrying harder.
    var explainT = null;
    function explainReconnect() {
      if (explainT) return;
      explainT = setTimeout(function () { explainT = null; }, 4000);
      api("/api/v1/cameras").then(function (list) {
        var c = (list || []).filter(function (x) { return x.id === cam.id; })[0];
        var key = statusOf(c);
        if (key === "no_camera")
          statusEl.textContent = "no camera connected to this slot — check its power and Wi-Fi";
        else if (key === "no_video")
          statusEl.textContent = "camera connected, but no video is arriving";
        else if (key === "offline")
          statusEl.textContent = "media server for this slot is not answering";
        else
          statusEl.textContent = "reconnecting…";
      }).catch(function () {});
    }
    function restart(why) {
      if (closed || restartT) return;
      restarts++;
      dbg("jmuxer", {ev: "restart", why: why, n: restarts, cam: cam.id});
      statusEl.textContent = "reconnecting…";
      explainReconnect();
      try { if (ws) ws.close(); } catch (e) {}
      var wait = Math.min(8000, 500 * Math.pow(2, Math.min(restarts - 1, 4)));
      restartT = setTimeout(function () {
        restartT = null;
        makeMuxer();
        conn();
      }, wait);
    }
    // The <video> element can also fail underneath MSE (decode error).
    var onVidErr = function () {
      dbg("jmuxer", {ev: "video_error", cam: cam.id,
                     code: (video.error && video.error.code) || 0});
      restart("video");
    };
    video.addEventListener("error", onVidErr);

    makeMuxer();
    stops.push(function () {
      video.removeEventListener("error", onVidErr);
      if (restartT) clearTimeout(restartT);
      try { if (jm) jm.destroy(); } catch (e) {}
    });

    var proto = location.protocol === "https:" ? "wss://" : "ws://";

    function conn() {
      wsUrl(cam.live_ws).then(function (url) { connTo(url); });
    }

    function connTo(url) {
      ws = new WebSocket(url);
      ws.binaryType = "arraybuffer";
      ws.onopen = function () { statusEl.textContent = "live"; };
      ws.onmessage = function (e) {
        if (typeof e.data === "string") {          // {"ts": <epoch-ms>} capture time
          try { var j = JSON.parse(e.data); if (j.ts && showTs) showTs(j.ts); } catch (x) {}
          return;
        }
        try {
          jm.feed({ video: new Uint8Array(e.data) });
        } catch (x) {                       // muxer died mid-feed
          dbg("jmuxer", {ev: "feed_throw", cam: cam.id});
          restart("feed");
          return;
        }
        n++;
        if (Date.now() - t0 > 1000) {
          statusEl.textContent = "live ~" + n + " fps"; n = 0; t0 = Date.now();
          restarts = 0;                     // healthy again: clear the backoff
        }
      };
      ws.onclose = function () {
        if (closed) return;
        statusEl.textContent = "reconnecting…";
        explainReconnect();
        retry = setTimeout(conn, 1000);
      };
      ws.onerror = function () { try { ws.close(); } catch (e) {} };
    }
    conn();
    stops.push(function () { closed = true; clearTimeout(retry); try { ws.close(); } catch (e) {} });
  }

  function startHLS(cam, video, statusEl, stops, showTs) {
    // iPhone/Safari path — native HLS in the same <video> element.  Needs an
    // explicit load()+play() (muted autoplay is allowed on iOS) and precise
    // error reporting so a failure isn't just a black frame.
    statusEl.textContent = "connecting…";
    video.muted = true;                 // iOS checks the PROPERTY to allow muted autoplay
    video.setAttribute("playsinline", "");
    video.src = cam.hls_url;
    var ERR = { 1: "aborted", 2: "network", 3: "decode", 4: "unsupported" };
    var onPlaying = function () { statusEl.textContent = "live · HLS"; recN = 0; };
    var onWaiting = function () { statusEl.textContent = "buffering…";
      dbg("hls", {ev: "stall", cam: cam.id}); };
    var onLoaded  = function () { var p = video.play(); if (p && p.catch) p.catch(function () {}); };
    // ── resilience ────────────────────────────────────────────────────────
    // A live HLS window is inherently lossy: a segment can be rolled out from
    // under a client that stalls for a moment, and the <video> element treats
    // that as FATAL (on iOS it surfaces as SRC_NOT_SUPPORTED(4)).  Never leave
    // the user on a dead player — reload the source with backoff, forever.
    var recN = 0, recT = null;
    function recover(reason) {
      if (recT) return;                       // one recovery in flight
      recN++;
      dbg("hls", {ev: "recover", why: reason, n: recN, cam: cam.id});
      statusEl.textContent = "reconnecting…";
      var wait = Math.min(8000, 500 * Math.pow(2, Math.min(recN - 1, 4)));
      recT = setTimeout(function () {
        recT = null;
        try { video.pause(); } catch (e) {}
        // cache-bust so a stale/404 playlist isn't re-served from cache
        video.src = cam.hls_url + (cam.hls_url.indexOf("?") < 0 ? "?" : "&") +
                    "_r=" + Date.now();
        try { video.load(); } catch (e) {}
        var p = video.play(); if (p && p.catch) p.catch(function () {});
      }, wait);
    }
    var onErr = function () {
      var c = (video.error && video.error.code) || 0;
      dbg("hls", {ev: "error", code: c, cam: cam.id,
                  src: String(video.currentSrc || "").slice(-40)});
      recover("err" + c);
    };
    // capture time = playlist PROGRAM-DATE-TIME start + playback position
    // (native HLS on Safari exposes it via getStartDate(); absent elsewhere)
    var onTime = function () {
      if (!showTs || typeof video.getStartDate !== "function") return;
      var sd = video.getStartDate();
      if (sd && !isNaN(sd.getTime())) showTs(sd.getTime() + video.currentTime * 1000);
    };
    video.addEventListener("timeupdate", onTime);
    video.addEventListener("playing", onPlaying);
    video.addEventListener("waiting", onWaiting);
    video.addEventListener("loadedmetadata", onLoaded);
    video.addEventListener("error", onErr);
    try { video.load(); } catch (e) {}
    var p0 = video.play(); if (p0 && p0.catch) p0.catch(function () {});
    stops.push(function () {
      if (recT) clearTimeout(recT);
      video.removeEventListener("timeupdate", onTime);
      video.removeEventListener("playing", onPlaying);
      video.removeEventListener("waiting", onWaiting);
      video.removeEventListener("loadedmetadata", onLoaded);
      video.removeEventListener("error", onErr);
      video.removeAttribute("src");
      try { video.load(); } catch (e) {}
    });
  }

  // Detection overlay (NPU boxes) — best-effort; silently no-ops if the camera
  // has no detector.  Works over both transports.
  function startOverlay(cam, video, canvas, stage, stops) {
    var ctx = canvas.getContext("2d");
    var detEl = stage.querySelector(".det");
    var stopped = false;

    // Draw only what the capture policy would act on: a box appears once
    // its score reaches that class's START threshold.  Below it the
    // detector is still guessing — the policy ignores those frames, so
    // showing them made the overlay flicker with objects that never
    // trigger anything.  Classes with no policy entry are always drawn
    // (an unconfigured class shouldn't silently vanish), and the count
    // of suppressed boxes stays visible so nothing disappears unexplained.
    var startBy = null;                     // {class: startThreshold}
    function loadPolicy() {
      api("/api/v1/cameras/" + encodeURIComponent(cam.id) + "/inference")
        .then(function (d) {
          var engines = ((d || {}).policy || {}).engines || {};
          var m = {};
          // service first, then edge — edge wins for a class in both,
          // since the boxes come from the camera's own detector
          ["service", "edge"].forEach(function (en) {
            var cls = (engines[en] || {}).classes || {};
            Object.keys(cls).forEach(function (k) {
              var s = cls[k] && cls[k].start;
              if (typeof s === "number") m[k] = s;
            });
          });
          startBy = m;
        }).catch(function () { startBy = startBy || {}; });
    }
    loadPolicy();
    var polTimer = setInterval(loadPolicy, 30000);
    stops.push(function () { clearInterval(polTimer); });
    function tick() {
      if (stopped) return;
      api(cam.detections).then(function (j) {
        var vw = j.w || 1280, vh = j.h || 720;
        var rect = video.getBoundingClientRect();
        canvas.width = rect.width; canvas.height = rect.height;
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        var scale = Math.min(canvas.width / vw, canvas.height / vh);
        var dw = vw * scale, dh = vh * scale;
        var ox = (canvas.width - dw) / 2, oy = (canvas.height - dh) / 2;
        var ds = j.detections || [];
        ctx.lineWidth = 2; ctx.font = "14px ui-monospace, monospace"; ctx.textBaseline = "bottom";
        var nb = 0, hidden = 0;
        ds.forEach(function (d) {
          var b = d.box; if (!b || b.length < 4) return;
          var thr = startBy ? startBy[d.cls] : undefined;
          if (typeof thr === "number" && typeof d.score === "number" &&
              d.score < thr) { hidden++; return; }
          nb++;
          var x = ox + b[0] * scale, y = oy + b[1] * scale, w = b[2] * scale, h = b[3] * scale;
          var col = d.cls === "person" ? "#38ff8f" : "#ffd23a";
          ctx.strokeStyle = col; ctx.strokeRect(x, y, w, h);
          ctx.fillStyle = col;
          ctx.fillText((d.cls || "") + " " + (d.score != null ? d.score.toFixed(2) : ""), x + 2, y - 2);
        });
        detEl.textContent = (j.active ? "● REC  " : "") + nb + " obj" +
          (hidden ? "  (" + hidden + " below threshold)" : "") +
          (j.yolo_ms ? "  yolo=" + j.yolo_ms + "ms" : "");
      }).catch(function () {});
    }
    var timer = setInterval(tick, 400);
    tick();
    stops.push(function () { stopped = true; clearInterval(timer); });
  }

  // ── events ─────────────────────────────────────────────────────────────────
  function viewEvents(camId) {
    setHeader("…", true);
    api("/api/v1/cameras").catch(function () { return []; }).then(function (cams) {
      var cam = findCam(cams, camId);
      setDeviceHeader("cam", camId, "#/events/");
      app.innerHTML = '<div class="empty">loading…</div>';
      api(cam.events_url).then(function (evs) {
        if (!evs.length) {
          app.innerHTML = '<div class="empty">no events recorded yet</div>';
          mountTabs("cam", camId, "events");
          return;
        }
        infoEl.textContent = evs.length + " events";
        var wrap = document.createElement("div");
        wrap.className = "events";
        var lastDay = "", lastHour = -1;
        evs.forEach(function (e) {
          if (e.day !== lastDay) {
            var d = document.createElement("div");
            d.className = "ev-day"; d.textContent = e.day;
            wrap.appendChild(d); lastDay = e.day; lastHour = -1;
          }
          var hh = new Date((e.at || 0) * 1000).getHours();
          if (e.at && hh !== lastHour) {
            var hEl = document.createElement("div");
            hEl.className = "ev-hour";
            hEl.textContent = String(hh).padStart(2, "0") + ":00";
            wrap.appendChild(hEl); lastHour = hh;
          }
          wrap.appendChild(eventRow(e));
        });
        app.innerHTML = "";
        app.appendChild(wrap);
        mountTabs("cam", camId, "events");
      }).catch(function (e) {
        app.innerHTML = '<div class="empty err">failed to load events: ' + esc(e.message) + "</div>";
        mountTabs("cam", camId, "events");
      });
    });
  }

  function fileUrl(e, name) {
    return "/api/v1/media/file/" + encodeURIComponent(e.day) + "/" +
      encodeURIComponent(e.event) + "/" + encodeURIComponent(name);
  }
  function evTime(ev) {
    var m = ev.match(/(\d{8})-(\d{6})/);            // ...YYYYMMDD-HHMMSS
    if (!m) return ev;
    var t = m[2];
    return t.slice(0, 2) + ":" + t.slice(2, 4) + ":" + t.slice(4, 6);
  }

  // ── event selection (multi-select + swipe actions) ───────────────────────
  // Selection mode turns on with the first selected row; every row then shows
  // a tick to the LEFT of the play icon, so the whole list is selectable
  // without hunting for a menu.
  var selMode = false, selected = {};   // "day/event" -> event object

  function selKey(e) { return e.day + "/" + e.event; }

  function selCount() { return Object.keys(selected).length; }

  function setSelMode(on) {
    if (selMode === on) return;
    selMode = on;
    document.body.classList.toggle("selmode", on);
    if (!on) { selected = {}; 
      [].forEach.call(document.querySelectorAll(".ev.sel"), function (r) {
        r.classList.remove("sel");
      });
    }
    updateBulkBar();
  }

  // Explicit rather than a toggle: the state machine needs "make selected"
  // (left swipe while selecting) and "make deselected" (right swipe)
  // separately, and only the tap inverts.
  function setSel(e, row, on) {
    var k = selKey(e);
    if (on) {
      if (row.collapsePlayback) row.collapsePlayback();   // playback -> select
      row.classList.remove("actions-open");
      selected[k] = e; row.classList.add("sel");
      setSelMode(true); updateBulkBar();
    } else {
      delete selected[k]; row.classList.remove("sel");
      if (selCount() === 0) setSelMode(false);            // last one -> normal
      else updateBulkBar();
    }
  }

  function updateBulkBar() {
    var bar = document.getElementById("bulkbar");
    if (!selMode) { if (bar) bar.remove(); return; }
    if (!bar) {
      bar = document.createElement("div");
      bar.id = "bulkbar";
      bar.innerHTML =
        '<span id="bulkn"></span>' +
        '<button class="bulkbtn" id="bulk-dl">⬇ Download</button>' +
        '<button class="bulkbtn danger" id="bulk-del">🗑 Delete</button>' +
        '<button class="bulkbtn" id="bulk-cancel">Cancel</button>';
      document.body.appendChild(bar);
      bar.querySelector("#bulk-cancel").onclick = function () { setSelMode(false); };
      bar.querySelector("#bulk-dl").onclick = function () {
        Object.keys(selected).forEach(function (k, i) {
          // stagger: browsers drop simultaneous programmatic downloads
          setTimeout(function () { downloadEvent(selected[k]); }, i * 400);
        });
      };
      bar.querySelector("#bulk-del").onclick = function () { deleteSelected(); };
    }
    bar.querySelector("#bulkn").textContent = selCount() + " selected";
  }

  function downloadEvent(e) {
    var mp4 = (e.files || []).filter(function (f) { return /\.mp4$/.test(f.name); })[0];
    var f = mp4 || (e.files || [])[0];
    if (!f) return;
    var a = document.createElement("a");
    a.href = fileUrl(e, f.name);
    a.download = e.event + "-" + f.name;
    document.body.appendChild(a); a.click(); a.remove();
  }

  function deleteSelected() {
    var items = Object.keys(selected).map(function (k) {
      return {day: selected[k].day, event: selected[k].event};
    });
    if (!items.length) return;
    if (!confirm("Delete " + items.length + " event" +
                 (items.length > 1 ? "s" : "") + "? This cannot be undone."))
      return;
    fetch("/api/v1/media/events/delete", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({events: items})
    }).then(function (r) { return r.json(); })
      .then(function (res) {
        (res.deleted || []).forEach(function (k) {
          var row = document.querySelector('.ev[data-key="' + k + '"]');
          if (row) row.remove();
        });
        if ((res.failed || []).length)
          alert("Some events could not be deleted:\n" + res.failed.join("\n"));
        setSelMode(false);
        router();                       // refresh counts/day headers
      })
      .catch(function () { alert("delete failed"); });
  }

  // Swipe: LEFT reveals delete/download actions, RIGHT selects.  Pointer
  // events cover touch and mouse; the row only moves horizontally once the
  // gesture is clearly horizontal, so vertical scrolling still works.
  function attachSwipe(row, e) {
    var x0 = 0, y0 = 0, dx = 0, dragging = false, decided = false, horiz = false;
    row.addEventListener("pointerdown", function (ev) {
      if (ev.pointerType === "mouse" && ev.button !== 0) return;
      x0 = ev.clientX; y0 = ev.clientY; dx = 0;
      dragging = true; decided = false; horiz = false;
      // capture: keep receiving moves even when the pointer leaves the row,
      // otherwise a fast drag ends mid-gesture
      try { row.setPointerCapture(ev.pointerId); } catch (e) {}
    });
    row.addEventListener("pointermove", function (ev) {
      if (!dragging) return;
      dx = ev.clientX - x0;
      var dy = ev.clientY - y0;
      if (!decided) {
        if (Math.abs(dx) < 8 && Math.abs(dy) < 8) return;
        decided = true; horiz = Math.abs(dx) > Math.abs(dy);
        if (horiz) row.classList.add("swiping");
      }
      if (!horiz) return;
      var shift = Math.max(-140, Math.min(0, dx));      // only LEFT opens
      row.style.transform = "translateX(" + shift + "px)";
    });
    function end() {
      if (!dragging) return;
      dragging = false;
      row.classList.remove("swiping");
      row.style.transform = "";
      if (!horiz) return;
      if (dx < -60) row.onSwipeLeft();
      else if (dx > 60) row.onSwipeRight();
      // Any HORIZONTAL gesture suppresses the click that may follow, so a
      // sloppy sideways drag never expands the row.  The flag self-clears:
      // Chrome emits no click after a long drag, and a flag left set would
      // silently eat the user's next real tap.
      row.swiped = true;
      clearTimeout(row._swT);
      row._swT = setTimeout(function () { row.swiped = false; }, 350);
    }
    row.addEventListener("pointerup", end);
    row.addEventListener("pointercancel", end);
  }

  function eventRow(e) {
    var row = document.createElement("div");
    row.className = "ev";
    var mp4 = e.files.filter(function (f) { return /\.mp4$/.test(f.name); })[0];
    var tot = e.files.reduce(function (a, f) { return a + f.bytes; }, 0);
    row.setAttribute("data-key", selKey(e));
    row.innerHTML =
      '<div class="tick" title="select"></div>' +
      '<div class="ico">▶</div>' +
      '<div class="meta"><b>' + esc(evTime(e.event)) + "</b>" +
      "<small>" + e.files.length + " files · " + fmtBytes(tot) + (mp4 ? " · video" : "") + "</small></div>" +
      '<div class="chev">›</div>' +
      '<div class="swipeacts">' +
        '<button class="sq dl" title="Download">⬇</button>' +
        '<button class="sq del" title="Delete">🗑</button>' +
      '</div>';
    attachSwipe(row, e);
    row.querySelector(".tick").onclick = function (ev) {
      ev.stopPropagation(); setSel(e, row, !selected[selKey(e)]);
    };
    row.querySelector(".swipeacts .dl").onclick = function (ev) {
      ev.stopPropagation(); downloadEvent(e); row.classList.remove("actions-open");
    };
    row.querySelector(".swipeacts .del").onclick = function (ev) {
      ev.stopPropagation();
      selected = {}; selected[selKey(e)] = e; deleteSelected();
    };

    // ── item state machine ────────────────────────────────────────────────
    // States: normal | option (action tray out) | playback (expanded) and the
    // LIST-WIDE select mode, which takes precedence while it is on.
    //   normal   : ← option        · → select (turns the whole list on) · tap playback
    //   option   : ← option (stay) · → normal (recover)                 · tap ignored
    //   select   : ← stay selected · → deselect (last one exits mode)   · tap inverts
    //   playback : ← option, keeps playing · → select, collapses        · tap normal
    var open = false, body = null;

    function closeTray() { row.classList.remove("actions-open"); }
    function isTray() { return row.classList.contains("actions-open"); }

    function collapse() {
      if (!open) return;
      open = false; row.classList.remove("open");
      if (body) { body.remove(); body = null; }
      row.querySelector(".chev").textContent = "›";
    }
    row.collapsePlayback = collapse;

    row.onSwipeLeft = function () {
      if (selMode) { setSel(e, row, true); return; }   // select: stays selected
      row.classList.add("actions-open");               // normal/playback -> option
    };
    row.onSwipeRight = function () {
      if (selMode) { setSel(e, row, false); return; }  // select: deselect
      if (isTray()) { closeTray(); return; }           // option -> normal
      collapse();                                      // playback -> select
      setSel(e, row, true);
    };
    row.onTap = function () {
      if (selMode) { setSel(e, row, !selected[selKey(e)]); return; }
      if (isTray()) return;                            // option: buttons only
      if (open) { collapse(); return; }                // playback -> normal
      expand();
    };
    row.onclick = function () {
      if (row.swiped) { row.swiped = false; return; }  // the gesture's own click
      row.onTap();
    };

    function expand() {
      open = true; row.classList.add("open");
      row.querySelector(".chev").textContent = "⌄";
      body = document.createElement("div");
      body.className = "ev-body";
      var h = "";
      if (mp4) h += '<video controls autoplay muted playsinline src="' + fileUrl(e, mp4.name) + '"></video>';
      else h += '<div class="empty">no video in this event</div>';
      h += '<div class="files">';
      e.files.forEach(function (f) {
        h += '<a class="dl" href="' + fileUrl(e, f.name) + '" download>' +
          esc(f.name) + " (" + fmtBytes(f.bytes) + ")</a>";
      });
      h += '</div><div class="log">loading log…</div>';
      body.innerHTML = h;
      row.after(body);

      var logf = e.files.filter(function (f) { return /\.(jsonl|json|log|txt)$/.test(f.name); })[0];
      var logEl = body.querySelector(".log");
      if (logf) {
        fetch(fileUrl(e, logf.name)).then(function (r) { return r.text(); }).then(function (t) {
          logEl.textContent = t.trim().split("\n").map(function (ln) {
            try {
              var o = JSON.parse(ln);
              return (o.ts || "") + "  motion=" + (o.motion != null ? o.motion : "-") +
                (o.detections ? "  " + JSON.stringify(o.detections) : "") +
                (o.start ? "  [START]" : "") + (o.end ? "  [END]" : "");
            } catch (_) { return ln; }
          }).join("\n");
        }).catch(function () { logEl.textContent = "(no log)"; });
      } else {
        logEl.textContent = "(no event log)";
      }
    }
    return row;
  }

  router();
})();
