/*
 * CaimanDB Studio — cliente 100% JS puro (sin build step).
 *
 * Todo lo que se muestra en esta interfaz viene de peticiones reales al
 * HTTP Query Server embebido en el propio nodo
 * (internal/caimandb/http_query.go):
 *
 *   GET  /entities   -> bases de datos y bloques reales (barra lateral)
 *   POST /query      -> ejecuta NQL de verdad; "rows"/"columns" vienen de
 *                        un FIND/GET real re-ejecutado en el motor
 *   GET  /status      -> métricas reales del proceso (panel "Dashboard")
 *   GET  /watch       -> stream de cambios reales (Server-Sent Events)
 *
 * No hay setInterval con Math.random en ningún sitio de este archivo:
 * cada número que se pinta salió de una respuesta HTTP real.
 *
 * Autenticación: HTTP Basic Auth con las credenciales del formulario de
 * conexión (el mismo usuario creado con CREATE USER). Solo vive en
 * memoria del script; nunca se persiste.
 */

(() => {
  "use strict";

  // ---------------------------------------------------------------------
  // Estado de sesión (en memoria, nunca persistido)
  // ---------------------------------------------------------------------
  const session = {
    baseUrl: "",
    authHeader: "",
    username: "",
    db: "default",
    dbs: [],
    blocks: [], // [{name, documents, size_bytes}] del db activo
    watchSource: null,
    statusPoll: null,
    lastStatus: null,
    lastStatusAt: 0,
    sessionTimer: null,
    connectedAt: 0,
  };

  const SESSION_TTL_MS = 30 * 60 * 1000; // 30 minutos

  const history = {
    opsPerSec: [], // [{t, v}]
    latencyMs: [],
    maxPoints: 30,
  };

  const qlog = { rows: 0 };

  // ---------------------------------------------------------------------
  // Referencias DOM
  // ---------------------------------------------------------------------
  const connectScreen = document.getElementById("connect-screen");
  const appEl = document.getElementById("app");
  const connectForm = document.getElementById("connectForm");
  const connectBtn = document.getElementById("connectBtn");
  const connectError = document.getElementById("connectError");

  const fHost = document.getElementById("fHost");
  const fPort = document.getElementById("fPort");
  const fDb = document.getElementById("fDb");
  const fUser = document.getElementById("fUser");
  const fPass = document.getElementById("fPass");
  const fTls = document.getElementById("fTls");

  const targetLabel = document.getElementById("targetLabel");
  const statusDot = document.getElementById("statusDot");
  const statusText = document.getElementById("statusText");
  const disconnectBtn = document.getElementById("disconnectBtn");
  const railAvatar = document.getElementById("railAvatar");

  const dbSelect = document.getElementById("dbSelect");
  const refreshEntitiesBtn = document.getElementById("refreshEntities");
  const filterBlocksInput = document.getElementById("filterBlocksInput");
  const entitiesList = document.getElementById("entitiesList");
  const entityCount = document.getElementById("entityCount");
  const addBlockBtn = document.getElementById("addBlockBtn");

  const watchToggle = document.getElementById("watchToggle");
  const watchLog = document.getElementById("watchLog");

  const iconRail = document.getElementById("iconRail");
  const viewQuery = document.getElementById("view-query");
  const viewDashboard = document.getElementById("view-dashboard");

  const tabsBar = document.getElementById("tabsBar");
  const btnRun = document.getElementById("btnRun");
  const btnFormat = document.getElementById("btnFormat");
  const btnClear = document.getElementById("btnClear");
  const quickCmds = document.getElementById("quickCmds");
  const promptDb = document.getElementById("promptDb");
  const connTag = document.getElementById("connTag");
  const connTagText = document.getElementById("connTagText");

  const rtabResult = document.getElementById("rtabResult");
  const rtabLog = document.getElementById("rtabLog");
  const resultPane = document.getElementById("resultPane");
  const logPane = document.getElementById("logPane");
  const qlogBody = document.getElementById("qlogBody");
  const qlogCount = document.getElementById("qlogCount");
  const btnCopyResult = document.getElementById("btnCopyResult");
  const resultsPanel = document.getElementById("resultsPanel");
  const btnToggleResults = document.getElementById("btnToggleResults");

  const statRows = document.getElementById("statRows");
  const statTime = document.getElementById("statTime");
  const statStatus = document.getElementById("statStatus");

  const dashClock = document.getElementById("dashClock");
  const dashNodeBadge = document.getElementById("dashNodeBadge");
  const kpiGrid = document.getElementById("kpiGrid");
  const healthGrid = document.getElementById("healthGrid");

  // If served by CaimanDB's own query server (the normal case:
  // http://host:QueryPort/), prefill the connect form from the page's own
  // origin instead of the hardcoded defaults.
  if (location.protocol !== "file:" && location.hostname) {
    fHost.value = location.hostname;
    if (location.port) fPort.value = location.port;
    fTls.checked = location.protocol === "https:";
  }

  // ---------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------
  function buildBaseUrl() {
    const scheme = fTls.checked ? "https" : "http";
    const host = fHost.value.trim() || "localhost";
    const port = fPort.value.trim() || "1555";
    return `${scheme}://${host}:${port}`;
  }

  async function apiFetch(path, opts = {}) {
    return fetch(session.baseUrl + path, {
      ...opts,
      headers: { Authorization: session.authHeader, ...(opts.headers || {}) },
    });
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function setStatus(state, text) {
    statusDot.classList.remove("ok", "err");
    if (state) statusDot.classList.add(state);
    statusText.textContent = text;
  }

  function fmtCell(v) {
    if (v === null || v === undefined) return "";
    if (typeof v === "object") return JSON.stringify(v);
    return String(v);
  }

  function fmtDuration(ms) {
    if (ms < 1000) return `${ms.toFixed(1)} ms`;
    return `${(ms / 1000).toFixed(2)} s`;
  }

  function fmtUptime(seconds) {
    seconds = Math.floor(seconds);
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  }

  // ---------------------------------------------------------------------
  // NQL: editor CodeMirror con resaltado de palabras clave reales de NQL
  // ---------------------------------------------------------------------
  const NQL_KEYWORDS = [
    "CREATE", "DROP", "RENAME", "TO", "USE", "SHOW", "DBS", "INFO", "DB",
    "DESCRIBE", "STATS", "SIZE", "COMPACT", "ANALYZE", "OPTIMIZE", "BACKUP",
    "RESTORE", "FROM", "BLOCK", "BLOCKS", "EMPTY", "CLEAR", "REBUILD",
    "CHECK", "REPAIR", "INSERT", "FIND", "SELECT", "ONLY", "EXCLUDE",
    "ORDER", "DESC", "ASC", "LIMIT", "OFFSET", "WHERE", "AND", "OR", "NOT",
    "LIKE", "IN", "BETWEEN", "IS", "NULL", "GET", "SEARCH", "EXACT",
    "FUZZY", "SIMILAR", "WITH", "SCORE", "MATCHES", "PHRASE", "UPDATE",
    "SET", "INC", "PUSH", "PULL", "ALL", "DELETE", "COUNT", "SUM", "AVG",
    "MIN", "MAX", "MEDIAN", "MODE", "STDDEV", "GROUP", "BY", "HAVING",
    "BEGIN", "COMMIT", "ROLLBACK", "ABORT", "TX", "STATUS", "LIST",
    "ISOLATION", "JOIN", "ON", "VIEW", "EXPORT", "IMPORT", "USERS", "USER",
    "ROLE", "HELP", "EXIT", "QUIT", "PING", "VERSION", "HEALTH", "CACHE",
    "INDEXES",
  ];

  if (window.CodeMirror && CodeMirror.defineSimpleMode) {
    CodeMirror.defineSimpleMode("nql", {
      start: [
        { regex: /"(?:[^\\"]|\\.)*"?/, token: "string" },
        { regex: /'(?:[^\\']|\\.)*'?/, token: "string" },
        { regex: /--.*/, token: "comment" },
        { regex: /\b\d+\.?\d*\b/, token: "number" },
        {
          regex: new RegExp("\\b(?:" + NQL_KEYWORDS.join("|") + ")\\b", "i"),
          token: "nql-keyword",
        },
        { regex: /[=<>!]+/, token: "operator" },
      ],
    });
  }

  let cm = null;
  if (window.CodeMirror) {
    cm = CodeMirror.fromTextArea(document.getElementById("sqlEditor"), {
      mode: "nql",
      theme: "default",
      lineNumbers: true,
      matchBrackets: true,
      autoCloseBrackets: true,
      styleActiveLine: true,
      indentUnit: 2,
      extraKeys: {
        "Ctrl-Enter": () => runActiveTab(),
        "Cmd-Enter": () => runActiveTab(),
      },
    });
    cm.on("change", () => {
      const t = activeTab();
      if (t) t.content = cm.getValue();
    });
  }

  // ---------------------------------------------------------------------
  // Pestañas de consulta — cada una tiene su propio texto NQL y su
  // propio último resultado real; cambiar de pestaña realmente cambia
  // lo que se ve, no es solo cosmético.
  // ---------------------------------------------------------------------
  let tabs = [];
  let activeTabId = null;
  let tabSeq = 0;

  function activeTab() {
    return tabs.find((t) => t.id === activeTabId) || null;
  }

  function addTab(content = "") {
    tabSeq += 1;
    const tab = { id: `t${tabSeq}`, title: `Consulta ${tabSeq}`, content, result: null };
    tabs.push(tab);
    switchTab(tab.id);
    return tab;
  }

  function closeTab(id) {
    const idx = tabs.findIndex((t) => t.id === id);
    if (idx === -1) return;
    tabs.splice(idx, 1);
    if (tabs.length === 0) {
      addTab();
      return;
    }
    if (activeTabId === id) {
      switchTab(tabs[Math.max(0, idx - 1)].id);
    } else {
      renderTabsBar();
    }
  }

  function switchTab(id) {
    const prev = activeTab();
    if (prev && cm) prev.content = cm.getValue();
    activeTabId = id;
    const t = activeTab();
    if (t && cm) cm.setValue(t.content || "");
    renderTabsBar();
    renderResult(t ? t.result : null);
  }

  function renderTabsBar() {
    tabsBar.innerHTML = "";
    tabs.forEach((t) => {
      const div = document.createElement("div");
      div.className = "tab" + (t.id === activeTabId ? " active" : "");
      div.innerHTML = `<i class="ti ti-code"></i><span class="tab-title">${escapeHtml(t.title)}</span>${
        tabs.length > 1 ? '<i class="ti ti-x xbtn"></i>' : ""
      }`;
      div.addEventListener("click", (ev) => {
        if (ev.target.classList.contains("xbtn")) {
          ev.stopPropagation();
          closeTab(t.id);
        } else {
          switchTab(t.id);
        }
      });
      tabsBar.appendChild(div);
    });
    const addBtn = document.createElement("button");
    addBtn.className = "tab-add";
    addBtn.title = "Nueva pestaña de consulta";
    addBtn.innerHTML = '<i class="ti ti-plus"></i>';
    addBtn.addEventListener("click", () => addTab());
    tabsBar.appendChild(addBtn);
  }

  // ---------------------------------------------------------------------
  // Conexión / login
  // ---------------------------------------------------------------------
  connectForm.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    connectError.hidden = true;
    connectBtn.disabled = true;
    connectBtn.querySelector("span").textContent = "Conectando…";

    const baseUrl = buildBaseUrl();
    const username = fUser.value.trim();
    const password = fPass.value;
    const db = fDb.value.trim() || "default";
    const authHeader = "Basic " + btoa(`${username}:${password}`);

    try {
      // /status responde igual en el puerto de consultas (1555 por
      // defecto) y en el puerto de administración (1556 por defecto):
      // ambos exponen las mismas rutas de consola (/query, /status,
      // /health, /watch, /entities) con CORS habilitado, así que
      // cualquiera de los dos puertos funciona aquí.
      const res = await fetch(baseUrl + "/status", { headers: { Authorization: authHeader } });
      if (res.status === 401) throw new Error("Usuario o contraseña incorrectos.");
      if (!res.ok) throw new Error(`El servidor respondió ${res.status}.`);
      const status = await res.json();

      session.baseUrl = baseUrl;
      session.authHeader = authHeader;
      session.username = username;
      session.db = db;
      session.connectedAt = Date.now();

      targetLabel.textContent = `${fHost.value}:${fPort.value} · nodo ${status.node_id || "—"}`;
      railAvatar.textContent = (username || "?").slice(0, 2);
      railAvatar.title = username;
      dashNodeBadge.textContent = `${status.node_id || "—"} · v${status.version || "—"}`;

      connectScreen.style.display = "none";
      appEl.classList.add("visible");
      setStatus("ok", "conectado");
      connTag.classList.remove("err");
      connTagText.textContent = "conectado";

      if (tabs.length === 0) addTab("SHOW BLOCKS");
      await loadEntities();
      applyStatus(status);
      startStatusPolling();
      scheduleSessionTimeout();
    } catch (err) {
      connectError.textContent = describeConnectError(err, baseUrl);
      connectError.hidden = false;
      setStatus("err", "sin conexión");
    } finally {
      connectBtn.disabled = false;
      connectBtn.querySelector("span").textContent = "Conectar";
    }
  });

  // fetch() lanza un TypeError genérico ("Failed to fetch" / "Load
  // failed") tanto si el host/puerto está caído como si el navegador
  // bloqueó la petición por CORS — sin distinción. Como ya no hay nada
  // que bloquear (ambos puertos responden con CORS habilitado), en la
  // práctica esto casi siempre significa que el nodo no está escuchando
  // en ese host:puerto todavía, así que el mensaje lo dice explícito en
  // vez de repetir el texto críptico del navegador.
  function describeConnectError(err, baseUrl) {
    if (err instanceof TypeError) {
      return `No se pudo contactar ${baseUrl}. Verifica que CaimanDB esté ` +
        `arrancado y que el host/puerto sean correctos (puerto de consultas, ` +
        `1555 por defecto, o de administración, 1556 por defecto — ambos sirven ` +
        `la consola).`;
    }
    return err.message || "No se pudo conectar con el nodo.";
  }

  function scheduleSessionTimeout() {
    clearSessionTimeout();
    session.sessionTimer = setTimeout(() => {
      doDisconnect("Tu sesión expiró (30 minutos desde el inicio de sesión). Vuelve a iniciar sesión.");
    }, SESSION_TTL_MS);
  }
  function clearSessionTimeout() {
    if (session.sessionTimer) clearTimeout(session.sessionTimer);
    session.sessionTimer = null;
  }

  function doDisconnect(message) {
    stopWatch();
    stopStatusPolling();
    clearSessionTimeout();
    session.baseUrl = "";
    session.authHeader = "";
    session.connectedAt = 0;
    session.lastStatus = null;
    session.blocks = [];
    session.dbs = [];
    history.opsPerSec = [];
    history.latencyMs = [];
    history._prevOps = null;
    qlog.rows = 0;
    qlogBody.innerHTML = "";
    qlogCount.textContent = "0 consultas en esta sesión";
    fPass.value = "";
    tabs = [];
    activeTabId = null;
    kpiGrid.dataset.built = "";
    closeResults();
    appEl.classList.remove("visible");
    connectScreen.style.display = "flex";
    if (message) {
      connectError.textContent = message;
      connectError.hidden = false;
    } else {
      connectError.hidden = true;
    }
  }

  disconnectBtn.addEventListener("click", () => doDisconnect());

  // ---------------------------------------------------------------------
  // Entidades reales: bases de datos + bloques (/entities)
  // ---------------------------------------------------------------------
  async function loadEntities() {
    try {
      const res = await apiFetch(`/entities?db=${encodeURIComponent(session.db)}`);
      if (!res.ok) return;
      const data = await res.json();
      session.dbs = data.dbs || [];
      session.blocks = data.blocks || [];
      renderDbSelect();
      renderEntities();
      promptDb.textContent = `${session.db}>`;
    } catch {
      /* el nodo puede estar reiniciando; no interrumpe la UI */
    }
  }

  function renderDbSelect() {
    dbSelect.innerHTML = session.dbs
      .map((d) => `<option value="${escapeHtml(d)}" ${d === session.db ? "selected" : ""}>${escapeHtml(d)}</option>`)
      .join("") || `<option>${escapeHtml(session.db)}</option>`;
  }

  dbSelect.addEventListener("change", async () => {
    session.db = dbSelect.value;
    await loadEntities();
  });

  refreshEntitiesBtn.addEventListener("click", () => {
    refreshEntitiesBtn.classList.add("spin");
    loadEntities().finally(() => setTimeout(() => refreshEntitiesBtn.classList.remove("spin"), 400));
  });

  function renderEntities() {
    const filter = filterBlocksInput.value.trim().toLowerCase();
    const visible = session.blocks.filter((b) => b.name.toLowerCase().includes(filter));
    entityCount.textContent = session.blocks.length;

    if (session.blocks.length === 0) {
      entitiesList.innerHTML = `<p class="entities-empty">No hay bloques en "${escapeHtml(session.db)}" todavía. Usa "+ Bloque" para crear uno.</p>`;
      return;
    }
    if (visible.length === 0) {
      entitiesList.innerHTML = `<p class="entities-empty">Sin coincidencias.</p>`;
      return;
    }

    entitiesList.innerHTML = visible
      .map(
        (b) => `<div class="entity-row" data-block="${escapeHtml(b.name)}">
          <i class="ti ti-table"></i><span>${escapeHtml(b.name)}</span>
          <span class="ent-count">${b.documents ?? "—"}</span>
        </div>`
      )
      .join("");

    entitiesList.querySelectorAll(".entity-row").forEach((row) => {
      row.addEventListener("click", () => {
        const block = row.dataset.block;
        showView("query");
        const t = activeTab() || addTab();
        const nql = `FIND ${block} LIMIT 50`;
        t.content = nql;
        if (cm) cm.setValue(nql);
        runActiveTab();
      });
    });
  }

  filterBlocksInput.addEventListener("input", renderEntities);

  addBlockBtn.addEventListener("click", async () => {
    const name = prompt("Nombre del nuevo bloque:");
    if (!name || !name.trim()) return;
    const res = await apiFetch(
      `/query?db=${encodeURIComponent(session.db)}&query=${encodeURIComponent(`CREATE BLOCK ${name.trim()}`)}`
    );
    await res.json().catch(() => null);
    await loadEntities();
  });

  // ---------------------------------------------------------------------
  // Vista Query vs Dashboard (rail de iconos) — cambio real, no decorativo
  // ---------------------------------------------------------------------
  iconRail.addEventListener("click", (ev) => {
    const btn = ev.target.closest(".rail-btn[data-view]");
    if (!btn) return;
    showView(btn.dataset.view);
  });

  function showView(view) {
    iconRail.querySelectorAll(".rail-btn[data-view]").forEach((b) => {
      b.classList.toggle("active", b.dataset.view === view);
    });
    if (view === "dashboard") {
      viewQuery.style.display = "none";
      viewDashboard.style.display = "flex";
      refreshDashboardOnce();
    } else {
      viewQuery.style.display = "flex";
      viewDashboard.style.display = "none";
      if (cm) cm.refresh();
    }
  }

  // ---------------------------------------------------------------------
  // Ejecutar NQL contra /query — resultado real (texto o grilla real)
  // ---------------------------------------------------------------------
  async function runActiveTab() {
    const t = activeTab();
    if (!t || !session.baseUrl) return;
    const nql = (cm ? cm.getValue() : t.content).trim();
    if (!nql) return;

    btnRun.disabled = true;
    openResults();
    statStatus.textContent = "ejecutando…";
    statStatus.className = "badge";
    statTime.textContent = "—";
    const started = performance.now();
    let elapsed = 0;
    let outcome = { ok: false };

    try {
      const url = `/query?db=${encodeURIComponent(session.db)}&query=${encodeURIComponent(nql)}`;
      const res = await apiFetch(url);
      elapsed = performance.now() - started;

      if (res.status === 401) {
        outcome = { ok: false, error: "Credenciales rechazadas (401). La sesión pudo expirar." };
      } else {
        const body = await res.json().catch(() => null);
        if (!res.ok || (body && body.success === false)) {
          outcome = { ok: false, error: (body && (body.error || body.result)) || `Error HTTP ${res.status}` };
        } else {
          outcome = { ok: true, body };
        }
      }
    } catch (err) {
      elapsed = performance.now() - started;
      outcome = { ok: false, error: `Error de red: ${err.message}` };
    } finally {
      btnRun.disabled = false;
    }

    t.result = { nql, elapsed, outcome };
    renderResult(t.result);
    logQuery(nql, elapsed, outcome);
    openResults();
  }

  btnRun.addEventListener("click", runActiveTab);

  btnFormat.addEventListener("click", () => {
    if (!cm) return;
    const kwRe = new RegExp("\\b(" + NQL_KEYWORDS.join("|") + ")\\b", "gi");
    const parts = cm.getValue().split(/("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/g);
    const formatted = parts
      .map((part, i) => (i % 2 === 1 ? part : part.replace(kwRe, (m) => m.toUpperCase())))
      .join("");
    cm.setValue(formatted);
    const t = activeTab();
    if (t) t.content = formatted;
  });

  btnClear.addEventListener("click", () => {
    if (!cm) return;
    cm.setValue("");
    const t = activeTab();
    if (t) t.content = "";
  });

  quickCmds.addEventListener("click", (ev) => {
    const btn = ev.target.closest("button[data-cmd]");
    if (!btn) return;
    const t = activeTab() || addTab();
    t.content = btn.dataset.cmd;
    if (cm) cm.setValue(btn.dataset.cmd);
    runActiveTab();
  });

  function renderResult(result) {
    if (!result) {
      resultPane.innerHTML = `<p class="result-empty">Escribe un comando NQL y pulsa Ejecutar.</p>`;
      statRows.textContent = "—";
      statTime.textContent = "—";
      statStatus.textContent = "—";
      statStatus.className = "badge";
      return;
    }

    const { elapsed, outcome } = result;
    statTime.textContent = fmtDuration(elapsed);

    if (!outcome.ok) {
      resultPane.innerHTML = `<p class="result-error">${escapeHtml(outcome.error)}</p>`;
      statRows.textContent = "—";
      statStatus.textContent = "error";
      statStatus.className = "badge err";
      return;
    }

    const body = outcome.body;
    if (Array.isArray(body.rows) && Array.isArray(body.columns)) {
      statRows.textContent = `${body.rows.length} fila${body.rows.length === 1 ? "" : "s"}`;
      resultPane.innerHTML = renderTable(body.rows, body.columns);
    } else {
      statRows.textContent = "—";
      resultPane.innerHTML = `<pre class="result-text">${escapeHtml(prettyResult(body.result))}</pre>`;
    }
    statStatus.textContent = "ok";
    statStatus.className = "badge ok";
  }

  function renderTable(rows, columns) {
    if (rows.length === 0) return `<p class="result-empty">Sin documentos.</p>`;
    const head = columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
    const body = rows
      .map((r) => {
        const cells = columns
          .map((c) => {
            let v;
            if (c === "_id") v = r._id;
            else if (c === "_shard_id") v = r._shard_id;
            else v = r.data ? r.data[c] : undefined;
            return `<td title="${escapeHtml(fmtCell(v))}">${escapeHtml(fmtCell(v))}</td>`;
          })
          .join("");
        return `<tr>${cells}</tr>`;
      })
      .join("");
    return `<table class="result-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  }

  function prettyResult(result) {
    if (result === null || result === undefined) return "";
    if (typeof result === "string") return result;
    try {
      return JSON.stringify(result, null, 2);
    } catch {
      return String(result);
    }
  }

  btnCopyResult.addEventListener("click", () => {
    const t = activeTab();
    if (!t || !t.result || !t.result.outcome.ok) return;
    const text = t.result.outcome.body.rows
      ? JSON.stringify(t.result.outcome.body.rows, null, 2)
      : prettyResult(t.result.outcome.body.result);
    navigator.clipboard?.writeText(text).catch(() => {});
  });

  // Pestañas "Resultado" / "Historial de esta sesión"
  function openResults() { resultsPanel.classList.add("open"); }
  function closeResults() { resultsPanel.classList.remove("open"); }
  function toggleResults() { resultsPanel.classList.toggle("open"); }

  btnToggleResults.addEventListener("click", (ev) => {
    ev.stopPropagation();
    toggleResults();
  });
  // El resto de la cabecera (fuera de las pestañas/acciones) también
  // funciona como tirador del cajón, como en cualquier panel plegable.
  document.getElementById("rpHeader").addEventListener("click", (ev) => {
    if (ev.target.closest(".rtab") || ev.target.closest(".rp-actions")) return;
    toggleResults();
  });

  rtabResult.addEventListener("click", () => {
    rtabResult.classList.add("active");
    rtabLog.classList.remove("active");
    resultPane.style.display = "";
    logPane.style.display = "none";
    openResults();
  });
  rtabLog.addEventListener("click", () => {
    rtabLog.classList.add("active");
    rtabResult.classList.remove("active");
    resultPane.style.display = "none";
    logPane.style.display = "";
    openResults();
  });

  function logQuery(nql, elapsedMs, outcome) {
    qlog.rows += 1;
    const rowCount =
      outcome.ok && Array.isArray(outcome.body.rows) ? outcome.body.rows.length : outcome.ok ? "—" : "—";
    const status = outcome.ok ? "ok" : "err";
    const now = new Date();
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${now.toLocaleTimeString([], { hour12: false })}</td>
      <td><span class="qsnippet">${escapeHtml(nql)}</span></td>
      <td>${rowCount}</td>
      <td>${fmtDuration(elapsedMs)}</td>
      <td><span class="badge ${status}">${status === "ok" ? "ok" : "error"}</span></td>`;
    qlogBody.prepend(tr);
    while (qlogBody.children.length > 200) qlogBody.removeChild(qlogBody.lastChild);
    qlogCount.textContent = `${qlog.rows} consulta${qlog.rows === 1 ? "" : "s"} en esta sesión`;
  }

  // ---------------------------------------------------------------------
  // Dashboard: /status real, con historial acumulado en el navegador
  // (nada de datos sintéticos — si el nodo no reporta un campo, el KPI
  // simplemente no aparece).
  // ---------------------------------------------------------------------
  const KPI_DEFS = [
    { key: "databases", label: "Bases de datos", icon: "ti-database", color: "var(--acc)" },
    { key: "blocks", label: "Bloques (db activa)", icon: "ti-stack-2", color: "var(--mauve)" },
    { key: "ops", label: "Operaciones totales", icon: "ti-bolt", color: "var(--yellow)" },
    { key: "queries", label: "Consultas", icon: "ti-terminal-2", color: "var(--green)" },
    { key: "avgMs", label: "Latencia media", icon: "ti-gauge", color: "var(--teal)" },
    { key: "hitRatio", label: "Aciertos de caché", icon: "ti-server-2", color: "var(--acc2)" },
    { key: "uptime", label: "Tiempo activo", icon: "ti-clock", color: "var(--red)" },
  ];

  function applyStatus(status) {
    session.lastStatus = status;
    session.lastStatusAt = Date.now();

    const values = {
      databases: status.databases ?? "—",
      blocks: session.blocks.length,
      ops: status.ops ?? "—",
      queries: (status.metrics && status.metrics.queries) ?? "—",
      avgMs: status.metrics ? `${Number(status.metrics.avg_ms || 0).toFixed(1)} ms` : "—",
      hitRatio: (status.l1_cache && status.l1_cache.hit_ratio) || "—",
      uptime: typeof status.startup_duration === "number" ? fmtUptime(status.startup_duration) : "—",
    };

    if (!kpiGrid.dataset.built) {
      kpiGrid.innerHTML = KPI_DEFS.map(
        (k) => `<div class="kpi-card" style="--kpi-c:${k.color}">
          <div class="kpi-top">
            <div class="kpi-icon"><i class="ti ${k.icon}"></i></div>
          </div>
          <div class="kpi-value" id="kpi-${k.key}">—</div>
          <div class="kpi-label">${k.label}</div>
        </div>`
      ).join("");
      kpiGrid.dataset.built = "1";
    }
    KPI_DEFS.forEach((k) => {
      const el = document.getElementById(`kpi-${k.key}`);
      if (el) el.textContent = values[k.key];
    });

    // Historial derivado de contadores reales (no aleatorio): ops/s se
    // calcula como delta del contador total de operaciones entre dos
    // muestreos reales.
    const now = Date.now();
    const prev = history._prevOps;
    if (prev && typeof status.ops === "number") {
      const dtSec = (now - prev.t) / 1000;
      const opsPerSec = dtSec > 0 ? Math.max(0, (status.ops - prev.v) / dtSec) : 0;
      pushHistory(history.opsPerSec, opsPerSec);
    }
    if (typeof status.ops === "number") history._prevOps = { t: now, v: status.ops };

    if (status.metrics && typeof status.metrics.avg_ms === "number") {
      pushHistory(history.latencyMs, status.metrics.avg_ms);
    }

    renderCharts(status);
    renderHealth(status);

    dashClock.textContent = new Date().toLocaleTimeString([], { hour12: false });
    if (status.node_id) dashNodeBadge.textContent = `${status.node_id} · v${status.version || "—"}`;
  }

  function pushHistory(arr, v) {
    arr.push(v);
    while (arr.length > history.maxPoints) arr.shift();
  }

  let chartOps, chartBlocks, chartLatency;
  const CHART_COLORS = { acc: "#89b4fa", mauve: "#cba6f7", green: "#a6e3a1", grid: "rgba(255,255,255,.05)" };

  function ensureCharts() {
    if (!window.Chart) return;
    Chart.defaults.color = "#6c7086";
    Chart.defaults.font.family = "'Inter', sans-serif";
    Chart.defaults.font.size = 10.5;

    if (!chartOps) {
      chartOps = new Chart(document.getElementById("chartOps"), {
        type: "line",
        data: { labels: [], datasets: [{ label: "ops/s", data: [], borderColor: CHART_COLORS.acc, backgroundColor: "rgba(137,180,250,.12)", fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }] },
        options: {
          responsive: true,
          animation: false,
          plugins: { legend: { display: false } },
          scales: { x: { grid: { color: CHART_COLORS.grid } }, y: { grid: { color: CHART_COLORS.grid }, beginAtZero: true } },
        },
      });
    }
    if (!chartLatency) {
      chartLatency = new Chart(document.getElementById("chartLatency"), {
        type: "line",
        data: { labels: [], datasets: [{ label: "ms", data: [], borderColor: CHART_COLORS.mauve, backgroundColor: "rgba(203,166,247,.12)", fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }] },
        options: {
          responsive: true,
          animation: false,
          plugins: { legend: { display: false } },
          scales: { x: { grid: { color: CHART_COLORS.grid } }, y: { grid: { color: CHART_COLORS.grid }, beginAtZero: true } },
        },
      });
    }
    if (!chartBlocks) {
      chartBlocks = new Chart(document.getElementById("chartBlocks"), {
        type: "bar",
        data: { labels: [], datasets: [{ data: [], backgroundColor: CHART_COLORS.green, borderRadius: 4, maxBarThickness: 18 }] },
        options: {
          indexAxis: "y",
          responsive: true,
          animation: false,
          plugins: { legend: { display: false } },
          scales: { x: { grid: { color: CHART_COLORS.grid }, beginAtZero: true }, y: { grid: { display: false } } },
        },
      });
    }
  }

  function renderCharts(status) {
    ensureCharts();
    if (!window.Chart) return;

    const labels = history.opsPerSec.map((_, i) => "");
    chartOps.data.labels = labels;
    chartOps.data.datasets[0].data = history.opsPerSec;
    chartOps.update("none");

    chartLatency.data.labels = history.latencyMs.map((_, i) => "");
    chartLatency.data.datasets[0].data = history.latencyMs;
    chartLatency.update("none");

    const top = [...session.blocks].sort((a, b) => (b.documents || 0) - (a.documents || 0)).slice(0, 10);
    chartBlocks.data.labels = top.map((b) => b.name);
    chartBlocks.data.datasets[0].data = top.map((b) => b.documents || 0);
    chartBlocks.update("none");
  }

  function ring(pct, color) {
    const r = 15, c = 2 * Math.PI * r;
    const off = c - (Math.max(0, Math.min(100, pct)) / 100) * c;
    return `<svg class="ring" viewBox="0 0 36 36">
      <circle class="bg" cx="18" cy="18" r="${r}"></circle>
      <circle class="fg" cx="18" cy="18" r="${r}" stroke="${color}" stroke-dasharray="${c}" stroke-dashoffset="${off}"></circle>
    </svg>`;
  }

  function renderHealth(status) {
    const l1 = status.l1_cache || {};
    const items = [];

    if (typeof l1.used_bytes === "number" && typeof l1.max_bytes === "number" && l1.max_bytes > 0) {
      const pct = (l1.used_bytes / l1.max_bytes) * 100;
      items.push({ name: "Uso de caché", pct, label: `${l1.used || "—"} / ${l1.max || "—"}`, color: "var(--acc)" });
    }
    if (typeof l1.hit_ratio_pct === "number") {
      items.push({ name: "Aciertos", pct: l1.hit_ratio_pct, label: l1.hit_ratio || "—", color: "var(--green)" });
    }
    if (status.metrics && typeof status.metrics.queries === "number" && status.metrics.queries > 0) {
      const errPct = (Number(status.metrics.errors || 0) / status.metrics.queries) * 100;
      items.push({ name: "Tasa de error", pct: errPct, label: `${errPct.toFixed(2)}%`, color: "var(--red)" });
    }

    if (items.length === 0) {
      healthGrid.innerHTML = `<p class="result-empty">Sin datos de caché todavía.</p>`;
      return;
    }

    healthGrid.innerHTML = items
      .map(
        (h) => `<div class="health-item">
          <div class="hi-top"><div class="hi-name">${h.name}</div><div class="hi-pct">${h.label}</div></div>
          <div class="ring-row">${ring(h.pct, h.color)}<div class="bar-track"><div class="bar-fill" style="width:${Math.max(0, Math.min(100, h.pct))}%;background:${h.color}"></div></div></div>
        </div>`
      )
      .join("");
  }

  async function pollStatus() {
    if (!session.baseUrl) return;
    try {
      const res = await apiFetch("/status");
      if (!res.ok) return;
      const status = await res.json();
      await loadEntities();
      applyStatus(status);
    } catch {
      /* fallo de red puntual: no interrumpe la sesión */
    }
  }

  function startStatusPolling() {
    stopStatusPolling();
    pollStatus();
    session.statusPoll = setInterval(pollStatus, 5000);
  }
  function stopStatusPolling() {
    if (session.statusPoll) clearInterval(session.statusPoll);
    session.statusPoll = null;
  }
  function refreshDashboardOnce() {
    if (session.lastStatus) {
      renderCharts(session.lastStatus);
      renderHealth(session.lastStatus);
    }
    pollStatus();
  }

  // ---------------------------------------------------------------------
  // Eventos en vivo (/watch, Server-Sent Events) — reales
  // ---------------------------------------------------------------------
  function addWatchEntry(html) {
    const entry = document.createElement("div");
    entry.className = "watch-entry";
    entry.innerHTML = html;
    watchLog.prepend(entry);
    while (watchLog.children.length > 200) watchLog.removeChild(watchLog.lastChild);
  }

  async function startWatch() {
    if (!session.baseUrl || session.watchSource) return;
    const controller = new AbortController();
    session.watchSource = controller;
    watchLog.innerHTML = "";

    try {
      const res = await apiFetch(`/watch?db=${encodeURIComponent(session.db)}`, { signal: controller.signal });
      if (!res.ok || !res.body) {
        addWatchEntry(`<span class="op-delete">no se pudo abrir el stream (HTTP ${res.status})</span>`);
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let sep;
        while ((sep = buffer.indexOf("\n\n")) !== -1) {
          const rawEvent = buffer.slice(0, sep);
          buffer = buffer.slice(sep + 2);
          if (!rawEvent.trim() || rawEvent.startsWith(":")) continue;
          const dataLine = rawEvent.split("\n").find((line) => line.startsWith("data:"));
          if (!dataLine) continue;
          let data;
          try {
            data = JSON.parse(dataLine.slice(5).trim());
          } catch {
            data = { raw: dataLine.slice(5).trim() };
          }
          const op = (data.op || data.type || "event").toString().toLowerCase();
          const opClass = op.includes("insert") ? "op-insert" : op.includes("update") ? "op-update" : op.includes("delete") ? "op-delete" : "";
          addWatchEntry(`<span class="${opClass}">${escapeHtml(op)}</span> ${escapeHtml(JSON.stringify(data))}`);
        }
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        addWatchEntry(`<span class="op-delete">conexión perdida: ${escapeHtml(err.message)}</span>`);
      }
    } finally {
      if (session.watchSource === controller) session.watchSource = null;
    }
  }

  function stopWatch() {
    if (session.watchSource) {
      session.watchSource.abort();
      session.watchSource = null;
    }
    watchLog.innerHTML = `<p class="watch-empty">Activa el interruptor para transmitir inserts/updates/deletes reales.</p>`;
  }

  watchToggle.addEventListener("change", () => {
    if (watchToggle.checked) startWatch();
    else stopWatch();
  });
})();
