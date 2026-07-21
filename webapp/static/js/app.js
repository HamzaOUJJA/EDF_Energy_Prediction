(function () {
  "use strict";

  const RUN_POLL_MS = 2000;
  const TOPOLOGY_IDLE_POLL_MS = 8000;
  const LOG_POLL_MS = 3000;
  const INFRA_POLL_MS = 10000;
  const STORAGE_POLL_MS = 30000;
  const EVENTS_POLL_MS = 10000;
  const ACTIVE_RUN_STATES = new Set(["queued", "running", null]);

  let currentRunId = null;
  let runPollTimer = null;
  let topologyPollTimer = null;
  let logPollTimer = null;
  let selectedStage = null;
  let latestStages = [];

  // ------------------------------------------------------------- utilities

  function escapeHtml(value) {
    const div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
  }

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  }

  async function fetchJson(url, options) {
    const resp = await fetch(url, options);
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Request failed (${resp.status})`);
    return data;
  }

  // ----------------------------------------------------------------- tabs

  function initTabs() {
    const tabs = document.querySelectorAll(".tab");
    tabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        tabs.forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");
        document.querySelectorAll(".tab-panel").forEach((panel) => {
          panel.hidden = panel.id !== `tab-${tab.dataset.tab}`;
        });
      });
    });
  }

  // -------------------------------------------------------------- trigger

  function yearCheckboxes() {
    return Array.from(document.querySelectorAll('input[name="years"]'));
  }

  function initTriggerBar() {
    const selectAllBtn = document.getElementById("select-all-btn");
    const selectNoneBtn = document.getElementById("select-none-btn");
    const triggerBtn = document.getElementById("trigger-btn");
    const triggerError = document.getElementById("trigger-error");

    if (selectAllBtn) {
      selectAllBtn.addEventListener("click", () => yearCheckboxes().forEach((cb) => (cb.checked = true)));
    }
    if (selectNoneBtn) {
      selectNoneBtn.addEventListener("click", () => yearCheckboxes().forEach((cb) => (cb.checked = false)));
    }

    triggerBtn.addEventListener("click", async () => {
      triggerError.hidden = true;
      triggerError.classList.remove("info");
      const years = yearCheckboxes().filter((cb) => cb.checked).map((cb) => parseInt(cb.value, 10));

      triggerBtn.disabled = true;
      try {
        const data = await fetchJson("/api/trigger", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ years }),
        });
        currentRunId = data.dag_run_id;
        document.getElementById("run-id").textContent = currentRunId;
        startRunPolling();
        startTopologyPolling();
      } catch (err) {
        triggerError.textContent = err.message;
        triggerError.hidden = false;
      } finally {
        triggerBtn.disabled = false;
      }
    });

    document.getElementById("stop-btn").addEventListener("click", stopAllRuns);
    document.getElementById("refresh-btn").addEventListener("click", refreshAll);
    document.getElementById("clear-btn").addEventListener("click", clearEverything);
  }

  function setBadge(elNode, state) {
    const label = state || "pending";
    elNode.textContent = label;
    elNode.className = "badge " + label;
  }

  async function stopAllRuns() {
    if (!window.confirm("Stop every running/queued Airflow run for this DAG?")) return;
    const stopBtn = document.getElementById("stop-btn");
    stopBtn.disabled = true;
    const triggerError = document.getElementById("trigger-error");
    triggerError.hidden = true;
    try {
      const data = await fetchJson("/api/stop-all", { method: "POST" });
      triggerError.textContent = data.count
        ? `Stopped ${data.count} run(s): ${data.stopped.join(", ")}`
        : "No active runs to stop.";
      triggerError.classList.add("info");
      triggerError.hidden = false;
      await pollRun();
      await pollTopology();
    } catch (err) {
      triggerError.classList.remove("info");
      triggerError.textContent = err.message;
      triggerError.hidden = false;
    } finally {
      stopBtn.disabled = false;
    }
  }

  async function refreshAll() {
    pollTopology();
    pollInfra();
    pollStorage();
    pollEvents();
    loadConfig();
    if (currentRunId) pollRun();
  }

  function resetDashboardState() {
    stopRunPolling();
    stopLogPolling();
    currentRunId = null;
    selectedStage = null;
    latestStages = [];
    document.getElementById("run-id").textContent = "no run selected";
    setBadge(document.getElementById("run-state"), null);
    const detailPanel = document.getElementById("detail-panel");
    detailPanel.innerHTML = "";
    detailPanel.appendChild(el("p", "empty-hint", "Click a stage to see its tasks and logs."));
    document.querySelector('.tab[data-tab="overview"]').click();
  }

  async function clearEverything() {
    if (!window.confirm("Clear all Airflow run history? This resets the dashboard and deletes past DAG runs from Airflow. Pipeline data (MinIO/Postgres) is not touched.")) {
      return;
    }
    const clearBtn = document.getElementById("clear-btn");
    clearBtn.disabled = true;
    try {
      await fetchJson("/api/history/clear", { method: "POST" });
    } catch (err) {
      console.error(err);
    } finally {
      clearBtn.disabled = false;
    }
    resetDashboardState();
    refreshAll();
  }

  // --------------------------------------------------------------- run poll

  async function pollRun() {
    if (!currentRunId) return;
    try {
      const data = await fetchJson(`/api/runs/${encodeURIComponent(currentRunId)}`);
      setBadge(document.getElementById("run-state"), data.state);
      if (!ACTIVE_RUN_STATES.has(data.state)) {
        stopRunPolling();
      }
    } catch (err) {
      console.error(err);
    }
  }

  function startRunPolling() {
    stopRunPolling();
    pollRun();
    runPollTimer = setInterval(pollRun, RUN_POLL_MS);
  }

  function stopRunPolling() {
    if (runPollTimer) {
      clearInterval(runPollTimer);
      runPollTimer = null;
    }
  }

  // ---------------------------------------------------------------- topology

  function renderKpiStrip(stages) {
    const strip = document.getElementById("kpi-strip");
    strip.innerHTML = "";

    const running = stages.filter((s) => s.state === "running").length;
    const failed = stages.filter((s) => s.state === "failed").length;
    const done = stages.filter((s) => s.state === "success").length;

    const tiles = [
      ["Active stages", running],
      ["Completed stages", `${done} / ${stages.length}`],
      ["Failed stages", failed],
    ];
    for (const stage of stages) {
      if (stage.metric) {
        tiles.push([stage.metric.label, stage.metric.value]);
      }
    }

    for (const [label, value] of tiles) {
      const tile = el("div", "kpi-tile");
      tile.appendChild(el("div", "kpi-label", label));
      tile.appendChild(el("div", "kpi-value", value));
      strip.appendChild(tile);
    }
  }

  function renderTopology(stages) {
    latestStages = stages;
    const container = document.getElementById("topology");
    container.innerHTML = "";

    for (const stage of stages) {
      const box = el("div", `stage-box state-${stage.state}`);
      box.dataset.stageId = stage.id;
      if (selectedStage === stage.id) box.classList.add("selected");

      box.appendChild(el("div", "stage-label", stage.label));
      box.appendChild(el("div", "stage-tooltip", stage.tooltip));
      if (stage.metric) {
        const metricEl = el("div", "stage-metric");
        metricEl.appendChild(el("span", "metric-label", stage.metric.label));
        metricEl.appendChild(document.createTextNode(String(stage.metric.value)));
        box.appendChild(metricEl);
      }
      const badge = el("span", "badge " + stage.state, stage.state);
      const stateWrap = el("div", "stage-state");
      stateWrap.appendChild(badge);
      box.appendChild(stateWrap);

      box.addEventListener("click", () => selectStage(stage.id));
      container.appendChild(box);
    }

    renderKpiStrip(stages);

    if (selectedStage) {
      const stage = stages.find((s) => s.id === selectedStage);
      if (stage) renderDetailPanel(stage);
    }
  }

  function selectStage(stageId) {
    selectedStage = stageId;
    document.querySelectorAll(".stage-box").forEach((box) => {
      box.classList.toggle("selected", box.dataset.stageId === stageId);
    });
    const stage = latestStages.find((s) => s.id === stageId);
    if (stage) renderDetailPanel(stage);
  }

  function renderDetailPanel(stage) {
    const panel = document.getElementById("detail-panel");
    panel.innerHTML = "";
    panel.appendChild(el("h3", null, stage.label));
    panel.appendChild(el("div", "stage-tooltip", stage.tooltip));

    for (const task of stage.tasks) {
      const row = el("div", "task-row");
      row.appendChild(el("span", null, task.task_id));
      const right = el("span", null);
      const badge = el("span", "badge " + (task.state || "pending"), task.state || "pending");
      right.appendChild(badge);
      const link = el("a", null, "logs");
      link.style.marginLeft = "8px";
      link.addEventListener("click", () => showLogs(task.task_id, task.try_number));
      right.appendChild(link);
      row.appendChild(right);
      panel.appendChild(row);
    }

    const logsBox = el("pre");
    logsBox.id = "logs-content";
    logsBox.textContent = "Select a task to view its logs.";
    panel.appendChild(logsBox);
  }

  async function showLogs(taskId, tryNumber) {
    stopLogPolling();
    const logsBox = document.getElementById("logs-content");
    if (!logsBox || !currentRunId) {
      if (logsBox) logsBox.textContent = "No run selected yet.";
      return;
    }
    logsBox.textContent = "Loading...";

    const refresh = async () => {
      try {
        const stage = latestStages.find((s) => s.id === selectedStage);
        const task = stage ? stage.tasks.find((t) => t.task_id === taskId) : null;
        const attempt = task ? task.try_number : tryNumber;
        const data = await fetchJson(
          `/api/runs/${encodeURIComponent(currentRunId)}/tasks/${encodeURIComponent(taskId)}/logs?try_number=${attempt}`
        );
        logsBox.textContent = data.log || "(empty log)";
        if (!task || task.state !== "running") stopLogPolling();
      } catch (err) {
        logsBox.textContent = "Error: " + err.message;
        stopLogPolling();
      }
    };

    await refresh();
    logPollTimer = setInterval(refresh, LOG_POLL_MS);
  }

  function stopLogPolling() {
    if (logPollTimer) {
      clearInterval(logPollTimer);
      logPollTimer = null;
    }
  }

  async function pollTopology() {
    let active = false;
    try {
      const url = currentRunId
        ? `/api/topology?run_id=${encodeURIComponent(currentRunId)}`
        : "/api/topology";
      const data = await fetchJson(url);
      if (!currentRunId && data.dag_run_id) {
        currentRunId = data.dag_run_id;
        document.getElementById("run-id").textContent = currentRunId;
        pollRun();
      }
      renderTopology(data.stages);
      active = data.stages.some((s) => s.state === "running" || s.state === "queued");
    } catch (err) {
      console.error(err);
    }

    topologyPollTimer = setTimeout(pollTopology, active ? RUN_POLL_MS : TOPOLOGY_IDLE_POLL_MS);
  }

  function startTopologyPolling() {
    stopTopologyPolling();
    pollTopology();
  }

  function stopTopologyPolling() {
    if (topologyPollTimer) {
      clearTimeout(topologyPollTimer);
      topologyPollTimer = null;
    }
  }

  // ------------------------------------------------------------------ infra

  function renderInfra(data) {
    const grid = document.getElementById("infra-cards");
    grid.innerHTML = "";
    for (const [name, info] of Object.entries(data)) {
      const card = el("div", "info-card");
      const title = el("div", "card-title");
      title.appendChild(document.createTextNode(name));
      const dot = el("span", "status-dot " + (info.status === "up" ? "up" : "down"));
      title.appendChild(dot);
      card.appendChild(title);
      card.appendChild(el("div", "card-sub", `${info.status} · ${info.latency_ms}ms`));
      if (info.workers_alive != null) {
        card.appendChild(el("div", "card-sub", `${info.workers_alive}/${info.workers_total} workers`));
      }
      grid.appendChild(card);
    }
  }

  async function pollInfra() {
    try {
      renderInfra(await fetchJson("/api/infra"));
    } catch (err) {
      console.error(err);
    }
  }

  // ---------------------------------------------------------------- storage

  function formatBytes(bytes) {
    if (bytes == null) return "—";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit += 1;
    }
    return `${value.toFixed(1)} ${units[unit]}`;
  }

  function renderStorage(data) {
    const grid = document.getElementById("storage-cards");
    grid.innerHTML = "";
    for (const bucket of data.buckets) {
      const card = el("div", "info-card");
      card.appendChild(el("div", "card-title", bucket.bucket));
      card.appendChild(el("div", "card-sub", formatBytes(bucket.size_bytes)));
      card.appendChild(el("div", "card-sub", bucket.object_count != null ? `${bucket.object_count} objects` : "unavailable"));
      grid.appendChild(card);
    }
    if (data.postgres) {
      const card = el("div", "info-card");
      card.appendChild(el("div", "card-title", "Postgres DW"));
      card.appendChild(el("div", "card-sub", formatBytes(data.postgres.size_bytes)));
      grid.appendChild(card);
    }
  }

  async function pollStorage() {
    try {
      renderStorage(await fetchJson("/api/storage"));
    } catch (err) {
      console.error(err);
    }
  }

  // ------------------------------------------------------------------ events

  function renderEventLog(containerId, events) {
    const container = document.getElementById(containerId);
    container.innerHTML = "";
    if (!events.length) {
      container.appendChild(el("p", "empty-hint", "No events recorded yet."));
      return;
    }
    for (const evt of events) {
      const line = el("div", "event-line");
      line.appendChild(el("span", "ts", new Date(evt.ts).toLocaleString()));
      line.appendChild(el("span", "lvl " + evt.level, evt.level));
      line.appendChild(el("span", "src", evt.source));
      line.appendChild(el("span", null, evt.message));
      container.appendChild(line);
    }
  }

  async function pollEvents() {
    try {
      const overview = await fetchJson("/api/events?limit=50");
      renderEventLog("event-log", overview.events);
      const alerts = await fetchJson("/api/events?limit=50&severity=warn,error");
      renderEventLog("alerts-log", alerts.events);
    } catch (err) {
      console.error(err);
    }
  }

  // ------------------------------------------------------------------ config

  async function loadConfig() {
    try {
      const cfg = await fetchJson("/api/config");

      const list = document.getElementById("config-list");
      list.innerHTML = "";
      const entries = [
        ["DAG", cfg.dag_id],
        ["Airflow API", cfg.airflow_base_url],
        ["Postgres", `${cfg.postgres.host}:${cfg.postgres.port}/${cfg.postgres.database}`],
        ["MinIO endpoint", cfg.minio_endpoint],
        ["Data dir", cfg.data_dir],
      ];
      for (const [label, value] of entries) {
        list.appendChild(el("dt", null, label));
        list.appendChild(el("dd", null, value));
      }

      const links = document.getElementById("config-links");
      links.innerHTML = "";
      for (const [name, url] of Object.entries(cfg.links)) {
        const li = document.createElement("li");
        const a = document.createElement("a");
        a.href = url;
        a.target = "_blank";
        a.rel = "noopener";
        a.textContent = `${name} — ${url}`;
        li.appendChild(a);
        links.appendChild(li);
      }

      const grafanaBase = cfg.links.grafana;
      document.getElementById("grafana-frame-1").src = `${grafanaBase}/d/edf-platform-overview?orgId=1&kiosk&theme=light`;
      document.getElementById("grafana-frame-2").src = `${grafanaBase}/d/edf-data-analytics?orgId=1&kiosk&theme=light`;
    } catch (err) {
      console.error(err);
    }
  }

  // -------------------------------------------------------------------- init

  document.addEventListener("DOMContentLoaded", () => {
    initTabs();
    initTriggerBar();
    loadConfig();
    startTopologyPolling();
    pollInfra();
    setInterval(pollInfra, INFRA_POLL_MS);
    pollStorage();
    setInterval(pollStorage, STORAGE_POLL_MS);
    pollEvents();
    setInterval(pollEvents, EVENTS_POLL_MS);
  });
})();
