// EcoSync Sentinel Frontend Logic
// Vanilla JS, shared across all pages

(function () {
  const body = document.body;
  const page = body.getAttribute("data-page");

  // ---------- Global Top Bar ----------
  const systemTimeEl = document.getElementById("system-time-value");
  if (systemTimeEl) {
    const updateTime = () => {
      const now = new Date();
      const pad = (n) => String(n).padStart(2, "0");
      systemTimeEl.textContent = `${pad(now.getHours())}:${pad(
        now.getMinutes()
      )}:${pad(now.getSeconds())}`;
    };
    updateTime();
    setInterval(updateTime, 1000);
  }

  // ---------- Emergency Banner ----------
  const emergencyBanner = document.getElementById("emergency-banner");
  const emergencyBannerText = document.getElementById("emergency-banner-text");
  const emergencyBannerAck = document.getElementById("emergency-banner-ack");

  function showEmergencyBanner(message) {
    if (!emergencyBanner || !emergencyBannerText) return;
    emergencyBannerText.textContent = `CRITICAL ALERT: ${message}`;
    emergencyBanner.classList.remove("hidden");
    // trigger slide
    requestAnimationFrame(() => {
      emergencyBanner.classList.add("visible");
    });
  }

  function hideEmergencyBanner() {
    if (!emergencyBanner) return;
    emergencyBanner.classList.remove("visible");
    // after transition, hide completely
    setTimeout(() => {
      emergencyBanner.classList.add("hidden");
    }, 400);
  }

  if (emergencyBannerAck) {
    emergencyBannerAck.addEventListener("click", hideEmergencyBanner);
  }

  // ---------- SSE Connections ----------
  const connectionDot = document.getElementById("connection-dot");
  let sensorSource = null;
  let alertSource = null;
  let activeSSECount = 0;

  function updateConnectionIndicator() {
    if (!connectionDot) return;
    if (activeSSECount > 0) {
      connectionDot.classList.remove("dot-disconnected");
      connectionDot.classList.add("dot-connected");
    } else {
      connectionDot.classList.add("dot-disconnected");
      connectionDot.classList.remove("dot-connected");
    }
  }

  const sensorListeners = [];
  const alertListeners = [];

  function addSensorListener(fn) {
    sensorListeners.push(fn);
  }

  function addAlertListener(fn) {
    alertListeners.push(fn);
  }

  function startSensorStream() {
    if (sensorSource) return;
    try {
      sensorSource = new EventSource("/api/stream/sensors");
      activeSSECount++;
      updateConnectionIndicator();
      sensorSource.onmessage = (event) => {
        sensorListeners.forEach(fn => fn(event));
      };
      sensorSource.onerror = () => {
        sensorSource.close();
        sensorSource = null;
        activeSSECount = Math.max(0, activeSSECount - 1);
        updateConnectionIndicator();
        // simple retry with delay
        setTimeout(startSensorStream, 5000);
      };
    } catch (e) {
      console.error("Error opening sensor stream", e);
    }
  }

  function startAlertStream() {
    if (alertSource) return;
    try {
      alertSource = new EventSource("/api/stream/alerts");
      activeSSECount++;
      updateConnectionIndicator();
      alertSource.onmessage = (event) => {
        alertListeners.forEach(fn => fn(event));
      };
      alertSource.onerror = () => {
        alertSource.close();
        alertSource = null;
        activeSSECount = Math.max(0, activeSSECount - 1);
        updateConnectionIndicator();
        setTimeout(startAlertStream, 5000);
      };
    } catch (e) {
      console.error("Error opening alert stream", e);
    }
  }

  // ---------- Dashboard Page ----------
  function initDashboard() {
    const healthEl = document.getElementById("kpi-health");
    const co2El = document.getElementById("kpi-co2");
    const criticalAlertsEl = document.getElementById("kpi-critical-alerts");
    const machinesOnlineEl = document.getElementById("kpi-machines-online");
    const telemetryTerminal = document.getElementById("telemetry-terminal");
    const taskListEl = document.getElementById("task-list");

    const kpiElements = {
      health: { el: healthEl, card: document.querySelector('[data-kpi="health"]') },
      co2: { el: co2El, card: document.querySelector('[data-kpi="co2"]') },
      criticalAlerts: {
        el: criticalAlertsEl,
        card: document.querySelector('[data-kpi="critical-alerts"]'),
      },
      machinesOnline: {
        el: machinesOnlineEl,
        card: document.querySelector('[data-kpi="machines-online"]'),
      },
    };

    function animateKpi(key, newText) {
      const k = kpiElements[key];
      if (!k || !k.el || !k.card) return;
      if (k.el.textContent === newText) return;
      k.el.textContent = newText;
      k.card.classList.add("kpi-animate");
      setTimeout(() => k.card.classList.remove("kpi-animate"), 500);
    }

    // Telemetry terminal
    function appendTelemetryLine(line, level) {
      if (!telemetryTerminal) return;
      const div = document.createElement("div");
      div.classList.add("terminal-line");
      if (level === "WARNING") {
        div.classList.add("terminal-line-warning");
      } else if (level === "LLM") {
        div.classList.add("terminal-line-llm");
      } else {
        div.classList.add("terminal-line-normal");
      }
      div.textContent = line;
      telemetryTerminal.appendChild(div);
      // If we have more than 50 lines, remove the oldest (top) one
      while (telemetryTerminal.children.length > 50) {
        telemetryTerminal.removeChild(telemetryTerminal.firstChild);
      }
      telemetryTerminal.scrollTop = telemetryTerminal.scrollHeight;
    }

    // Tasks list
    let tasks = [];

    function renderTasks() {
      if (!taskListEl) return;
      taskListEl.innerHTML = "";

      let criticalCount = tasks.filter((t) => t.severity === "CRITICAL").length;
      animateKpi("criticalAlerts", String(criticalCount));
      tasks.forEach((task) => {
        const item = document.createElement("div");
        item.className = "task-item";

        const header = document.createElement("div");
        header.className = "task-header";

        const machineSpan = document.createElement("span");
        machineSpan.className = "task-machine";
        machineSpan.textContent = task.machine;

        const severity = document.createElement("span");
        severity.className = "severity-pill";
        if (task.severity === "CRITICAL") {
          severity.classList.add("severity-critical");
        } else if (task.severity === "WARNING") {
          severity.classList.add("severity-warning");
        } else {
          severity.classList.add("severity-normal");
        }
        severity.textContent = task.severity || "INFO";

        header.appendChild(machineSpan);
        header.appendChild(severity);

        const msg = document.createElement("div");
        msg.className = "task-message";
        msg.textContent = task.message;

        const actions = document.createElement("div");
        actions.className = "task-actions";
        const btn = document.createElement("button");
        btn.className = "btn";
        btn.textContent = task.severity === "CRITICAL" ? "Acknowledge" : "Clear";
        btn.addEventListener("click", () => {
          tasks = tasks.filter((t) => t !== task);
          renderTasks();
        });
        actions.appendChild(btn);

        item.appendChild(header);
        item.appendChild(msg);
        item.appendChild(actions);
        taskListEl.appendChild(item);
      });
    }

    // Node navigation
    document.querySelectorAll(".workflow-diagram .node").forEach((btn) => {
      btn.addEventListener("click", () => {
        const machineId = btn.getAttribute("data-machine-id");
        const param = encodeURIComponent(machineId || "");
        window.location.hash = `#diagnostics?machine=${param}`;
      });
    });

    // SSE handlers
    addSensorListener((event) => {
      let payload;
      try {
        payload = JSON.parse(event.data);
      } catch {
        return;
      }

      const timeStr = new Date().toLocaleTimeString("en-GB", {
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });

      const machine = payload.machineId || payload.machine || "UNKNOWN";
      const temp = payload.temperature ?? payload.temp;
      const vibration = payload.vibration;
      const sound = payload.sound ?? payload.soundDb;
      const status = payload.status || "NORMAL";
      const category = payload.category || status;

      const line = `[${timeStr}] ${machine} temp=${temp} vibration=${vibration} sound=${sound}`;

      let level = "NORMAL";
      if (String(category).toUpperCase().includes("WARNING")) {
        level = "WARNING";
      } else if (String(category).toUpperCase().includes("LLM")) {
        level = "LLM";
      }
      appendTelemetryLine(line, level);

      // Simple derived KPIs if available
      if (typeof payload.systemHealth === "number") {
        animateKpi("health", `${payload.systemHealth.toFixed(0)}%`);
      }
      if (typeof payload.co2Footprint === "number") {
        animateKpi("co2", `${payload.co2Footprint.toFixed(1)} kg`);
      }
      if (typeof payload.machinesOnline === "number") {
        animateKpi("machinesOnline", String(payload.machinesOnline));
      }
    });

    addAlertListener((event) => {
      let payload;
      try {
        payload = JSON.parse(event.data);
      } catch {
        return;
      }
      const severity = (payload.severity || "").toUpperCase();
      const machine = payload.machine || "UNKNOWN";
      const message = payload.message || "Alert";

      const existingIdx = tasks.findIndex(
        (t) => t.machine === machine && t.message === message
      );
      if (existingIdx !== -1) {
        tasks.splice(existingIdx, 1);
      }

      tasks.unshift({
        machine,
        message,
        severity,
      });
      // keep limited history
      if (tasks.length > 30) tasks = tasks.slice(0, 30);
      renderTasks();

      if (severity === "CRITICAL") {
        showEmergencyBanner(message);
      }
    });
  }

  // ---------- Diagnostics Page ----------
  function initDiagnostics() {
    const hashSplit = window.location.hash.split('?');
    const urlParams = new URLSearchParams(hashSplit[1] || "");
    
    // Support hash change query parameter updates
    window.addEventListener("hashchange", () => {
      if (window.location.hash.startsWith("#diagnostics?machine=")) {
        const newHashParams = new URLSearchParams(window.location.hash.split('?')[1] || "");
        const newMachineId = newHashParams.get("machine");
        if (newMachineId && newMachineId !== currentMachineId) {
          currentMachineId = newMachineId;
          renderMachineList();
          fetchDigitalTwin();
        }
      }
    });
    const initialMachineParam = urlParams.get("machine");
    const machineListEl = document.getElementById("machine-list");
    const dropzone = document.getElementById("pdf-dropzone");
    const pdfInput = document.getElementById("pdf-input");
    const overlay = document.getElementById("manual-overlay");

    const digitalTwinEl = document.getElementById("digital-twin");
    const editBtn = document.getElementById("edit-limits-btn");
    const saveBtn = document.getElementById("save-limits-btn");
    const cancelBtn = document.getElementById("cancel-limits-btn");

    const tempCanvas = document.getElementById("chart-temperature");
    const vibCanvas = document.getElementById("chart-vibration");
    const soundCanvas = document.getElementById("chart-sound");

    let currentMachineId = initialMachineParam || null;
    let machines = [];
    let digitalTwinData = null;

    // Chart.js setup
    const chartConfig = (label, color) => ({
      type: "line",
      data: {
        labels: [],
        datasets: [
          {
            label,
            data: [],
            borderColor: color,
            backgroundColor: "transparent",
            pointRadius: 0,
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        animation: false,
        scales: {
          x: {
            ticks: {
              color: "#6b7280",
              maxTicksLimit: 6,
            },
            grid: {
              color: "rgba(31, 41, 55, 0.8)",
            },
          },
          y: {
            ticks: {
              color: "#6b7280",
            },
            grid: {
              color: "rgba(31, 41, 55, 0.8)",
            },
          },
        },
        plugins: {
          legend: {
            labels: {
              color: "#e5e7eb",
            },
          },
        },
      },
    });

    // Custom plugin for red threshold line
    const thresholdPlugin = {
      id: "thresholdLine",
      afterDraw(chart) {
        const threshold = chart.options.thresholdValue;
        if (typeof threshold !== "number") return;
        const {
          ctx,
          chartArea: { left, right, top, bottom },
          scales: { y },
        } = chart;
        const yPos = y.getPixelForValue(threshold);
        if (yPos < top || yPos > bottom) return;
        ctx.save();
        ctx.strokeStyle = "#ef4444";
        ctx.lineWidth = 2;
        ctx.setLineDash([6, 4]);
        ctx.beginPath();
        ctx.moveTo(left, yPos);
        ctx.lineTo(right, yPos);
        ctx.stroke();
        ctx.restore();
      },
    };

    if (window.Chart && tempCanvas && vibCanvas && soundCanvas) {
      Chart.register(thresholdPlugin);
    }

    const charts = {
      temperature: tempCanvas
        ? new Chart(tempCanvas.getContext("2d"), chartConfig("Temperature", "#22c55e"))
        : null,
      vibration: vibCanvas
        ? new Chart(vibCanvas.getContext("2d"), chartConfig("Vibration", "#38bdf8"))
        : null,
      sound: soundCanvas
        ? new Chart(soundCanvas.getContext("2d"), chartConfig("Sound", "#a855f7"))
        : null,
    };

    charts.temperature && (charts.temperature.options.thresholdValue = null);
    charts.vibration && (charts.vibration.options.thresholdValue = null);
    charts.sound && (charts.sound.options.thresholdValue = null);

    const MAX_POINTS = 40;
    function pushData(chart, value) {
      if (!chart) return;
      const timeStr = new Date().toLocaleTimeString("en-GB", {
        hour12: false,
        minute: "2-digit",
        second: "2-digit",
      });
      chart.data.labels.push(timeStr);
      chart.data.datasets[0].data.push(value);
      if (chart.data.labels.length > MAX_POINTS) {
        chart.data.labels.shift();
        chart.data.datasets[0].data.shift();
      }
      chart.update();
    }

    // Machines list
    function renderMachineList() {
      if (!machineListEl) return;
      machineListEl.innerHTML = "";
      machines.forEach((m) => {
        const li = document.createElement("li");
        li.className = "machine-list-item";
        if (m.id === currentMachineId) {
          li.classList.add("active");
        }
        li.dataset.id = m.id;
        li.textContent = m.name || m.id;
        const status = document.createElement("span");
        status.textContent = m.status || "";
        status.style.fontSize = "11px";
        status.style.color = "#6b7280";
        li.appendChild(status);
        li.addEventListener("click", () => {
          currentMachineId = m.id;
          renderMachineList();
          fetchDigitalTwin();
        });
        machineListEl.appendChild(li);
      });
    }

    async function fetchMachines() {
      try {
        const res = await fetch("/api/machines");
        if (!res.ok) throw new Error("Failed to fetch machines");
        const data = await res.json();
        machines = Array.isArray(data) ? data : data.machines || [];
        if (!currentMachineId && machines.length > 0) {
          currentMachineId = machines[0].id || machines[0].machineId;
        }
        renderMachineList();
        fetchDigitalTwin();
      } catch (e) {
        console.error(e);
      }
    }

    async function fetchDigitalTwin() {
      if (!currentMachineId || !digitalTwinEl) return;
      try {
        const res = await fetch(`/api/machines/${encodeURIComponent(currentMachineId)}`);
        if (!res.ok) throw new Error("Failed to fetch machine details");
        const data = await res.json();
        digitalTwinData = data;
        const name = data.name || currentMachineId;
        const type = data.type || data.machineType || "--";
        const desc = data.description || "--";
        const limits = data.operatingLimits || data.limits || {};
        const limitsStr =
          typeof limits === "string" ? limits : JSON.stringify(limits, null, 2);

        digitalTwinEl.querySelector('[data-field="name"]').textContent = name;
        digitalTwinEl.querySelector('[data-field="type"]').textContent = type;
        digitalTwinEl.querySelector('[data-field="description"]').textContent = desc;
        digitalTwinEl.querySelector('[data-field="operatingLimits"]').textContent =
          limitsStr;
      } catch (e) {
        console.error(e);
      }
    }

    // Edit limits
    let editing = false;
    function enterEditMode() {
      if (!digitalTwinEl || editing) return;
      editing = true;
      const limitsSpan = digitalTwinEl.querySelector(
        '[data-field="operatingLimits"]'
      );
      if (!limitsSpan) return;
      const current = limitsSpan.textContent;
      const textarea = document.createElement("textarea");
      textarea.value = current;
      textarea.style.width = "100%";
      textarea.style.minHeight = "120px";
      textarea.style.background = "#020617";
      textarea.style.color = "#e5e7eb";
      textarea.style.borderRadius = "8px";
      textarea.style.border = "1px solid rgba(55,65,81,0.9)";
      limitsSpan.replaceWith(textarea);

      if (editBtn && saveBtn && cancelBtn) {
        editBtn.classList.add("hidden");
        saveBtn.classList.remove("hidden");
        cancelBtn.classList.remove("hidden");
      }
    }

    function exitEditMode(cancel = false) {
      if (!digitalTwinEl || !editing) return;
      const textarea = digitalTwinEl.querySelector("textarea");
      if (!textarea) return;
      const span = document.createElement("span");
      span.className = "dt-value";
      span.dataset.field = "operatingLimits";
      if (cancel && digitalTwinData) {
        const limits = digitalTwinData.operatingLimits || digitalTwinData.limits || {};
        span.textContent =
          typeof limits === "string" ? limits : JSON.stringify(limits, null, 2);
      } else {
        span.textContent = textarea.value;
      }
      textarea.replaceWith(span);

      if (editBtn && saveBtn && cancelBtn) {
        editBtn.classList.remove("hidden");
        saveBtn.classList.add("hidden");
        cancelBtn.classList.add("hidden");
      }
      editing = false;
    }

    async function saveLimits() {
      if (!digitalTwinEl || !currentMachineId) return;
      const textarea = digitalTwinEl.querySelector("textarea");
      if (!textarea) return;
      let payload;
      try {
        payload = JSON.parse(textarea.value);
      } catch {
        alert("Operating limits must be valid JSON.");
        return;
      }
      try {
        const res = await fetch(
          `/api/machines/${encodeURIComponent(currentMachineId)}/limits`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          }
        );
        if (!res.ok) throw new Error("Failed to save limits");
        digitalTwinData = digitalTwinData || {};
        digitalTwinData.operatingLimits = payload;
        exitEditMode(false);
      } catch (e) {
        console.error(e);
        alert("Failed to save limits.");
      }
    }

    if (editBtn) editBtn.addEventListener("click", enterEditMode);
    if (saveBtn) saveBtn.addEventListener("click", saveLimits);
    if (cancelBtn) cancelBtn.addEventListener("click", () => exitEditMode(true));

    // PDF upload
    function showOverlay() {
      overlay && overlay.classList.remove("hidden");
    }
    function hideOverlay() {
      overlay && overlay.classList.add("hidden");
    }

async function uploadFile(file) {
      if (!file) return;
      const form = new FormData();
      form.append("file", file);
      showOverlay();
      
      try {
        const res = await fetch("/api/manuals/upload", {
          method: "POST",
          body: form,
        });
        
        if (!res.ok) throw new Error("Upload failed");
        
        // 1. Capture the JSON response from FastAPI
        const jsonResponse = await res.json();
        
        // 2. Unhide the summary container and inject the formatted JSON
        const summaryContainer = document.getElementById("llm-summary-container");
        const summaryContent = document.getElementById("llm-summary-content");
        
        if (summaryContainer && summaryContent) {
          // Format the JSON nicely with 2 spaces of indentation
          summaryContent.textContent = JSON.stringify(jsonResponse.data, null, 2);
          summaryContainer.classList.remove("hidden");
        }

        // 3. Force the machines list and Digital Twin view to refresh
        // This ensures the new limits pop up instantly on the right side!
        await fetchMachines();

      } catch (e) {
        console.error(e);
        alert("Failed to upload manual.");
      } finally {
        hideOverlay();
      }
    }

    if (dropzone) {
      dropzone.addEventListener("click", () => pdfInput && pdfInput.click());
      dropzone.addEventListener("dragover", (e) => {
        e.preventDefault();
        dropzone.classList.add("dragover");
      });
      dropzone.addEventListener("dragleave", (e) => {
        e.preventDefault();
        dropzone.classList.remove("dragover");
      });
      dropzone.addEventListener("drop", (e) => {
        e.preventDefault();
        dropzone.classList.remove("dragover");
        const file = e.dataTransfer.files[0];
        if (file && file.type === "application/pdf") {
          uploadFile(file);
        }
      });
    }

    if (pdfInput) {
      pdfInput.addEventListener("change", () => {
        const file = pdfInput.files[0];
        if (file && file.type === "application/pdf") {
          uploadFile(file);
        }
      });
    }

    // SSE sensors -> charts + thresholds
    addSensorListener((event) => {
      let payload;
      try {
        payload = JSON.parse(event.data);
      } catch {
        return;
      }
      const targetMachine = payload.machineId || payload.machine || null;
      
      // If we don't have a specific machine selected yet, lock onto the first one that streams in
      if (!currentMachineId && targetMachine) {
          currentMachineId = targetMachine;
          renderMachineList();
          fetchDigitalTwin();
      }

      if (currentMachineId && targetMachine && targetMachine !== currentMachineId) {
        return;
      }

      if (typeof payload.temperature === "number") {
        pushData(charts.temperature, payload.temperature);
      }
      if (typeof payload.vibration === "number") {
        pushData(charts.vibration, payload.vibration);
      }
      if (typeof payload.sound === "number" || typeof payload.soundDb === "number") {
        const soundVal = payload.sound ?? payload.soundDb;
        pushData(charts.sound, soundVal);
      }

      // thresholds from LLM extraction
      if (typeof payload.temperatureMax === "number" && charts.temperature) {
        charts.temperature.options.thresholdValue = payload.temperatureMax;
        charts.temperature.update("none");
      }
      if (typeof payload.vibrationMax === "number" && charts.vibration) {
        charts.vibration.options.thresholdValue = payload.vibrationMax;
        charts.vibration.update("none");
      }
      if (typeof payload.soundMax === "number" && charts.sound) {
        charts.sound.options.thresholdValue = payload.soundMax;
        charts.sound.update("none");
      }
    });

    addAlertListener((event) => {
      let payload;
      try {
        payload = JSON.parse(event.data);
      } catch {
        return;
      }
      const severity = (payload.severity || "").toUpperCase();
      if (severity === "CRITICAL") {
        showEmergencyBanner(payload.message || "Alert");
      }
    });

    fetchMachines();
  }

  // ---------- Maintenance Page ----------
  function initMaintenance() {
    const calendarGrid = document.getElementById("calendar-grid");
    const calendarLabel = document.getElementById("calendar-month-label");
    const taskListEl = document.getElementById("maintenance-task-list");
    const sortSelect = document.getElementById("task-sort");
    const modal = document.getElementById("day-modal");
    const modalBody = document.getElementById("day-modal-body");
    const modalClose = document.getElementById("day-modal-close");
    const inventoryTable = document.getElementById("inventory-table");
    const generateBomBtn = document.getElementById("generate-bom-btn");

    let tasks = [];
    let inventory = [];

    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth(); // 0-index

    function formatDateKey(y, m, d) {
      const mm = String(m + 1).padStart(2, "0");
      const dd = String(d).padStart(2, "0");
      return `${y}-${mm}-${dd}`;
    }

    function renderCalendar() {
      if (!calendarGrid || !calendarLabel) return;
      calendarGrid.innerHTML = "";
      const firstDay = new Date(currentYear, currentMonth, 1);
      const lastDay = new Date(currentYear, currentMonth + 1, 0);
      const startWeekday = firstDay.getDay(); // 0 = Sunday
      const daysInMonth = lastDay.getDate();

      const monthName = firstDay.toLocaleString("default", { month: "long" });
      calendarLabel.textContent = `${monthName} ${currentYear}`;

      const totalCells = Math.ceil((startWeekday + daysInMonth) / 7) * 7;

      for (let i = 0; i < totalCells; i++) {
        const cell = document.createElement("div");
        cell.className = "calendar-cell";

        const dateNum = i - startWeekday + 1;
        if (dateNum <= 0 || dateNum > daysInMonth) {
          cell.style.visibility = "hidden";
          calendarGrid.appendChild(cell);
          continue;
        }
        const dateSpan = document.createElement("div");
        dateSpan.className = "calendar-date";
        dateSpan.textContent = String(dateNum);
        cell.appendChild(dateSpan);

        const cellKey = formatDateKey(currentYear, currentMonth, dateNum);
        const dayTasks = tasks.filter((t) => t.date === cellKey);

        // Highlight today
        if (cellKey === formatDateKey(today.getFullYear(), today.getMonth(), today.getDate())) {
            cell.classList.add("today");
            dateSpan.style.backgroundColor = "rgba(59, 130, 246, 0.5)"; // Blue highlight
            dateSpan.style.borderRadius = "50%";
            dateSpan.style.width = "24px";
            dateSpan.style.height = "24px";
            dateSpan.style.display = "flex";
            dateSpan.style.alignItems = "center";
            dateSpan.style.justifyContent = "center";
        }

        const tasksContainer = document.createElement("div");
        tasksContainer.className = "cell-tasks";
        tasksContainer.style.marginTop = "4px";
        tasksContainer.style.display = "flex";
        tasksContainer.style.flexDirection = "column";
        tasksContainer.style.gap = "2px";
        tasksContainer.style.overflow = "hidden";

        dayTasks.forEach((t) => {
          const taskLabel = document.createElement("div");
          taskLabel.style.fontSize = "9px";
          taskLabel.style.padding = "2px 4px";
          taskLabel.style.borderRadius = "3px";
          taskLabel.style.whiteSpace = "nowrap";
          taskLabel.style.textOverflow = "ellipsis";
          taskLabel.style.overflow = "hidden";
          
          if (t.severity === "CRITICAL") {
            taskLabel.style.backgroundColor = "rgba(239, 68, 68, 0.2)";
            taskLabel.style.color = "#fca5a5";
            taskLabel.style.border = "1px solid rgba(239, 68, 68, 0.3)";
          } else if (t.severity === "WARNING") {
            taskLabel.style.backgroundColor = "rgba(234, 179, 8, 0.2)";
            taskLabel.style.color = "#fde047";
            taskLabel.style.border = "1px solid rgba(234, 179, 8, 0.3)";
          } else {
            taskLabel.style.backgroundColor = "rgba(34, 197, 94, 0.2)";
            taskLabel.style.color = "#86efac";
            taskLabel.style.border = "1px solid rgba(34, 197, 94, 0.3)";
          }
          taskLabel.textContent = `${t.machine}: ${t.task}`;
          tasksContainer.appendChild(taskLabel);
        });
        cell.appendChild(tasksContainer);

        cell.addEventListener("click", () => {
          openDayModal(cellKey, dayTasks);
        });

        calendarGrid.appendChild(cell);
      }
    }

    function openDayModal(dateKey, dayTasks) {
      if (!modal || !modalBody) return;
      modalBody.innerHTML = "";
      const label = document.createElement("div");
      label.style.marginBottom = "8px";
      label.style.fontSize = "12px";
      label.style.color = "#9ca3af";
      label.textContent = dateKey;
      modalBody.appendChild(label);

      if (dayTasks.length === 0) {
        const empty = document.createElement("div");
        empty.textContent = "No tasks scheduled.";
        empty.style.fontSize = "13px";
        modalBody.appendChild(empty);
      } else {
        dayTasks.forEach((t) => {
          const box = document.createElement("div");
          box.className = "modal-task";

          const title = document.createElement("div");
          title.className = "modal-task-title";
          title.textContent = `${t.machine} — ${t.task}`;

          const notes = document.createElement("div");
          notes.className = "modal-task-notes";
          notes.textContent = t.notes || "";

          box.appendChild(title);
          box.appendChild(notes);
          modalBody.appendChild(box);
        });
      }
      modal.classList.remove("hidden");
    }

    if (modalClose && modal) {
      modalClose.addEventListener("click", () => {
        modal.classList.add("hidden");
      });
      modal.addEventListener("click", (e) => {
        if (e.target === modal) {
          modal.classList.add("hidden");
        }
      });
    }

    function computeTimeRemaining(task) {
      const now = new Date();
      const [y, m, d] = task.date.split("-").map((v) => parseInt(v, 10));
      const target = new Date(y, m - 1, d);
      return target.getTime() - now.getTime();
    }

    function renderTaskList() {
      if (!taskListEl) return;
      const sortMode = sortSelect ? sortSelect.value : "time";
      const sorted = [...tasks];
      if (sortMode === "machine") {
        sorted.sort((a, b) => a.machine.localeCompare(b.machine));
      } else {
        sorted.sort((a, b) => computeTimeRemaining(a) - computeTimeRemaining(b));
      }
      taskListEl.innerHTML = "";
      sorted.forEach((t) => {
        const item = document.createElement("div");
        item.className = "task-item";

        const header = document.createElement("div");
        header.className = "task-header";
        const machineSpan = document.createElement("span");
        machineSpan.className = "task-machine";
        machineSpan.textContent = t.machine;

        const pill = document.createElement("span");
        pill.className = "severity-pill";
        if (t.severity === "CRITICAL") {
          pill.classList.add("severity-critical");
        } else if (t.severity === "WARNING") {
          pill.classList.add("severity-warning");
        } else {
          pill.classList.add("severity-normal");
        }
        pill.textContent = t.severity || "INFO";

        header.appendChild(machineSpan);
        header.appendChild(pill);

        const msg = document.createElement("div");
        msg.className = "task-message";
        msg.textContent = `${t.task} — ${t.date}`;

        const notes = document.createElement("div");
        notes.className = "task-message";
        notes.textContent = t.notes || "";

        item.appendChild(header);
        item.appendChild(msg);
        if (t.notes) item.appendChild(notes);
        taskListEl.appendChild(item);
      });
    }

    if (sortSelect) {
      sortSelect.addEventListener("change", renderTaskList);
    }

    async function fetchMaintenance() {
      try {
        const res = await fetch("/api/maintenance/schedule");
        if (!res.ok) throw new Error("Failed to fetch maintenance schedule");
        const data = await res.json();
        tasks = Array.isArray(data) ? data : data.tasks || [];
      } catch (e) {
        console.error(e);
        // Fallback stub tasks
        if (tasks.length === 0) {
          const todayKey = formatDateKey(currentYear, currentMonth, today.getDate());
          tasks = [
            {
              machine: "CNC_1",
              task: "Spindle inspection",
              date: todayKey,
              notes: "Check vibration baseline and coolant levels.",
              severity: "WARNING",
            },
          ];
        }
      } finally {
        renderCalendar();
        renderTaskList();
      }
    }

    // Inventory
    function renderInventory() {
      if (!inventoryTable) return;
      const tbody = inventoryTable.querySelector("tbody");
      tbody.innerHTML = "";
      inventory.forEach((part, idx) => {
        const tr = document.createElement("tr");

        const tdSelect = document.createElement("td");
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.dataset.index = idx;
        tdSelect.appendChild(checkbox);

        const tdName = document.createElement("td");
        const nameSpan = document.createElement("span");
        nameSpan.textContent = part.name || part.partName || "Unnamed";
        tdName.appendChild(nameSpan);

        const description = (part.description || "").toLowerCase();
        if (description.includes("suggested by llm")) {
          const flag = document.createElement("span");
          flag.className = "llm-flag";
          const inner = document.createElement("span");
          inner.className = "llm-flag-icon";
          flag.appendChild(inner);
          tdName.appendChild(flag);
        }

        const tdNumber = document.createElement("td");
        tdNumber.textContent = part.partNumber || part.number || "-";

        const tdStock = document.createElement("td");
        tdStock.textContent = String(part.currentStock ?? part.stock ?? 0);

        const tdMin = document.createElement("td");
        tdMin.textContent = String(part.minimumRequired ?? part.minRequired ?? 0);

        const tdStatus = document.createElement("td");
        const pill = document.createElement("span");
        pill.className = "status-pill";
        const stock = part.currentStock ?? part.stock ?? 0;
        const min = part.minimumRequired ?? part.minRequired ?? 0;
        if (stock <= min) {
          pill.classList.add("status-red");
          pill.textContent = "LOW";
        } else {
          pill.classList.add("status-green");
          pill.textContent = "OK";
        }
        tdStatus.appendChild(pill);

        tr.appendChild(tdSelect);
        tr.appendChild(tdName);
        tr.appendChild(tdNumber);
        tr.appendChild(tdStock);
        tr.appendChild(tdMin);
        tr.appendChild(tdStatus);

        tbody.appendChild(tr);
      });
    }

    async function fetchInventory() {
      try {
        const res = await fetch("/api/inventory");
        if (!res.ok) throw new Error("Failed to fetch inventory");
        const data = await res.json();
        inventory = Array.isArray(data) ? data : data.items || [];
      } catch (e) {
        console.error(e);
        if (inventory.length === 0) {
          inventory = [
            {
              name: "Bearing Set A",
              partNumber: "BR-1001",
              currentStock: 6,
              minimumRequired: 4,
              description: "Standard bearing set suggested by llm",
            },
            {
              name: "Coolant Filter",
              partNumber: "CF-220",
              currentStock: 1,
              minimumRequired: 3,
              description: "Primary coolant filtration unit",
            },
          ];
        }
      } finally {
        renderInventory();
      }
    }

    if (generateBomBtn && inventoryTable) {
      generateBomBtn.addEventListener("click", () => {
        const tbody = inventoryTable.querySelector("tbody");
        const checkedRows = tbody.querySelectorAll("input[type='checkbox']:checked");
        const selected = [];
        checkedRows.forEach((cb) => {
          const idx = parseInt(cb.dataset.index, 10);
          if (!Number.isNaN(idx) && inventory[idx]) {
            selected.push(inventory[idx]);
          }
        });
        if (selected.length === 0) {
          alert("Select at least one part to generate a Bill of Materials.");
          return;
        }
        const blob = new Blob([JSON.stringify(selected, null, 2)], {
          type: "application/json",
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "bill_of_materials.json";
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      });
    }

    fetchMaintenance();
    fetchInventory();
  }

  // ---------- Page Router ----------
  function handleHashChange() {
    let hash = window.location.hash || "#dashboard";
    // If hash contains query params, strip them for routing
    const target = hash.substring(1).split("?")[0];

    // Hide all views
    document.querySelectorAll(".view-section").forEach(el => {
      el.style.display = "none";
    });

    // Update nav links
    document.querySelectorAll(".nav-link").forEach(el => {
      el.classList.remove("active");
      const targetHash = el.getAttribute("href");
      if (targetHash === "#" + target) {
        el.classList.add("active");
      }
    });

    // Show target view
    const view = document.getElementById(`view-${target}`);
    if (view) {
      view.style.display = ""; // default display
    } else {
      // fallback
      const fallback = document.getElementById("view-dashboard");
      if (fallback) fallback.style.display = "";
    }
  }

  if (page === "spa") {
    // Initialize all views once
    initDashboard();
    initDiagnostics();
    initMaintenance();

    // Start single SSE connections
    startSensorStream();
    startAlertStream();

    // Listen to hash changes
    window.addEventListener("hashchange", handleHashChange);
    
    // Trigger initially
    handleHashChange();
  } else {
    // Fallback for non-SPA
    addAlertListener((event) => {
      let payload;
      try {
        payload = JSON.parse(event.data);
      } catch {
        return;
      }
      const severity = (payload.severity || "").toUpperCase();
      if (severity === "CRITICAL") {
        showEmergencyBanner(payload.message || "Alert");
      }
    });
    startAlertStream();
  }
})();

