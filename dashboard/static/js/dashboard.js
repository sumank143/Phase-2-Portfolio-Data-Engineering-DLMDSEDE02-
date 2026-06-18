// dashboard.js — WebSocket client + chart rendering

const WS_URL = `ws://${location.host}/ws`;
let ws = null;

// ── WebSocket ───────────────────────────────────────────
function connectWebSocket() {
  ws = new WebSocket(WS_URL);
  const status = document.getElementById('ws-status');

  ws.onopen = () => {
    status.textContent = 'Live';
    status.className = 'status connected';
    // Send periodic keepalive
    setInterval(() => { if (ws.readyState === 1) ws.send('ping'); }, 30000);
  };

  ws.onclose = () => {
    status.textContent = 'Disconnected';
    status.className = 'status disconnected';
    setTimeout(connectWebSocket, 3000);
  };

  ws.onerror = () => ws.close();

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      updateDashboard(data);
    } catch (e) {
      console.error('Parse error:', e);
    }
  };
}

// ── Update Dashboard ────────────────────────────────────
function updateDashboard(data) {
  // KPI cards
  const s = data.stats || {};
  setText('total-accidents', formatNum(s.total_accidents));
  setText('total-fatal', formatNum(s.total_fatal));
  setText('total-serious', formatNum(s.total_serious));
  setText('total-slight', formatNum(s.total_slight));
  setText('total-casualties', formatNum(s.total_casualties));

  // Charts
  renderBarChart('districts-chart', data.top_districts || [], 'local_authority_district', 'severity');
  renderBarChart('weather-chart', data.top_weather || [], 'weather', 'total');
  renderBarChart('age-chart', data.age_bands || [], 'age_band_of_driver', 'total');
}

function renderBarChart(containerId, items, labelKey, valueKey) {
  const container = document.getElementById(containerId);
  if (!items.length) { container.innerHTML = '<div style="color:#64748b">No data yet</div>'; return; }

  const maxVal = Math.max(...items.map(i => Number(i[valueKey]) || 0), 1);
  container.innerHTML = items.map(item => {
    const val = Number(item[valueKey]) || 0;
    const pct = Math.round((val / maxVal) * 100);
    const label = String(item[labelKey]).substring(0, 8);
    return `<div class="bar">
      <span class="bar-label">${label}</span>
      <div class="bar-fill" style="width:${pct}%">${formatNum(val)}</div>
    </div>`;
  }).join('');
}

// ── Prediction ──────────────────────────────────────────
async function loadFeatures() {
  try {
    const resp = await fetch('/api/predict/features');
    const opts = await resp.json();

    populateSelect('age-band', opts.Age_Band_of_Driver || []);
    populateSelect('sex-driver', opts.Sex_of_Driver || []);
    populateSelect('vehicle-type', opts.Vehicle_Type || []);
  } catch (e) {
    console.warn('Could not load features:', e);
  }
}

function populateSelect(id, options) {
  const el = document.getElementById(id);
  el.innerHTML = options.map(o => `<option value="${o}">${o}</option>`).join('');
}

async function runPrediction() {
  const body = {
    age_band_of_driver: document.getElementById('age-band').value,
    sex_of_driver: document.getElementById('sex-driver').value,
    vehicle_type: document.getElementById('vehicle-type').value,
  };

  const resultDiv = document.getElementById('predict-result');
  resultDiv.textContent = 'Predicting...';

  try {
    const resp = await fetch('/api/predict', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();

    if (data.predictions) {
      resultDiv.innerHTML = data.predictions.map(p =>
        `<strong>${p.label}:</strong> ${(p.probability * 100).toFixed(1)}%`
      ).join(' &nbsp;|&nbsp; ');

      if (data.model_accuracy) {
        resultDiv.innerHTML += `<br><small style="color:#64748b">Model accuracy: ${(data.model_accuracy * 100).toFixed(1)}%</small>`;
      }
    }
  } catch (e) {
    resultDiv.textContent = 'Prediction failed: ' + e.message;
  }
}

// ── Helpers ─────────────────────────────────────────────
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function formatNum(n) {
  if (n == null || n === '--') return '--';
  return Number(n).toLocaleString();
}

// ── Init ────────────────────────────────────────────────
connectWebSocket();
loadFeatures();
