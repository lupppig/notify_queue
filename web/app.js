const POLL_MS = 2000;
const STATUSES = ["pending", "queued", "claimed", "sent", "failed", "dead_lettered"];

const PAYLOAD_EXAMPLES = {
  email: { subject: "Welcome aboard", body: "Hi there, thanks for signing up." },
  sms: { message: "Your verification code is 123456" },
  push: { title: "Order shipped", body: "Your package is on its way" },
};

const $ = (sel) => document.querySelector(sel);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const state = {
  filter: null,
  lastUpdated: new Map(),
  channel: "email",
  priority: "medium",
  scheduleMode: "now",
};

async function request(path, options = {}, retries = 3) {
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await fetch(path, options);
      setConnected(true);
      return res;
    } catch (err) {
      if (attempt >= retries) {
        setConnected(false);
        throw err;
      }
      await sleep(250 * 2 ** attempt);
    }
  }
}

const getJSON = async (path) => (await request(path)).json();

function setConnected(ok) {
  const conn = $("#conn");
  conn.textContent = ok ? "connected" : "disconnected";
  conn.className = `conn ${ok ? "ok" : "down"}`;
}

const esc = (value) =>
  String(value ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]
  );

const shortId = (id) => id.slice(0, 8);
const ts = (iso) => (iso ? new Date(iso).toLocaleTimeString() : "");

function renderMetrics(counts) {
  $("#metrics").innerHTML = STATUSES.map(
    (s) => `<button type="button" class="tile ${state.filter === s ? "on" : ""}" data-status="${s}">
      <div class="n status-${s}">${counts[s]}</div>
      <div class="l">${s.replace("_", " ")}</div>
    </button>`
  ).join("");

  const total = STATUSES.reduce((sum, s) => sum + counts[s], 0);
  $("#metrics-bar").innerHTML = total
    ? STATUSES.filter((s) => counts[s] > 0)
        .map((s) => `<span class="bar-${s}" style="width:${(counts[s] / total) * 100}%"></span>`)
        .join("")
    : "";

  const clear = $("#clear-filter");
  clear.hidden = !state.filter;
  if (state.filter) clear.textContent = `showing ${state.filter.replace("_", " ")} — clear`;
}

function jobRow(j) {
  const changed = state.lastUpdated.has(j.id) && state.lastUpdated.get(j.id) !== j.updated_at;
  state.lastUpdated.set(j.id, j.updated_at);
  return `<tr class="${changed ? "flash" : ""}">
    <td title="${j.id}">${shortId(j.id)}</td>
    <td title="${esc(j.recipient)}">${esc(j.recipient)}</td>
    <td>${j.channel}</td>
    <td>${j.priority}</td>
    <td class="status-${j.status}" title="${esc(j.error_message)}">${j.status}</td>
    <td>${j.attempt_count}/${j.max_attempts}</td>
    <td>${ts(j.send_at)}</td>
    <td>${ts(j.updated_at)}</td>
  </tr>`;
}

function renderJobs(jobs) {
  $("#jobs tbody").innerHTML = jobs.length
    ? jobs.map(jobRow).join("")
    : `<tr><td colspan="8" class="empty">no jobs${state.filter ? " with this status" : " yet"}</td></tr>`;
}

function renderDlq(jobs) {
  $("#retry-all").hidden = jobs.length === 0;
  $("#dlq tbody").innerHTML = jobs.length
    ? jobs
        .map(
          (j) => `<tr>
            <td title="${j.id}">${shortId(j.id)}</td>
            <td>${esc(j.recipient)}</td>
            <td>${j.channel}</td>
            <td>${j.attempt_count}</td>
            <td title="${esc(j.error_message)}">${esc(j.error_message)}</td>
            <td><button data-retry="${j.id}">retry</button></td>
          </tr>`
        )
        .join("")
    : `<tr><td colspan="6" class="empty">empty</td></tr>`;
}

async function refresh() {
  try {
    const jobsPath = state.filter ? `/jobs?status=${state.filter}&limit=50` : "/jobs?limit=50";
    const [metrics, jobs, dlq] = await Promise.all([
      getJSON("/metrics"),
      getJSON(jobsPath),
      getJSON("/jobs?status=dead_lettered&limit=20"),
    ]);
    renderMetrics(metrics);
    renderJobs(jobs.jobs);
    renderDlq(dlq.jobs);
  } catch {
    // banner already shows disconnected; next poll retries
  }
}

let toastTimer;
function toast(message, kind = "ok") {
  const el = $("#toast");
  el.textContent = message;
  el.className = `toast ${kind}`;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), 4000);
}

function segmented(el, onChange) {
  el.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-value]");
    if (!btn) return;
    for (const b of el.querySelectorAll("button")) b.classList.toggle("on", b === btn);
    onChange(btn.dataset.value);
  });
}

function setPayloadExample() {
  const editor = $("#payload");
  if (editor.dataset.touched !== "1" || !editor.value.trim()) {
    editor.value = JSON.stringify(PAYLOAD_EXAMPLES[state.channel], null, 2);
  }
}

function parsePayload() {
  const editor = $("#payload");
  let payload;
  try {
    payload = JSON.parse(editor.value);
  } catch (err) {
    return invalid(editor, `payload: ${err.message}`);
  }
  if (payload === null || Array.isArray(payload) || typeof payload !== "object") {
    return invalid(editor, "payload must be a json object");
  }
  return payload;
}

function scheduleFields() {
  $("#delay-inputs").hidden = state.scheduleMode !== "delay";
  $("#send-at").hidden = state.scheduleMode !== "at";
  if (state.scheduleMode === "at" && !$("#send-at").value) {
    const inTenMinutes = new Date(Date.now() + 10 * 60 * 1000);
    inTenMinutes.setSeconds(0, 0);
    $("#send-at").value = new Date(
      inTenMinutes.getTime() - inTenMinutes.getTimezoneOffset() * 60 * 1000
    )
      .toISOString()
      .slice(0, 16);
  }
}

function invalid(el, message) {
  el.classList.add("invalid");
  el.addEventListener("input", () => el.classList.remove("invalid"), { once: true });
  $("#form-error").textContent = message;
  el.focus();
  return null;
}

function buildRequest() {
  $("#form-error").textContent = "";

  const recipient = $("#recipient").value.trim();
  if (!recipient) return invalid($("#recipient"), "recipient is required");
  if (state.channel === "email" && !recipient.includes("@"))
    return invalid($("#recipient"), "email channel needs an email address");

  const payload = parsePayload();
  if (payload === null) return null;

  const body = { recipient, channel: state.channel, priority: state.priority, payload };

  if (state.scheduleMode === "delay") {
    const amount = Number($("#delay-amount").value);
    if (!Number.isFinite(amount) || amount < 1)
      return invalid($("#delay-amount"), "delay must be at least 1");
    body.delay_seconds = amount * Number($("#delay-unit").value);
  } else if (state.scheduleMode === "at") {
    const value = $("#send-at").value;
    if (!value) return invalid($("#send-at"), "pick a delivery time");
    body.send_at = new Date(value).toISOString();
  } else {
    body.delay_seconds = 0;
  }

  const callbackUrl = $("#callback-url").value.trim();
  if (callbackUrl) body.callback_url = callbackUrl;
  const idempotencyKey = $("#idempotency-key").value.trim();
  if (idempotencyKey) body.idempotency_key = idempotencyKey;

  return body;
}

async function submitJob(event) {
  event.preventDefault();
  const body = buildRequest();
  if (!body) return;

  const button = $("#submit-btn");
  button.disabled = true;
  try {
    const res = await request("/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (res.status === 201) {
      toast(`scheduled ${shortId(data.job_id)} · ${body.priority} ${body.channel}`, "ok");
      $("#idempotency-key").value = "";
    } else if (res.status === 409) {
      toast(`duplicate idempotency key — existing job ${shortId(data.existing_job_id)}`, "warn");
    } else if (res.status === 422) {
      const detail = Array.isArray(data.detail)
        ? data.detail.map((d) => d.msg).join("; ")
        : String(data.detail ?? "");
      $("#form-error").textContent = detail || "invalid request";
    } else {
      toast(`unexpected error ${res.status}`, "bad");
    }
  } catch {
    toast("request failed — check the API", "bad");
  } finally {
    button.disabled = false;
    refresh();
  }
}

async function retryJob(id) {
  const res = await request(`/jobs/${id}/retry`, { method: "POST" });
  return res.status === 200;
}

async function onDlqClick(event) {
  const id = event.target.dataset.retry;
  if (!id) return;
  event.target.disabled = true;
  try {
    const ok = await retryJob(id);
    toast(ok ? `requeued ${shortId(id)}` : `could not retry ${shortId(id)}`, ok ? "ok" : "bad");
  } catch {
    toast(`could not retry ${shortId(id)}`, "bad");
  } finally {
    refresh();
  }
}

async function retryAll() {
  const button = $("#retry-all");
  button.disabled = true;
  try {
    const { jobs } = await getJSON("/jobs?status=dead_lettered&limit=200");
    const results = await Promise.all(jobs.map((j) => retryJob(j.id)));
    const ok = results.filter(Boolean).length;
    toast(`requeued ${ok}/${jobs.length} dead-lettered jobs`, ok === jobs.length ? "ok" : "warn");
  } finally {
    button.disabled = false;
    refresh();
  }
}

segmented($("#channel"), (value) => {
  state.channel = value;
  setPayloadExample();
});
segmented($("#priority"), (value) => (state.priority = value));
segmented($("#schedule-mode"), (value) => {
  state.scheduleMode = value;
  scheduleFields();
});

$("#job-form").addEventListener("submit", submitJob);
$("#payload").addEventListener("input", (e) => (e.target.dataset.touched = "1"));
$("#format-json").addEventListener("click", () => {
  const payload = parsePayload();
  if (payload !== null) $("#payload").value = JSON.stringify(payload, null, 2);
});
$("#use-mock").addEventListener("click", () => {
  $("#callback-url").value = `${location.origin}/webhook-mock`;
});
$("#dlq").addEventListener("click", onDlqClick);
$("#retry-all").addEventListener("click", retryAll);
$("#metrics").addEventListener("click", (e) => {
  const tile = e.target.closest("[data-status]");
  if (!tile) return;
  state.filter = state.filter === tile.dataset.status ? null : tile.dataset.status;
  refresh();
});
$("#clear-filter").addEventListener("click", () => {
  state.filter = null;
  refresh();
});

setPayloadExample();
scheduleFields();
refresh();
setInterval(refresh, POLL_MS);
