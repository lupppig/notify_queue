const POLL_MS = 2000;
const STATUSES = ["pending", "queued", "claimed", "sent", "failed", "dead_lettered"];

const conn = document.getElementById("conn");
const formMsg = document.getElementById("form-msg");
const lastUpdated = new Map();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

function setConnected(ok) {
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
  document.getElementById("metrics").innerHTML = STATUSES.map(
    (s) => `<div class="tile">
      <div class="n status-${s}">${counts[s]}</div>
      <div class="l">${s.replace("_", " ")}</div>
    </div>`
  ).join("");
}

function renderJobs(jobs) {
  const body = document.querySelector("#jobs tbody");
  if (!jobs.length) {
    body.innerHTML = `<tr><td colspan="8" class="empty">no jobs yet</td></tr>`;
    return;
  }
  body.innerHTML = jobs
    .map((j) => {
      const changed = lastUpdated.has(j.id) && lastUpdated.get(j.id) !== j.updated_at;
      lastUpdated.set(j.id, j.updated_at);
      return `<tr class="${changed ? "flash" : ""}">
        <td title="${j.id}">${shortId(j.id)}</td>
        <td>${esc(j.recipient)}</td>
        <td>${j.channel}</td>
        <td>${j.priority}</td>
        <td class="status-${j.status}">${j.status}</td>
        <td>${j.attempt_count}/${j.max_attempts}</td>
        <td>${ts(j.send_at)}</td>
        <td>${ts(j.updated_at)}</td>
      </tr>`;
    })
    .join("");
}

function renderDlq(jobs) {
  const body = document.querySelector("#dlq tbody");
  if (!jobs.length) {
    body.innerHTML = `<tr><td colspan="6" class="empty">empty</td></tr>`;
    return;
  }
  body.innerHTML = jobs
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
    .join("");
}

async function refresh() {
  try {
    const [metricsRes, jobsRes, dlqRes] = await Promise.all([
      request("/metrics"),
      request("/jobs?limit=50"),
      request("/jobs?status=dead_lettered&limit=20"),
    ]);
    renderMetrics(await metricsRes.json());
    renderJobs((await jobsRes.json()).jobs);
    renderDlq((await dlqRes.json()).jobs);
  } catch {
    // banner already shows disconnected; next poll retries
  }
}

document.getElementById("dlq").addEventListener("click", async (e) => {
  const id = e.target.dataset.retry;
  if (!id) return;
  e.target.disabled = true;
  try {
    await request(`/jobs/${id}/retry`, { method: "POST" });
  } finally {
    refresh();
  }
});

document.getElementById("job-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = e.target;
  let payload;
  try {
    payload = JSON.parse(form.payload.value || "{}");
  } catch {
    formMsg.textContent = "payload is not valid JSON";
    return;
  }
  const body = {
    recipient: form.recipient.value,
    channel: form.channel.value,
    priority: form.priority.value,
    delay_seconds: Number(form.delay_seconds.value || 0),
    payload,
  };
  if (form.callback_url.value) body.callback_url = form.callback_url.value;
  if (form.idempotency_key.value) body.idempotency_key = form.idempotency_key.value;

  try {
    const res = await request("/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    formMsg.textContent =
      res.status === 201
        ? `scheduled ${shortId(data.job_id)}`
        : res.status === 409
          ? `duplicate of ${shortId(data.existing_job_id)}`
          : `error ${res.status}`;
  } catch {
    formMsg.textContent = "request failed";
  }
  refresh();
});

document.querySelector('[name="callback_url"]').value = `${location.origin}/webhook-mock`;

refresh();
setInterval(refresh, POLL_MS);
