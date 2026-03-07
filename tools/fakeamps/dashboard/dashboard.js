const state = {
  session: null,
  root: null,
  host: null,
  instance: null,
  history: [],
  historyMetric: 'instance.connections.current',
  timeCursor: null,
  topics: [],
  topicSearch: '',
  sqlLog: [],
  sqlTopic: 'orders',
  sqlMode: 'sow',
  sqlFilter: '',
  sqlTopN: '100',
  sqlOrderBy: '',
  sqlBookmark: '',
  sqlDelta: false,
  sqlLive: true,
  workspaceRows: [],
  workspaceStatus: 'idle',
  workspaceRequestId: '',
  workspaceSocket: null,
  workspaceSocketPromise: null,
  activeTab: 'overview',
};

const app = document.getElementById('app');
const metadataColumns = ['topic', 'sow_key', 'bookmark', 'timestamp', 'type'];

function request(path, options = {}) {
  return fetch(path, {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  }).then(async (response) => {
    const contentType = response.headers.get('content-type') || '';
    const payload = contentType.includes('application/json') ? await response.json() : await response.text();
    if (!response.ok) {
      const error = new Error(typeof payload === 'string' ? payload : payload.error || response.statusText);
      error.status = response.status;
      error.payload = payload;
      throw error;
    }
    return payload;
  });
}

function escapeHTML(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Math.round(value || 0));
}

function formatTime(value) {
  if (!value) {
    return 'n/a';
  }
  return new Date(value).toLocaleTimeString();
}

function topicMeta(topic) {
  const sources = (topic.sources || []).join(', ');
  return `${topic.message_type || 'json'}${sources ? ` · ${sources}` : ''}`;
}

function addSQLLogEntry(entry) {
  state.sqlLog.unshift({
    type: entry.type || 'event',
    payload: typeof entry.payload === 'string' ? entry.payload : JSON.stringify(entry.payload, null, 2),
  });
  state.sqlLog = state.sqlLog.slice(0, 30);
  render();
}

function tabDefinitions(instance, host) {
  const counts = instance?.counts || {};
  return [
    { id: 'overview', label: 'Overview', meta: `${formatNumber(counts.connections_current)} live` },
    { id: 'metrics', label: 'Metrics', meta: `${state.history.length} samples` },
    { id: 'host', label: 'Host', meta: `${escapeHTML(host?.system?.goos || 'runtime')}` },
    { id: 'transports', label: 'Transports', meta: `${(instance?.transports || []).length} tracked` },
    { id: 'replication', label: 'Replication', meta: `${(instance?.replication || []).length} peers` },
    { id: 'workspace', label: 'Workspace', meta: state.workspaceStatus },
    { id: 'admin', label: 'Admin', meta: state.session?.role || 'viewer' },
  ];
}

async function refreshAll() {
  const [session, root, host, instance] = await Promise.all([
    request('/amps/session').catch(() => null),
    request('/amps'),
    request('/amps/host'),
    request('/amps/instance'),
  ]);
  state.session = session;
  state.root = root;
  state.host = host;
  state.instance = instance;
  await Promise.all([refreshHistory(), refreshTopics()]);
  render();
}

async function refreshHistory() {
  const end = new Date();
  const start = new Date(end.getTime() - 15 * 60 * 1000);
  const metric = encodeURIComponent(state.historyMetric);
  state.history = await request(`/amps/instance/history?metric=${metric}&start=${start.toISOString()}&end=${end.toISOString()}`);
  if (!state.timeCursor && state.history.length > 0) {
    state.timeCursor = state.history[state.history.length - 1].timestamp;
  }
}

async function refreshTopics() {
  const query = state.topicSearch.trim();
  const path = query ? `/amps/instance/topics?search=${encodeURIComponent(query)}` : '/amps/instance/topics';
  const payload = await request(path).catch(() => ({ topics: [] }));
  state.topics = payload.topics || [];
}

function chartPath(points, width, height) {
  if (!points.length) {
    return '';
  }
  const values = points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 1);
  return points.map((point, index) => {
    const x = (index / Math.max(points.length - 1, 1)) * width;
    const y = height - (((point.value - min) / range) * (height - 24) + 12);
    return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(' ');
}

function nextWorkspaceRequestId() {
  return `workspace-${Date.now()}`;
}

function normalizeWorkspaceRow(row) {
  const normalized = {
    topic: row.topic || '',
    sow_key: row.sow_key || '',
    bookmark: row.bookmark || '',
    timestamp: row.timestamp || '',
    type: row.type || 'record',
    payload: null,
    raw_payload: '',
  };

  if (row.payload && typeof row.payload === 'object' && !Array.isArray(row.payload)) {
    normalized.payload = row.payload;
  } else if (typeof row.raw_payload === 'string') {
    normalized.raw_payload = row.raw_payload;
  } else if (typeof row.payload === 'string') {
    normalized.raw_payload = row.payload;
  } else if (row.payload != null) {
    normalized.raw_payload = JSON.stringify(row.payload);
  }

  normalized._rowKey = normalized.sow_key
    ? `${normalized.topic}::${normalized.sow_key}`
    : `${normalized.topic}::${normalized.bookmark || normalized.timestamp || Math.random().toString(16).slice(2)}`;
  return normalized;
}

function workspaceColumns() {
  const payloadFields = new Set();
  let hasRawPayload = false;

  state.workspaceRows.forEach((row) => {
    if (row.payload) {
      Object.keys(row.payload).forEach((field) => payloadFields.add(field));
    }
    if (row.raw_payload) {
      hasRawPayload = true;
    }
  });

  const columns = [...metadataColumns, ...Array.from(payloadFields).sort()];
  if (hasRawPayload) {
    columns.push('raw_payload');
  }
  return columns;
}

function mergeWorkspaceRow(row) {
  const normalized = normalizeWorkspaceRow(row);
  if (normalized.sow_key) {
    const index = state.workspaceRows.findIndex((current) => current._rowKey === normalized._rowKey);
    if (index >= 0) {
      state.workspaceRows[index] = normalized;
      return;
    }
  }
  state.workspaceRows = [normalized, ...state.workspaceRows].slice(0, 250);
}

function removeWorkspaceRow(topic, sowKey) {
  if (!sowKey) {
    return;
  }
  state.workspaceRows = state.workspaceRows.filter((row) => !(row.topic === topic && row.sow_key === sowKey));
}

function handleLegacyWorkspacePayload(message) {
  switch (message.type) {
    case 'sow_result':
      state.workspaceRows = (message.records || []).map(normalizeWorkspaceRow);
      state.workspaceStatus = 'complete';
      break;
    case 'sow_and_subscribe_result':
      state.workspaceRows = (message.records || []).map(normalizeWorkspaceRow);
      state.workspaceStatus = 'live';
      break;
    case 'subscribe_ready':
      state.workspaceStatus = 'live';
      break;
    case 'error':
      state.workspaceStatus = 'error';
      addSQLLogEntry({ type: 'error', payload: message.error || 'workspace error' });
      return;
    default:
      addSQLLogEntry({ type: 'legacy', payload: message });
      return;
  }
  render();
}

function handleWorkspaceMessage(message) {
  if (message.request_id && message.request_id !== state.workspaceRequestId) {
    return;
  }

  switch (message.type) {
    case 'workspace_ready':
      state.workspaceStatus = message.live ? 'live' : 'running';
      break;
    case 'workspace_snapshot':
      state.workspaceRows = (message.rows || []).map(normalizeWorkspaceRow);
      break;
    case 'workspace_row':
      if (message.row) {
        mergeWorkspaceRow(message.row);
      }
      break;
    case 'workspace_remove':
      removeWorkspaceRow(message.topic, message.sow_key);
      addSQLLogEntry({ type: 'remove', payload: message });
      break;
    case 'workspace_complete':
      state.workspaceStatus = 'complete';
      addSQLLogEntry({ type: 'complete', payload: message });
      break;
    case 'workspace_error':
      state.workspaceStatus = 'error';
      addSQLLogEntry({ type: 'error', payload: message.error || 'workspace error' });
      break;
    case 'sow_result':
    case 'sow_and_subscribe_result':
    case 'subscribe_ready':
    case 'error':
      handleLegacyWorkspacePayload(message);
      return;
    default:
      addSQLLogEntry({ type: 'message', payload: message });
      break;
  }
  render();
}

function ensureWorkspaceSocket() {
  if (state.workspaceSocket && state.workspaceSocket.readyState === WebSocket.OPEN) {
    return Promise.resolve(state.workspaceSocket);
  }
  if (state.workspaceSocketPromise) {
    return state.workspaceSocketPromise;
  }

  const scheme = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
  state.workspaceSocketPromise = new Promise((resolve, reject) => {
    const socket = new WebSocket(`${scheme}${window.location.host}/amps/sql/ws`);
    state.workspaceSocket = socket;

    socket.addEventListener('open', () => {
      state.workspaceSocketPromise = null;
      resolve(socket);
    }, { once: true });

    socket.addEventListener('message', (event) => {
      try {
        handleWorkspaceMessage(JSON.parse(event.data));
      } catch (error) {
        addSQLLogEntry({ type: 'error', payload: 'invalid workspace response' });
      }
    });

    socket.addEventListener('close', () => {
      if (state.workspaceSocket === socket) {
        state.workspaceSocket = null;
        state.workspaceSocketPromise = null;
        if (state.workspaceStatus === 'live' || state.workspaceStatus === 'running') {
          state.workspaceStatus = 'disconnected';
          render();
        }
      }
    });

    socket.addEventListener('error', () => {
      addSQLLogEntry({ type: 'error', payload: 'workspace connection failed' });
      if (state.workspaceSocketPromise) {
        state.workspaceSocketPromise = null;
        reject(new Error('workspace connection failed'));
      }
    }, { once: true });
  });

  return state.workspaceSocketPromise;
}

async function runWorkspaceQuery() {
  state.workspaceRows = [];
  state.sqlLog = [];
  state.workspaceStatus = 'connecting';
  state.workspaceRequestId = nextWorkspaceRequestId();
  render();

  const socket = await ensureWorkspaceSocket();
  const options = {
    order_by: state.sqlOrderBy,
    bookmark: state.sqlBookmark,
    delta: state.sqlDelta,
    live: state.sqlLive,
  };
  if (state.sqlTopN.trim() !== '') {
    options.top_n = Number.parseInt(state.sqlTopN, 10) || 0;
  }
  socket.send(JSON.stringify({
    type: 'run',
    request_id: state.workspaceRequestId,
    mode: state.sqlMode,
    topic: state.sqlTopic,
    filter: state.sqlFilter,
    options,
  }));
}

function stopWorkspaceQuery() {
  if (!state.workspaceSocket || state.workspaceSocket.readyState !== WebSocket.OPEN || !state.workspaceRequestId) {
    return;
  }
  state.workspaceSocket.send(JSON.stringify({
    type: 'stop',
    request_id: state.workspaceRequestId,
  }));
}

function clearWorkspaceOutput() {
  state.workspaceRows = [];
  state.sqlLog = [];
  state.workspaceStatus = 'idle';
  state.workspaceRequestId = '';
  render();
}

async function handleLogin(event) {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  await request('/amps/session/login', {
    method: 'POST',
    body: JSON.stringify({
      username: form.get('username'),
      password: form.get('password'),
    }),
  });
  await refreshAll();
}

async function handleLogout() {
  await request('/amps/session/logout', { method: 'POST' });
  state.session = null;
  render();
}

async function runDiagnostics() {
  const result = await request('/amps/administrator/diagnostics', { method: 'POST' });
  addSQLLogEntry({ type: 'diagnostics', payload: JSON.stringify(result, null, 2) });
}

function renderSession(stateSession) {
  if (stateSession) {
    return `
      <div class="session-strip">
        <span class="pill">${escapeHTML(stateSession.role)} · ${escapeHTML(stateSession.username)}</span>
        <button class="secondary" id="logout-button">Log out</button>
      </div>
    `;
  }

  return `
    <div class="session-strip">
      <span class="pill danger">anonymous</span>
    </div>
    <form class="login-form" id="login-form">
      <input name="username" placeholder="username" autocomplete="username">
      <input name="password" type="password" placeholder="password" autocomplete="current-password">
      <button type="submit">Start session</button>
    </form>
  `;
}

function renderTabList(instance, host) {
  const tabs = tabDefinitions(instance, host);
  return `
    <nav class="tab-list" aria-label="dashboard views">
      ${tabs.map((tab) => `
        <button class="tab${tab.id === state.activeTab ? ' active' : ''}" data-tab="${tab.id}" type="button">
          <span>${escapeHTML(tab.label)}</span>
          <small>${tab.meta}</small>
        </button>
      `).join('')}
    </nav>
  `;
}

function renderOverviewView(instance, host, root) {
  const counts = instance.counts || {};
  const status = instance.status || {};
  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Overview</span>
          <h2>Current fake broker state</h2>
        </div>
        <span class="muted">Updated ${escapeHTML(formatTime(instance.sampled_at))}</span>
      </header>
      <div class="stage-grid">
        <article class="view-panel">
          <h3>Traffic</h3>
          <div class="stat-grid">
            <div class="stat-card"><span class="muted">Connections</span><strong>${formatNumber(counts.connections_current)}</strong></div>
            <div class="stat-card"><span class="muted">Subscriptions</span><strong>${formatNumber(counts.subscriptions)}</strong></div>
            <div class="stat-card"><span class="muted">SOW Records</span><strong>${formatNumber(counts.sow_records)}</strong></div>
            <div class="stat-card"><span class="muted">Journal Entries</span><strong>${formatNumber(counts.journal_entries)}</strong></div>
          </div>
        </article>
        <article class="view-panel">
          <h3>Addresses</h3>
          <dl class="detail-list">
            <div><dt>Admin</dt><dd>${escapeHTML(status.admin_listener || 'n/a')}</dd></div>
            <div><dt>External</dt><dd>${escapeHTML(status.external_addr || root.external_inet_addr || 'n/a')}</dd></div>
            <div><dt>SQL Transport</dt><dd>${escapeHTML(status.sql_transport || 'n/a')}</dd></div>
            <div><dt>Host Runtime</dt><dd>${escapeHTML(host.system?.goos || 'n/a')}/${escapeHTML(host.system?.goarch || 'n/a')}</dd></div>
          </dl>
        </article>
      </div>
    </section>
  `;
}

function renderMetricsView() {
  const path = chartPath(state.history, 900, 320);
  const historyOptions = [
    'instance.connections.current',
    'instance.subscriptions.count',
    'instance.sow.records',
    'host.memory.alloc_mb',
    'host.runtime.goroutines',
  ].map((metric) => `<option value="${metric}"${metric === state.historyMetric ? ' selected' : ''}>${metric}</option>`).join('');

  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Metrics</span>
          <h2>Retained history and time machine</h2>
        </div>
        <span class="muted">Cursor ${escapeHTML(formatTime(state.timeCursor))}</span>
      </header>
      <article class="view-panel">
        <div class="toolbar">
          <select id="metric-select">${historyOptions}</select>
          <button class="secondary" id="refresh-button">Refresh</button>
        </div>
        <svg class="chart chart-large" viewBox="0 0 900 320" preserveAspectRatio="none">
          <path class="line" d="${path}"></path>
        </svg>
        <div class="snapshot-bar">
          <span>Samples ${state.history.length}</span>
          <span>Metric ${escapeHTML(state.historyMetric)}</span>
        </div>
      </article>
    </section>
  `;
}

function renderHostView(host) {
  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Host</span>
          <h2>Process and runtime profile</h2>
        </div>
        <span class="muted">${escapeHTML(host.system?.goos || 'n/a')}/${escapeHTML(host.system?.goarch || 'n/a')}</span>
      </header>
      <div class="stage-grid">
        <article class="view-panel">
          <h3>Memory</h3>
          <div class="stat-grid">
            <div class="stat-card"><span class="muted">Alloc MB</span><strong>${formatNumber(host.memory?.alloc_mb)}</strong></div>
            <div class="stat-card"><span class="muted">Total MB</span><strong>${formatNumber(host.memory?.total_alloc_mb)}</strong></div>
            <div class="stat-card"><span class="muted">Sys MB</span><strong>${formatNumber(host.memory?.sys_mb)}</strong></div>
            <div class="stat-card"><span class="muted">GC Cycles</span><strong>${formatNumber(host.memory?.num_gc)}</strong></div>
          </div>
        </article>
        <article class="view-panel">
          <h3>Runtime</h3>
          <div class="stat-grid">
            <div class="stat-card"><span class="muted">Goroutines</span><strong>${formatNumber(host.runtime?.goroutines)}</strong></div>
            <div class="stat-card"><span class="muted">GOMAXPROCS</span><strong>${formatNumber(host.runtime?.gomaxprocs)}</strong></div>
            <div class="stat-card"><span class="muted">CPU Count</span><strong>${formatNumber(host.runtime?.num_cpu)}</strong></div>
          </div>
        </article>
      </div>
    </section>
  `;
}

function renderTransportsView(instance) {
  const transportRows = (instance.transports || []).map((transport) => `
    <tr>
      <td>${escapeHTML(transport.name || transport.id)}</td>
      <td>${escapeHTML(transport.type)}</td>
      <td>${escapeHTML(transport.inet_addr)}</td>
      <td>${transport.enabled ? '<span class="pill">enabled</span>' : '<span class="pill danger">disabled</span>'}</td>
    </tr>
  `).join('');

  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Transports</span>
          <h2>Registered endpoints</h2>
        </div>
        <span class="muted">${(instance.transports || []).length} tracked</span>
      </header>
      <article class="view-panel">
        <table class="table">
          <thead><tr><th>Name</th><th>Type</th><th>Address</th><th>Status</th></tr></thead>
          <tbody>${transportRows || '<tr><td colspan="4" class="muted">No transports available</td></tr>'}</tbody>
        </table>
      </article>
    </section>
  `;
}

function renderReplicationView(instance) {
  const replicationRows = (instance.replication || []).map((peer) => `
    <tr>
      <td>${escapeHTML(peer.id)}</td>
      <td>${escapeHTML(peer.address)}</td>
      <td>${peer.connected ? '<span class="pill">connected</span>' : '<span class="pill danger">down</span>'}</td>
      <td>${escapeHTML(peer.mode)}</td>
    </tr>
  `).join('');

  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Replication</span>
          <h2>Peer state</h2>
        </div>
        <span class="muted">${(instance.replication || []).length} peers</span>
      </header>
      <article class="view-panel">
        <table class="table">
          <thead><tr><th>ID</th><th>Address</th><th>Link</th><th>Mode</th></tr></thead>
          <tbody>${replicationRows || '<tr><td colspan="4" class="muted">No replication peers configured</td></tr>'}</tbody>
        </table>
      </article>
    </section>
  `;
}

function renderWorkspaceTable() {
  const columns = workspaceColumns();
  const rows = state.workspaceRows.map((row) => `
    <tr>
      ${columns.map((column) => {
        let value = '';
        if (metadataColumns.includes(column)) {
          value = row[column] || '';
        } else if (column === 'raw_payload') {
          value = row.raw_payload || '';
        } else if (row.payload) {
          value = row.payload[column] == null ? '' : row.payload[column];
        }
        if (typeof value === 'object' && value !== null) {
          value = JSON.stringify(value);
        }
        return `<td>${escapeHTML(value)}</td>`;
      }).join('')}
    </tr>
  `).join('');

  return `
    <div class="workspace-results" id="workspace-results">
      <table class="table workspace-table">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHTML(column)}</th>`).join('')}</tr>
        </thead>
        <tbody>${rows || `<tr><td colspan="${columns.length}" class="muted">No rows yet. Run a query or start a live subscription.</td></tr>`}</tbody>
      </table>
    </div>
  `;
}

function renderWorkspaceView() {
  const topicButtons = state.topics.map((topic) => `
    <button class="topic-item${topic.name === state.sqlTopic ? ' active' : ''}" data-topic-select="${escapeHTML(topic.name)}" type="button">
      <strong>${escapeHTML(topic.name)}</strong>
      <small>${escapeHTML(topicMeta(topic))}</small>
      <span class="topic-count">${formatNumber(topic.sow_records)} records · ${formatNumber(topic.subscription_count)} subs</span>
    </button>
  `).join('');

  const logOutput = state.sqlLog.map((entry) => `[${entry.type}] ${entry.payload}`).join('\n');

  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Workspace</span>
          <h2>Read-only browser query console</h2>
        </div>
        <span class="muted live-status" id="live-status">${escapeHTML(state.workspaceStatus)}</span>
      </header>
      <div class="workspace-shell">
        <article class="view-panel topic-browser">
          <h3>Available Topics</h3>
          <div class="toolbar stack">
            <input id="topic-search" value="${escapeHTML(state.topicSearch)}" placeholder="search topics">
            <button class="secondary" id="topic-refresh" type="button">Refresh topics</button>
          </div>
          <div class="topic-list">
            ${topicButtons || '<div class="muted">No topics matched this search.</div>'}
          </div>
        </article>

        <div class="workspace-main">
          <article class="view-panel workspace-query">
            <h3>Query Builder</h3>
            <div class="toolbar stack">
              <select id="sql-mode">
                <option value="sow"${state.sqlMode === 'sow' ? ' selected' : ''}>sow</option>
                <option value="subscribe"${state.sqlMode === 'subscribe' ? ' selected' : ''}>subscribe</option>
                <option value="sow_and_subscribe"${state.sqlMode === 'sow_and_subscribe' ? ' selected' : ''}>sow_and_subscribe</option>
              </select>
              <input id="sql-topic" value="${escapeHTML(state.sqlTopic)}" placeholder="topic or wildcard">
              <input id="sql-filter" value="${escapeHTML(state.sqlFilter)}" placeholder="filter, for example /status = 'open'">
            </div>
            <div class="workspace-advanced">
              <div class="workspace-advanced-title">Advanced options</div>
              <div class="workspace-advanced-grid">
                <input id="sql-top-n" value="${escapeHTML(state.sqlTopN)}" placeholder="top_n">
                <input id="sql-order-by" value="${escapeHTML(state.sqlOrderBy)}" placeholder="order_by">
                <input id="sql-bookmark" value="${escapeHTML(state.sqlBookmark)}" placeholder="bookmark">
                <label class="checkbox-row"><input id="sql-delta" type="checkbox"${state.sqlDelta ? ' checked' : ''}> delta</label>
                <label class="checkbox-row"><input id="sql-live" type="checkbox"${state.sqlLive ? ' checked' : ''}> live</label>
              </div>
            </div>
            <div class="toolbar">
              <button id="sql-run" type="button">Run workspace</button>
              <button class="secondary" id="sql-stop" type="button">Stop</button>
              <button class="secondary" id="sql-clear" type="button">Clear</button>
            </div>
          </article>

          <article class="view-panel">
            <h3>Results</h3>
            ${renderWorkspaceTable()}
          </article>

          <article class="view-panel workspace-events">
            <h3>Events</h3>
            <div class="log">${escapeHTML(logOutput || 'Workspace events, lifecycle messages, and compatibility errors will appear here.')}</div>
          </article>
        </div>
      </div>
    </section>
  `;
}

function renderAdminView(instance) {
  const status = instance.status || {};
  return `
    <section class="stage-shell">
      <header class="stage-header">
        <div>
          <span class="section-tag">Admin</span>
          <h2>Operator actions</h2>
        </div>
        <span class="muted">${escapeHTML(status.auth_mode || 'open')}</span>
      </header>
      <div class="stage-grid">
        <article class="view-panel">
          <h3>Diagnostics</h3>
          <p class="muted">Run a structured diagnostic snapshot for the fake broker. Write actions remain permission-aware and stay deterministic when unsupported.</p>
          <button id="diagnostics-button" ${state.session?.role === 'operator' ? '' : 'disabled'}>Run diagnostics</button>
        </article>
        <article class="view-panel">
          <h3>Current access</h3>
          <dl class="detail-list">
            <div><dt>Role</dt><dd>${escapeHTML(state.session?.role || 'anonymous')}</dd></div>
            <div><dt>SQL Transport</dt><dd>${escapeHTML(status.sql_transport || 'n/a')}</dd></div>
            <div><dt>Admin Listener</dt><dd>${escapeHTML(status.admin_listener || 'n/a')}</dd></div>
          </dl>
        </article>
      </div>
    </section>
  `;
}

function renderActiveView(instance, host, root) {
  switch (state.activeTab) {
    case 'metrics':
      return renderMetricsView();
    case 'host':
      return renderHostView(host);
    case 'transports':
      return renderTransportsView(instance);
    case 'replication':
      return renderReplicationView(instance);
    case 'workspace':
      return renderWorkspaceView();
    case 'admin':
      return renderAdminView(instance);
    case 'overview':
    default:
      return renderOverviewView(instance, host, root);
  }
}

function render() {
  const instance = state.instance || { counts: {}, status: {}, transports: [], replication: [] };
  const host = state.host || { memory: {}, runtime: {}, system: {} };
  const root = state.root || {};

  app.innerHTML = `
    <main class="shell">
      <section class="hero">
        <div class="brand">
          <span class="eyebrow">fakeamps monitor</span>
          <h1>fake broker monitoring and control.</h1>
          <p>This dashboard is for the fakeamps test harness inside <code>amps-client-go</code>. Pick a tab to work one area at a time instead of scanning every card at once.</p>
          ${renderSession(state.session)}
        </div>
      </section>
      ${renderTabList(instance, host)}
      ${renderActiveView(instance, host, root)}
    </main>
  `;

  const loginForm = document.getElementById('login-form');
  if (loginForm) {
    loginForm.addEventListener('submit', handleLogin);
  }

  const logoutButton = document.getElementById('logout-button');
  if (logoutButton) {
    logoutButton.addEventListener('click', handleLogout);
  }

  document.querySelectorAll('[data-tab]').forEach((button) => {
    button.addEventListener('click', () => {
      state.activeTab = button.dataset.tab;
      render();
    });
  });

  const diagnosticsButton = document.getElementById('diagnostics-button');
  if (diagnosticsButton) {
    diagnosticsButton.addEventListener('click', runDiagnostics);
  }

  const refreshButton = document.getElementById('refresh-button');
  if (refreshButton) {
    refreshButton.addEventListener('click', async () => {
      const select = document.getElementById('metric-select');
      if (select) {
        state.historyMetric = select.value;
      }
      await refreshHistory();
      render();
    });
  }

  const topicSearch = document.getElementById('topic-search');
  if (topicSearch) {
    topicSearch.addEventListener('input', async (event) => {
      state.topicSearch = event.currentTarget.value;
      await refreshTopics();
      render();
    });
  }

  const topicRefresh = document.getElementById('topic-refresh');
  if (topicRefresh) {
    topicRefresh.addEventListener('click', async () => {
      await refreshTopics();
      render();
    });
  }

  document.querySelectorAll('[data-topic-select]').forEach((button) => {
    button.addEventListener('click', () => {
      state.sqlTopic = button.dataset.topicSelect;
      render();
    });
  });

  const sqlMode = document.getElementById('sql-mode');
  if (sqlMode) {
    sqlMode.addEventListener('change', (event) => {
      state.sqlMode = event.currentTarget.value;
    });
  }

  const sqlTopic = document.getElementById('sql-topic');
  if (sqlTopic) {
    sqlTopic.addEventListener('input', (event) => {
      state.sqlTopic = event.currentTarget.value;
    });
  }

  const sqlFilter = document.getElementById('sql-filter');
  if (sqlFilter) {
    sqlFilter.addEventListener('input', (event) => {
      state.sqlFilter = event.currentTarget.value;
    });
  }

  const sqlTopN = document.getElementById('sql-top-n');
  if (sqlTopN) {
    sqlTopN.addEventListener('input', (event) => {
      state.sqlTopN = event.currentTarget.value;
    });
  }

  const sqlOrderBy = document.getElementById('sql-order-by');
  if (sqlOrderBy) {
    sqlOrderBy.addEventListener('input', (event) => {
      state.sqlOrderBy = event.currentTarget.value;
    });
  }

  const sqlBookmark = document.getElementById('sql-bookmark');
  if (sqlBookmark) {
    sqlBookmark.addEventListener('input', (event) => {
      state.sqlBookmark = event.currentTarget.value;
    });
  }

  const sqlDelta = document.getElementById('sql-delta');
  if (sqlDelta) {
    sqlDelta.addEventListener('change', (event) => {
      state.sqlDelta = event.currentTarget.checked;
    });
  }

  const sqlLive = document.getElementById('sql-live');
  if (sqlLive) {
    sqlLive.addEventListener('change', (event) => {
      state.sqlLive = event.currentTarget.checked;
    });
  }

  const sqlRun = document.getElementById('sql-run');
  if (sqlRun) {
    sqlRun.addEventListener('click', () => {
      runWorkspaceQuery().catch((error) => {
        state.workspaceStatus = 'error';
        addSQLLogEntry({ type: 'error', payload: error.message || 'workspace run failed' });
      });
    });
  }

  const sqlStop = document.getElementById('sql-stop');
  if (sqlStop) {
    sqlStop.addEventListener('click', stopWorkspaceQuery);
  }

  const sqlClear = document.getElementById('sql-clear');
  if (sqlClear) {
    sqlClear.addEventListener('click', clearWorkspaceOutput);
  }
}

refreshAll().catch((error) => {
  addSQLLogEntry({ type: 'error', payload: error.message || 'dashboard bootstrap failed' });
  render();
});

setInterval(() => {
  refreshAll().catch(() => {});
}, 5000);
