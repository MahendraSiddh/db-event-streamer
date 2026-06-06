// Client State
let lastSequenceId = '0';
let ws = null;
let reconnectDelay = 1000;
const MAX_RECONNECT_DELAY = 30000;

// UI Elements
const statusBadge = document.getElementById('connection-status');
const statusText = statusBadge.querySelector('.status-text');
const eventsList = document.getElementById('events-list');

// --- Helpers ---

// Safely escapes a value for text insertion — prevents XSS from DB data
function safeText(value) {
  if (value === null || value === undefined) return '-';
  return String(value);
}

// Creates a td element with safe text content (no innerHTML injection)
function createCell(value) {
  const td = document.createElement('td');
  td.textContent = safeText(value);
  return td;
}

// Creates a td containing a styled badge span with safe text content
function createBadgeCell(value, classPrefix) {
  const td = document.createElement('td');
  const span = document.createElement('span');
  const safeValue = safeText(value).toLowerCase();
  span.className = `badge ${classPrefix}-${safeValue}`;
  span.textContent = safeText(value).toUpperCase();
  td.appendChild(span);
  return td;
}

// Creates a td containing a styled status badge span
function createStatusCell(value) {
  const td = document.createElement('td');
  const span = document.createElement('span');
  const safeValue = safeText(value).toLowerCase();
  span.className = `badge-status status-${safeValue}`;
  span.textContent = safeText(value);
  td.appendChild(span);
  return td;
}

// --- WebSocket Connection & Resilience ---
function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}`;

  console.log(`Connecting to WebSocket at ${wsUrl}`);
  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    console.log('WebSocket connection established');
    statusBadge.className = 'status-badge connected';
    statusText.textContent = 'Connected';

    // Send handshake with last seen sequence ID
    ws.send(JSON.stringify({
      type: 'handshake',
      last_event_id: lastSequenceId
    }));

    reconnectDelay = 1000; // Reset reconnection backoff
  };

  ws.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data);
      if (message.type === 'order_change') {
        handleIncomingEvent(message);
      }
    } catch (err) {
      console.error('Error parsing incoming socket frame:', err);
    }
  };

  ws.onclose = () => {
    console.log('WebSocket connection closed. Reconnecting...');
    statusBadge.className = 'status-badge disconnected';
    statusText.textContent = 'Reconnecting...';
    scheduleReconnect();
  };

  ws.onerror = (err) => {
    console.error('WebSocket connection error:', err);
    ws.close();
  };
}

function scheduleReconnect() {
  setTimeout(() => {
    console.log(`Attempting reconnect in ${reconnectDelay}ms...`);
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
    connectWebSocket();
  }, reconnectDelay);
}

// --- Event Handlers ---
function handleIncomingEvent(event) {
  const { event_id, action, timestamp, data } = event;

  // Track largest sequence number seen for recovery handshakes
  try {
    if (BigInt(event_id) > BigInt(lastSequenceId)) {
      lastSequenceId = event_id.toString();
    }
  } catch (err) {
    console.error('Failed to compare event_id:', err);
  }

  // Remove empty row placeholder if present
  if (document.getElementById('empty-row')) {
    eventsList.innerHTML = '';
  }

  // Build table row using DOM methods — no innerHTML, no XSS risk
  const row = document.createElement('tr');
  const safeAction = safeText(action).toLowerCase();
  row.className = `event-row action-${safeAction}`;

  const timeFormatted = new Date(timestamp).toLocaleTimeString();

  row.appendChild(createCell(data ? data.id : null));
  row.appendChild(createCell(data ? data.customer_name : null));
  row.appendChild(createCell(data ? data.product_name : null));
  row.appendChild(createStatusCell(data ? data.status : null));
  row.appendChild(createBadgeCell(action, 'badge'));

  const timeTd = document.createElement('td');
  timeTd.textContent = timeFormatted;
  row.appendChild(timeTd);

  // Prepend to top of the list (descending timestamp order)
  eventsList.insertBefore(row, eventsList.firstChild);

  // Keep only the last 100 events in UI
  while (eventsList.children.length > 100) {
    eventsList.removeChild(eventsList.lastChild);
  }
}

// Initial trigger
(() => {
  connectWebSocket();
})();
