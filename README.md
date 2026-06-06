# Real-Time Order Event Streamer

A real-time system that propagates database changes from a PostgreSQL `orders` table to connected clients in a web interface.

The system uses Log-Based Change Data Capture (CDC) with PostgreSQL logical replication, Redis Streams, and WebSockets.

## Approach & Design Decisions

Instead of periodic polling or trigger-based notify functions (LISTEN/NOTIFY), this system uses a log-based Change Data Capture (CDC) approach using PostgreSQL logical replication.

### Why Logical Replication?
- **Asynchronous CDC**: Reading directly from the PostgreSQL Write-Ahead Log (WAL) decouples the event generation from SQL transactions. Database writes aren't blocked or slowed down by network issues or downstream event delays.
- **Transaction ordering**: PostgreSQL streams WAL records in the exact order transactions are committed. This provides a natural, transactionally consistent order of event sequence numbers derived directly from the LSN (Log Sequence Number).
- **Decoupled Services**: By separating the database reader daemon (`db-listener`) from the client-facing WebSocket broker (`api-ws-server`), we prevent client traffic spikes from interfering with database event capture. Redis Streams serves as both the transient event log and the historical replay cache.

---

## Architecture

- **Postgres Database**: Contains the `orders` table and exposes a logical replication slot (`orders_slot`) publishing changes on table `orders`.
- **DB Listener Service**: Connects to the logical replication slot, captures WAL events (insert, update, delete), and appends them to a Redis Stream with a bounding capacity limit of 100 events.
- **API/WebSocket Server**: Exposes a GET API `/api/orders` to fetch the current database state, reads from the Redis Stream to listen for new database events, and broadcasts updates to connected WebSocket clients. On handshake, it replays missed events from the Redis Stream starting after the client's last seen event ID to recover client state.
- **Client (Frontend)**: A simple web dashboard displaying order updates in real-time.

## Environment Configurations

The system relies on the following environment variables:

| Variable | Description | Default (Docker) | Default (Local) |
| :--- | :--- | :--- | :--- |
| `PORT` | Listening port for the API server | `3000` | `3000` |
| `DATABASE_URL` | PostgreSQL connection string | `postgres://pguser:pgpassword@db:5432/order_stream` | `postgres://localhost:5432/order_stream` |
| `REDIS_URL` | Redis server URL | `redis://redis:6379` | `redis://localhost:6379` |
| `REDIS_STREAM_KEY` | Redis Stream key | `orders:stream` | `orders:stream` |

---

## How to Run

### Option A: Using Docker 

**Prerequisites:** Docker and Docker Compose installed and running.

1. **Start the system**:
   ```bash
   docker compose up --build
   ```
2. **Access the dashboard**:
   Open [http://localhost:3000](http://localhost:3000) in your browser.

3. **Test event streaming**:
   Run database commands to trigger CDC replication:
   ```bash
   docker compose exec db psql -U pguser -d order_stream -c "INSERT INTO orders (customer_name, product_name, status) VALUES ('John Doe', 'Pixel 8', 'pending');"
   ```

### Option B: Local Setup (Without Docker)

**Prerequisites:** Node.js (v18+), PostgreSQL (v15+ with logical replication active, i.e., `wal_level = logical`), and Redis (v7+).

1. **Configure PostgreSQL**:
   Update your `postgresql.conf` to set `wal_level = logical` and restart your database instance.
   Run the database initialization commands from [db/init.sql](file:///Users/mahendranath/Desktop/work/db-event-streamer/db/init.sql) to set up tables, publications, and slots.

2. **Run DB Listener Service**:
   ```bash
   cd db-listener
   npm install
   # Set DATABASE_URL and REDIS_URL environment variables
   npm start
   ```

3. **Run API & WebSocket Service**:
   ```bash
   cd api-ws-server
   npm install
   # Set DATABASE_URL and REDIS_URL environment variables
   npm start
   ```

---

## API & Protocol Documentation

### 1. REST API: GET `/api/orders`
Fetches the current list of orders stored in the PostgreSQL database.

* **Response Format (`200 OK`)**:
  ```json
  {
    "status": "success",
    "results": 1,
    "data": {
      "orders": [
        {
          "id": 1,
          "customer_name": "John Doe",
          "product_name": "Pixel 8",
          "status": "pending",
          "updated_at": "2026-06-06T09:00:00.000Z"
        }
      ]
    }
  }
  ```

### 2. WebSocket Connection Protocol
The client establishes a WebSocket connection to the root endpoint (`ws://localhost:3000`).

#### A. Handshake (Client Recovery)
Upon opening a connection, the client must send a handshake payload indicating the last sequence number (`event_id` or LSN position) it received to fetch missed history events:
```json
{
  "type": "handshake",
  "last_event_id": "2364537160"
}
```

#### B. Event Broadcasts (Server -> Client)
The server pushes notifications to all active clients for inserts, updates, deletes, and database state resync events.
```json
{
  "type": "order_change",
  "event_id": "2364537160",
  "action": "INSERT",
  "timestamp": "2026-06-06T09:05:00.000Z",
  "data": {
    "id": 2,
    "customer_name": "Jane Doe",
    "product_name": "iPhone 15",
    "status": "pending",
    "updated_at": "2026-06-06T09:05:00.000Z"
  },
  "replayed": false
}
```
*(Note: If `replayed` is `true`, the event was fetched from the Redis Stream during connection recovery.)*

#### C. SYNC Events (Slot-Loss Recovery)
If the `db-listener`'s replication slot is invalidated (e.g., after a prolonged outage causing WAL to be recycled), the service drops, re-creates the slot, and emits a full `SYNC` sweep of all current orders before resuming live streaming. These events share the same `type: order_change` envelope but carry `action: "SYNC"`.
```json
{
  "type": "order_change",
  "event_id": "23645371600000",
  "action": "SYNC",
  "timestamp": "2026-06-06T09:05:00.000Z",
  "data": {
    "id": 1,
    "customer_name": "John Doe",
    "product_name": "Pixel 8",
    "status": "pending",
    "updated_at": "2026-06-06T09:00:00.000Z"
  },
  "replayed": false
}
```

> **Recovery history cap:** The Redis Stream stores the last **100 events** (configured via `STREAM_LIMIT` in `db-listener/index.js` and trimmed using `MAXLEN`). A client offline for more than 100 events will recover only the most recent 100 on reconnection.

### 3. Health Check: GET `/health`

A lightweight liveness probe exposed by `api-ws-server`, used by Docker Compose to verify the service is alive.

* **Response Format (`200 OK`)**:
  ```json
  { "status": "ok", "timestamp": "2026-06-06T09:05:00.000Z" }
  ```

  <img width="1440" height="900" alt="Screenshot 2026-06-06 at 16 48 10" src="https://github.com/user-attachments/assets/c170baf7-4832-425d-b73d-7c04db70ea02" />


