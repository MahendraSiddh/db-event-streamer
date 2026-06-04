const { LogicalReplicationService, PgoutputPlugin } = require('pg-logical-replication');
const { Client } = require('pg');
const EventEmitter = require('events');

// Maximum time in ms to wait before flushing a pending WAL acknowledgement.
// Batching ACKs prevents a per-event serial round-trip under high insert rates.
const ACK_DEBOUNCE_MS = 200;

class PostgresListener extends EventEmitter {
  constructor(connectionString, onNotification) {
    super();
    this.connectionString = connectionString;
    this.onNotification = onNotification;
    this.service = null;
    this.reconnectTimer = null;
    this.retryDelay = 1000;
    this.MAX_RETRY_DELAY = 30000;
    this.isShuttingDown = false;
    this.publication = 'orders_pub';
    this.slot = 'orders_slot';

    // Batched ACK state
    this._pendingAckLsn = null;
    this._ackTimer = null;

    // Sequence tracking for duplicate LSNs
    this.lastLsn = '';
    this.lsnSequence = 0n;
  }

  // Schedules a debounced WAL acknowledgement for the given LSN.
  // Only the highest LSN received within the debounce window is sent,
  // which covers all earlier positions implicitly.
  _scheduleAck(lsn) {
    this._pendingAckLsn = lsn;
    if (this._ackTimer) return; // timer already running — let it fire
    this._ackTimer = setTimeout(async () => {
      this._ackTimer = null;
      const lsnToAck = this._pendingAckLsn;
      this._pendingAckLsn = null;
      if (lsnToAck && this.service) {
        try {
          await this.service.acknowledge(lsnToAck);
        } catch (err) {
          console.error(`[PostgresListener]: Failed to acknowledge LSN ${lsnToAck}:`, err.message);
        }
      }
    }, ACK_DEBOUNCE_MS);
  }

  // Performs a database table scan to load latest state if replication slot was lost
  async syncCurrentDatabaseState() {
    const client = new Client({ connectionString: this.connectionString });
    try {
      await client.connect();
      console.log('[PostgresListener]: Performing database table scan to resynchronize state...');

      // Get current WAL LSN to use as transaction event sequence ID
      const lsnRes = await client.query('SELECT pg_current_wal_lsn() AS current_lsn');
      const currentLsn = lsnRes.rows[0].current_lsn || '0/0';
      const [high, low] = currentLsn.split('/');
      const lsnBigInt = BigInt(`0x${high}`) * 0x100000000n + BigInt(`0x${low}`);

      const res = await client.query('SELECT id, customer_name, product_name, status, updated_at FROM orders');

      let sequence = 0n;
      for (const row of res.rows) {
        const event_id = (lsnBigInt * 10000n + sequence).toString();
        sequence++;
        const payload = JSON.stringify({
          event_id,
          action: 'SYNC',
          timestamp: new Date().toISOString(),
          data: row
        });

        if (this.onNotification) {
          await this.onNotification(payload);
        }
      }
      console.log(`[PostgresListener]: State resynchronization complete. Emitted ${res.rowCount} sync events starting at event_id ${lsnBigInt * 10000n}.`);
    } catch (err) {
      console.error('[PostgresListener]: Error synchronizing database state:', err.message);
      throw err;
    } finally {
      await client.end().catch(() => {});
    }
  }

  // Verifies the logical replication slot status and publications, recreates lost slots, and triggers state sync
  async verifyReplicationSlot() {
    const client = new Client({ connectionString: this.connectionString });
    try {
      await client.connect();

      // 1. Verify publication
      const pubRes = await client.query('SELECT 1 FROM pg_publication WHERE pubname = $1', [this.publication]);
      if (pubRes.rowCount === 0) {
        console.log(`[PostgresListener]: Publication "${this.publication}" does not exist. Creating...`);
        await client.query(`CREATE PUBLICATION ${this.publication} FOR TABLE orders`);
      }

      // 2. Verify replication slot
      const slotRes = await client.query(
        'SELECT slot_name, wal_status FROM pg_replication_slots WHERE slot_name = $1',
        [this.slot]
      );

      let shouldCreate = false;
      let wasLost = false;
      if (slotRes.rowCount > 0) {
        const slot = slotRes.rows[0];
        if (slot.wal_status === 'lost') {
          console.warn(`[PostgresListener]: Replication slot "${this.slot}" is in 'lost' state (invalidated). Dropping and recreating...`);
          await client.query('SELECT pg_drop_replication_slot($1)', [this.slot]);
          shouldCreate = true;
          wasLost = true;
        }
      } else {
        shouldCreate = true;
      }

      if (shouldCreate) {
        if (wasLost) {
          await this.syncCurrentDatabaseState();
        }
        console.log(`[PostgresListener]: Creating logical replication slot "${this.slot}"...`);
        await client.query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", [this.slot]);
      }
    } catch (err) {
      console.error('[PostgresListener]: Error verifying replication slot/publication:', err.message);
      throw err;
    } finally {
      await client.end().catch(() => {});
    }
  }

  // Establishes PG connection and starts logical replication streaming
  async connect() {
    if (this.isShuttingDown) return;

    try {
      await this.verifyReplicationSlot();
    } catch (err) {
      console.error('[PostgresListener]: Initialization verification failed:', err.message);
      this.scheduleReconnect();
      return;
    }

    return new Promise((resolve, reject) => {
      this.service = new LogicalReplicationService(
        { connectionString: this.connectionString },
        {
          acknowledge: {
            auto: false,
            timeoutSeconds: 10
          },
          flowControl: {
            enabled: true
          }
        }
      );

      let resolved = false;

      this.service.on('error', (err) => {
        console.error('PostgreSQL logical replication error:', err.message);
        if (!resolved) {
          resolved = true;
          reject(err);
        } else {
          this.scheduleReconnect();
        }
      });

      this.service.on('start', () => {
        console.log(`Successfully listening on replication slot: ${this.slot}`);
        this.retryDelay = 1000; // Reset delay on success
        if (!resolved) {
          resolved = true;
          resolve();
        }
      });

      const plugin = new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [this.publication]
      });

      this.service.on('data', async (lsn, log) => {
        if (log.tag === 'insert' || log.tag === 'update' || log.tag === 'delete') {
          if (log.relation && log.relation.name === 'orders') {
            const [high, low] = lsn.split('/');
            const lsnBigInt = BigInt(`0x${high}`) * 0x100000000n + BigInt(`0x${low}`);

            if (lsn === this.lastLsn) {
              this.lsnSequence++;
            } else {
              this.lastLsn = lsn;
              this.lsnSequence = 0n;
            }

            const event_id = (lsnBigInt * 10000n + this.lsnSequence).toString();

            let action = log.tag.toUpperCase();
            let data = null;

            if (log.tag === 'delete') {
              data = log.key || log.old;
            } else {
              data = log.new;
            }

            if (!data) return;

            const payload = JSON.stringify({
              event_id,
              action,
              timestamp: new Date().toISOString(),
              data
            });

            try {
              if (this.onNotification) {
                await this.onNotification(payload);
              }
              // Batch ACK: schedule a debounced acknowledgement instead of one per event
              this._scheduleAck(lsn);
            } catch (err) {
              console.error(`[PostgresListener]: Failed to process event for LSN ${lsn}, skipping ack:`, err.message);
            }
          }
        }
      });

      console.log(`Subscribing to PostgreSQL logical replication slot "${this.slot}"...`);
      this.service.subscribe(plugin, this.slot).catch((err) => {
        console.error('Failed to start logical replication subscription:', err.message);
        if (!resolved) {
          resolved = true;
          reject(err);
        } else {
          this.scheduleReconnect();
        }
      });
    });
  }

  // Backs off exponentially and schedules a new subscription attempt.
  // Delay is consumed first, then multiplied for the next attempt.
  scheduleReconnect() {
    if (this.isShuttingDown) return;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    const currentDelay = this.retryDelay;
    // Multiply for the NEXT attempt before scheduling, but use currentDelay now
    this.retryDelay = Math.min(this.retryDelay * 2, this.MAX_RETRY_DELAY);

    console.log(`Reconnecting to PostgreSQL logical replication in ${currentDelay}ms...`);
    this.reconnectTimer = setTimeout(async () => {
      if (this.service) {
        try {
          await this.service.stop().catch(() => {});
        } catch (e) {}
        this.service = null;
      }

      this.connect().catch((err) => {
        console.error('Reconnection failed, scheduling next attempt:', err.message);
        this.scheduleReconnect();
      });
    }, currentDelay);
  }

  // Clears outstanding reconnect timers, pending ACK timers, and stops logical replication
  async close() {
    this.isShuttingDown = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this._ackTimer) {
      clearTimeout(this._ackTimer);
      this._ackTimer = null;
    }
    if (this.service) {
      await this.service.stop().catch(() => {});
    }
  }
}

module.exports = PostgresListener;
