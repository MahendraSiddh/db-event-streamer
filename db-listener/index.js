require('dotenv').config();
const PostgresListener = require('./postgres');
const RedisStreamer = require('./redis');

const DB_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;
const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY || 'orders:stream';
const STREAM_LIMIT = 100;

const redisStreamer = new RedisStreamer(REDIS_URL);
const pgListener = new PostgresListener(DB_URL, async (payload) => {
  let eventId = '*';
  try {
    const parsed = JSON.parse(payload);
    if (parsed.event_id) {
      eventId = `${parsed.event_id}-0`;
    }
  } catch (err) {
    console.error('[db-listener]: Failed to parse payload event_id:', err.message);
  }

  await redisStreamer.appendToStream(
    REDIS_STREAM_KEY,
    eventId,
    STREAM_LIMIT,
    payload
  );
});

let isShuttingDown = false;

async function start() {
  await redisStreamer.connect();
  await pgListener.connect();
}

async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log('Shutting down db-listener...');

  try {
    await pgListener.close();
  } catch (err) {
    console.error('Error closing PostgreSQL client:', err.message);
  }

  try {
    await redisStreamer.close();
  } catch (err) {
    console.error('Error closing Redis client:', err.message);
  }

  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

start().catch((err) => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
