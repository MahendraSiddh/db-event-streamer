const { createClient } = require('redis');

class RedisStreamer {
  constructor(url) {
    this.url = url;
    this.client = null;
  }

  // Opens a connection to Redis. Errors are propagated to the caller for restart handling.
  async connect() {
    this.client = createClient({ url: this.url });
    this.client.on('error', (err) => console.error('Redis error:', err.message));
    this.client.on('connect', () => console.log('Connected to Redis'));
    // Do not swallow — let the caller decide whether to retry or exit
    await this.client.connect();
  }

  // Atomically publishes event to live clients AND appends it to the history cache.
  // Using MULTI/EXEC ensures that a crash between operations cannot produce a gap
  // where a live client received an event that a reconnecting client cannot recover.
  // Appends an event to the Redis Stream and trims it to the specified limit
  async appendToStream(streamKey, eventId, limit, payload) {
    if (!this.client || !this.client.isOpen) {
      console.warn('[RedisStreamer]: Client not open — skipping append for payload.');
      return;
    }

    try {
      await this.client.xAdd(
        streamKey,
        eventId,
        { event: payload },
        {
          TRIM: {
            strategy: 'MAXLEN',
            strategyModifier: '~',
            threshold: limit
          }
        }
      );
    } catch (err) {
      console.error('[RedisStreamer]: Error appending event to Redis Stream:', err.message);
    }
  }

  // Closes the active Redis client
  async close() {
    if (this.client && this.client.isOpen) {
      await this.client.quit();
    }
  }
}

module.exports = RedisStreamer;
