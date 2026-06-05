const { createClient } = require('redis');
const websocket = require('./websocket');

const REDIS_URL = process.env.REDIS_URL;
const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY || 'orders:stream';

class RedisConsumer {
  constructor() {
    this.redisSubClient = null;
    this.isListening = false;
  }

  // Connects to Redis and streams events from the database stream.
  // Errors during connection setup propagate to the bootstrap caller.
  async init() {
    this.redisSubClient = createClient({ url: REDIS_URL });

    this.redisSubClient.on('error', (err) => {
      console.error('[RedisConsumer]: Client error:', err.message);
    });

    await this.redisSubClient.connect();
    console.log('[RedisConsumer]: Connected to Redis for Stream consumption');

    this.isListening = true;
    
    // Background polling loop for Redis Stream events
    // We use '$' to read only newly appended messages
    let lastId = '$';

    const readStream = async () => {
      while (this.isListening) {
        if (!this.redisSubClient || !this.redisSubClient.isOpen) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }

        try {
          // Blocking read on the stream
          const response = await this.redisSubClient.xRead(
            [{ key: REDIS_STREAM_KEY, id: lastId }],
            { BLOCK: 0, COUNT: 10 }
          );

          if (response && response.length > 0) {
            const messages = response[0].messages;
            for (const msg of messages) {
              lastId = msg.id;
              try {
                const event = JSON.parse(msg.message.event);
                const wsPayload = {
                  type: 'order_change',
                  ...event,
                  replayed: false
                };
                websocket.broadcast(wsPayload);
              } catch (err) {
                console.error('[RedisConsumer]: Failed to parse Redis Stream event:', err.message);
              }
            }
          }
        } catch (err) {
          // Avoid logging error if we intentionally shut down
          if (this.isListening) {
            console.error('[RedisConsumer]: Stream read loop error:', err.message);
            // Delay before retry to avoid high CPU consumption on consecutive failures
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
        }
      }
    };

    // Run the stream reader in the background (non-blocking to main execution thread)
    readStream().catch((err) => {
      console.error('[RedisConsumer]: Stream loop crashed:', err.stack || err.message);
    });
  }

  // Closes the active Redis subscription client
  async shutdown() {
    this.isListening = false;
    if (this.redisSubClient && this.redisSubClient.isOpen) {
      // Disconnect immediately to unblock XREAD connection
      await this.redisSubClient.disconnect().catch(() => {});
      console.log('[RedisConsumer]: Disconnected stream client');
    }
  }
}

module.exports = new RedisConsumer();
