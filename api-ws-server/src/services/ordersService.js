const ordersRepository = require('../repositories/ordersRepository');
const redisClient = require('../db/redis');

const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY || 'orders:stream';

class OrdersService {
  async getAllOrders() {
    return await ordersRepository.findAll();
  }

  // Retrieve missed events for reconnecting clients using Redis Stream XRANGE
  async getMissedEvents(lastEventId) {
    if (!redisClient.isOpen) {
      console.warn('[OrdersService]: Redis client not open. Cannot retrieve missed events.');
      return [];
    }

    try {
      let startId = '-';
      try {
        if (lastEventId && BigInt(lastEventId) > 0n) {
          // '(' prefix signifies strictly greater than lastEventId-0
          startId = `(${lastEventId}-0`;
        }
      } catch (err) {
        startId = '-';
      }

      // Fetch missed entries from Redis Stream. '+' signifies end of stream.
      const entries = await redisClient.xRange(REDIS_STREAM_KEY, startId, '+');

      if (!entries || entries.length === 0) {
        return [];
      }

      const events = [];
      for (const entry of entries) {
        try {
          const eventObj = JSON.parse(entry.message.event);
          events.push(eventObj);
        } catch (err) {
          console.error('[OrdersService]: Failed to parse stream event JSON:', err.message);
        }
      }

      // Redis Stream XRANGE naturally returns entries in sequential, chronological order.
      return events;
    } catch (err) {
      console.error('[OrdersService]: Error getting missed events:', err.message);
      return [];
    }
  }

  async initRedis() {
    if (!redisClient.isOpen) {
      await redisClient.connect();
      console.log('[OrdersService]: Connected to Redis for history stream queries');
    }
  }

  async shutdownRedis() {
    if (redisClient.isOpen) {
      await redisClient.quit().catch(() => {});
      console.log('[OrdersService]: Disconnected from Redis');
    }
  }
}

module.exports = new OrdersService();
