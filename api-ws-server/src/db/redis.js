const { createClient } = require('redis');

const REDIS_URL = process.env.REDIS_URL;
const redisClient = createClient({ url: REDIS_URL });

redisClient.on('error', (err) => {
  console.error('Redis DB Client Error:', err.message);
});

module.exports = redisClient;
