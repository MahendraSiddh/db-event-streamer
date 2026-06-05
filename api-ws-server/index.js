require('dotenv').config();
const express = require('express');
const path = require('path');
const ordersRouter = require('./src/routes/orders');
const errorHandler = require('./src/middleware/errorHandler');
const db = require('./src/db/pool');
const websocketManager = require('./src/ws/websocket');
const redisConsumer = require('./src/ws/redisConsumer');
const ordersService = require('./src/services/ordersService');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// API Routes
app.use('/api/orders', ordersRouter);

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', timestamp: new Date() });
});

// Centralized error handling
app.use(errorHandler);

const server = app.listen(PORT, async () => {
  console.log(`HTTP & WebSocket Server running on port ${PORT}`);
  
  // Bootstrap services in sequence
  try {
    websocketManager.init(server);
    await ordersService.initRedis();
    await redisConsumer.init();
    console.log('System fully bootstrapped successfully');
  } catch (err) {
    console.error('Bootstrapping failed:', err.stack || err.message);
    process.exit(1);
  }
});

// Graceful Shutdown Handler
let isShuttingDown = false;
async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  console.log('Received shutdown signal. Closing server resources...');
  
  // 1. Close active WebSocket connections
  websocketManager.shutdown();

  // 2. Close Redis Consumer client
  await redisConsumer.shutdown().catch((err) => console.error('Error shutting down consumer:', err.message));

  // 3. Close Service Redis connection
  await ordersService.shutdownRedis().catch((err) => console.error('Error shutting down service Redis:', err.message));

  // 4. Close HTTP Server
  server.close(() => {
    console.log('HTTP server closed');
  });

  // 5. Drain PostgreSQL Pool
  try {
    await db.pool.end();
    console.log('Database connection pool drained');
  } catch (err) {
    console.error(`Error draining PG pool: ${err.message}`);
  }

  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

module.exports = server;
