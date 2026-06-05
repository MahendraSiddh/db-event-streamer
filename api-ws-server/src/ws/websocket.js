const WebSocket = require('ws');
const ordersService = require('../services/ordersService');

const HEARTBEAT_INTERVAL = 30000;

class WebSocketManager {
  constructor() {
    this.wss = null;
    this.activeClients = new Set();
    this.heartbeatTimer = null;
  }

  // Bind the WebSocket server to the HTTP server for upgrade handling
  init(httpServer) {
    this.wss = new WebSocket.Server({ noServer: true });

    httpServer.on('upgrade', (request, socket, head) => {
      this.wss.handleUpgrade(request, socket, head, (ws) => {
        this.wss.emit('connection', ws, request);
      });
    });

    // Ping clients periodically to detect and clean up dead sockets
    this.heartbeatTimer = setInterval(() => {
      for (const ws of this.activeClients) {
        if (ws.isAlive === false) {
          this.activeClients.delete(ws);
          ws.terminate();
        } else {
          ws.isAlive = false;
          ws.ping();
        }
      }
    }, HEARTBEAT_INTERVAL);

    this.wss.on('connection', (ws) => {
      ws.isAlive = true;
      this.activeClients.add(ws);

      ws.on('pong', () => {
        ws.isAlive = true;
      });

      ws.send(JSON.stringify({
        type: 'system',
        message: 'Connected to real-time order streams',
        timestamp: new Date().toISOString(),
      }));

      // Listen for handshake events to replay missed messages
      ws.on('message', async (messageData) => {
        try {
          const message = JSON.parse(messageData);
          
          if (message.type === 'handshake') {
            const lastEventIdStr = message.last_event_id;
            let isValid = false;
            try {
              if (lastEventIdStr !== undefined && lastEventIdStr !== null) {
                BigInt(lastEventIdStr);
                isValid = true;
              }
            } catch {}

            if (isValid) {
              const missedEvents = await ordersService.getMissedEvents(lastEventIdStr);
              for (const event of missedEvents) {
                this.sendTo(ws, {
                  type: 'order_change',
                  ...event,
                  replayed: true
                });
              }
            }
          }
        } catch (err) {
          console.error('Failed to parse incoming WS message:', err.message);
        }
      });

      ws.on('close', () => {
        this.activeClients.delete(ws);
      });

      ws.on('error', (err) => {
        console.error('WebSocket client error:', err.message);
        this.activeClients.delete(ws);
      });
    });
  }

  // Broadcasts event payload to all active connections
  broadcast(payload) {
    const wsMessage = JSON.stringify(payload);
    let sentCount = 0;
    for (const client of this.activeClients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(wsMessage);
        sentCount++;
      }
    }
    return sentCount;
  }

  // Safely sends a payload to a specific socket if open
  sendTo(ws, payload) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
    }
  }

  // Terminate connections and clear background timer
  shutdown() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    
    for (const client of this.activeClients) {
      client.close(1001, 'Server shutting down');
    }
    
    if (this.wss) {
      this.wss.close();
    }
  }
}

module.exports = new WebSocketManager();
