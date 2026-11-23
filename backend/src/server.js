import dns from 'dns';
dns.setServers(['8.8.8.8', '1.1.1.1']);
import 'dotenv/config';
import express from 'express';
import http from 'http';
import crypto from 'crypto';
import cors from 'cors';
import helmet from 'helmet';
import mongoose from 'mongoose';
import { WebSocketServer, WebSocket } from 'ws';
import { EventLog, Wallet } from './models.js';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// 1. Security & CORS (Allows frontend on any port/domain during testing & deployment)
app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(cors({ origin: '*' }));

// 2. Capture RAW body buffer specifically for HMAC signature verification
app.use(express.json({
  verify: (req, _res, buf) => {
    req.rawBody = buf;
  }
}));

// 3. Connect to MongoDB Atlas
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log('Connected to MongoDB Atlas Immutable Ledger'))
  .catch((err) => console.error('MongoDB Connection Error:', err.message));

// Health check endpoint (Used by cloud hosts & automated deployment judges)
app.get('/health', (_req, res) => {
  res.json({
    status: 'alive',
    dbState: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    timestamp: new Date().toISOString()
  });
});

// Fetch initial board state (Wallet balance + full encrypted event history)
app.get('/api/boards/:boardId/sync', async (req, res) => {
  try {
    const { boardId } = req.params;
    const afterHlc = req.query.afterHlc || '';

    const query = { boardId };
    if (afterHlc) {
      query.hlcTimestamp = { $gt: afterHlc };
    }

    const events = await EventLog.find(query).sort({ hlcTimestamp: 1 }).lean();
    const wallet = await Wallet.findOne({ boardId }).lean();

    res.json({
      events,
      balance: wallet ? wallet.balance : 0
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 4. Raw-Body HMAC-SHA256 Payment Webhook (Idempotent Event-Sourced Credit)
app.post('/api/webhooks/payment', async (req, res) => {
  try {
    const signature = req.headers['x-miracle-signature'];
    const secret = process.env.WEBHOOK_SECRET || 'whsec_xavier_faith_secret_2026';

    if (!signature || !req.rawBody) {
      return res.status(401).json({ error: 'Missing cryptographic signature or raw payload' });
    }

    // Compute HMAC-SHA256 over the exact raw bytes
    const expectedSig = crypto
      .createHmac('sha256', secret)
      .update(req.rawBody)
      .digest('hex');

    const sigBuffer = Buffer.from(signature, 'utf8');
    const expectedBuffer = Buffer.from(expectedSig, 'utf8');

    if (sigBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(sigBuffer, expectedBuffer)) {
      return res.status(401).json({ error: 'Invalid HMAC-SHA256 signature' });
    }

    const { webhookId, boardId, amount } = req.body;
    if (!webhookId || !boardId || typeof amount !== 'number') {
      return res.status(400).json({ error: 'Invalid webhook payload' });
    }

    // Idempotency Check: Atomically add webhookId only if not already processed
    const updatedWallet = await Wallet.findOneAndUpdate(
      { boardId, processedWebhooks: { $ne: webhookId } },
      {
        $inc: { balance: amount },
        $push: { processedWebhooks: webhookId }
      },
      { upsert: true, returnDocument: 'after' }
    ).catch(async (err) => {
      // Duplicate key error on upsert means webhookId was already processed concurrently
      if (err.code === 11000) return null;
      throw err;
    });

    if (!updatedWallet) {
      const existing = await Wallet.findOne({ boardId });
      return res.json({ status: 'already_processed', balance: existing?.balance || 0 });
    }

    // Append immutable TOKEN_CREDIT event to the ledger
    const hlcTimestamp = `${Date.now().toString().padStart(15, '0')}:0000:server`;
    const eventDoc = await EventLog.create({
      eventId: `wh_${webhookId}`,
      boardId,
      clientId: 'payment-gateway',
      hlcTimestamp,
      eventType: 'TOKEN_CREDIT',
      amount
    });

    // Broadcast updated balance to all live WebSocket clients on this board
    broadcastToBoard(boardId, {
      type: 'WALLET_UPDATED',
      balance: updatedWallet.balance,
      event: eventDoc
    });

    res.json({ status: 'credited', balance: updatedWallet.balance });
  } catch (err) {
    console.error('Webhook Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// 5. Stateful WebSocket Gateway with Heartbeat & Board Room Fan-Out
const boardRooms = new Map(); // boardId -> Set<WebSocket>

function broadcastToBoard(boardId, messageObj, excludeWs = null) {
  const clients = boardRooms.get(boardId);
  if (!clients) return;
  const data = JSON.stringify(messageObj);
  for (const client of clients) {
    if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.boardId = null;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // Client joins a specific encrypted board room
      if (msg.type === 'JOIN_BOARD') {
        ws.boardId = msg.boardId;
        if (!boardRooms.has(msg.boardId)) {
          boardRooms.set(msg.boardId, new Set());
        }
        boardRooms.get(msg.boardId).add(ws);

        // Send all missed events since client's last known Hybrid Logical Clock
        const query = { boardId: msg.boardId };
        if (msg.lastKnownHlc) {
          query.hlcTimestamp = { $gt: msg.lastKnownHlc };
        }
        const missedEvents = await EventLog.find(query).sort({ hlcTimestamp: 1 }).lean();
        const wallet = await Wallet.findOne({ boardId: msg.boardId }).lean();

        ws.send(JSON.stringify({
          type: 'SYNC_STATE',
          events: missedEvents,
          balance: wallet ? wallet.balance : 0
        }));
      }

      // Client pushes encrypted CRDT patches (works for live edits AND offline batch flushes)
      if (msg.type === 'PUSH_EVENTS' && Array.isArray(msg.events)) {
        const savedEvents = [];
        for (const ev of msg.events) {
          try {
            const created = await EventLog.create({
              eventId: ev.eventId,
              boardId: ev.boardId,
              clientId: ev.clientId,
              hlcTimestamp: ev.hlcTimestamp,
              eventType: 'ENCRYPTED_PATCH',
              encryptedPayload: ev.encryptedPayload
            });
            savedEvents.push(created);
          } catch (err) {
            // Ignore duplicate eventId (idempotent retry from offline queue)
            if (err.code !== 11000) console.error('Event insert error:', err.message);
          }
        }

        if (savedEvents.length > 0 && ws.boardId) {
          // Acknowledge to sender so it can clear its offline outbox
          ws.send(JSON.stringify({
            type: 'ACK_EVENTS',
            eventIds: savedEvents.map((e) => e.eventId)
          }));

          // Fan out encrypted patches to all other tabs/devices on this board
          broadcastToBoard(ws.boardId, {
            type: 'REMOTE_EVENTS',
            events: savedEvents
          }, ws);
        }
      }
    } catch (err) {
      console.error('WS Message Error:', err.message);
    }
  });

  ws.on('close', () => {
    if (ws.boardId && boardRooms.has(ws.boardId)) {
      boardRooms.get(ws.boardId).delete(ws);
    }
  });
});

// Ping all clients every 25s to keep connections alive through cloud proxies
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('close', () => clearInterval(heartbeat));

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Sanctuary Server running on http://localhost:${PORT}`);
});