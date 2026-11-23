import crypto from 'crypto';
import { WebSocket } from 'ws';

// Change these two URLs to your Render URLs after Step 7 cloud deployment!
const API_URL = process.env.JUDGE_API_URL || 'http://localhost:4000';
const WS_URL = process.env.JUDGE_WS_URL || 'ws://localhost:4000/ws';
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || 'whsec_xavier_faith_secret_2026';

const BOARD_ID = `judgment-board-${Date.now()}`;

function signPayload(rawString) {
  return crypto.createHmac('sha256', WEBHOOK_SECRET).update(rawString).digest('hex');
}

async function runJudgment() {
  console.log(`\n=== INITIATING FINAL JUDGMENT ON: ${API_URL} ===\n`);

  // TEST 1: Health & MongoDB Atlas Ledger
  const healthRes = await fetch(`${API_URL}/health`);
  const health = await healthRes.json();
  if (health.status !== 'alive' || health.dbState !== 'connected') {
    throw new Error(`TEST 1 FAILED: Server/DB unhealthy -> ${JSON.stringify(health)}`);
  }
  console.log('[PASS] Test 1: Sanctuary Server & MongoDB Atlas Ledger Connected');

  // TEST 2: Raw-Body HMAC Signature Rejection on Tampered Signature
  const webhookBody = JSON.stringify({
    webhookId: `wh_test_${Date.now()}`,
    boardId: BOARD_ID,
    amount: 500
  });

  const badRes = await fetch(`${API_URL}/api/webhooks/payment`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-miracle-signature': 'deadbeef_forged_signature_1234567890abcdef1234567890abcdef12345678'
    },
    body: webhookBody
  });
  if (badRes.status !== 401) {
    throw new Error(`TEST 2 FAILED: Expected 401 on forged signature, got ${badRes.status}`);
  }
  console.log('[PASS] Test 2: Forged Webhook Signature Rejected (HTTP 401)');

  // TEST 3: Valid Raw-Body HMAC + Idempotent Double-Fire Protection
  const validSig = signPayload(webhookBody);
  const firstCreditRes = await fetch(`${API_URL}/api/webhooks/payment`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-miracle-signature': validSig },
    body: webhookBody
  });
  const firstCredit = await firstCreditRes.json();

  // Fire exact same webhook a second time!
  const duplicateCreditRes = await fetch(`${API_URL}/api/webhooks/payment`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-miracle-signature': validSig },
    body: webhookBody
  });
  const duplicateCredit = await duplicateCreditRes.json();

  if (firstCredit.balance !== 500 || duplicateCredit.balance !== 500) {
    throw new Error(`TEST 3 FAILED: Idempotency violated! Balances: ${firstCredit.balance}, ${duplicateCredit.balance}`);
  }
  console.log('[PASS] Test 3: Raw-Body HMAC Verified & Duplicate Webhook Idempotency Enforced (Balance = 500)');

  // TEST 4: Split-Brain WebSocket CRDT Event Convergence
  await new Promise((resolve, reject) => {
    const peerA = new WebSocket(WS_URL);
    const peerB = new WebSocket(WS_URL);
    let peerAReady = false;
    let peerBReady = false;
    let peerBReceivedPatch = false;

    const timeout = setTimeout(() => {
      peerA.close();
      peerB.close();
      reject(new Error('TEST 4 FAILED: WebSocket CRDT sync timed out after 5s'));
    }, 5000);

    peerA.on('open', () => {
      peerA.send(JSON.stringify({ type: 'JOIN_BOARD', boardId: BOARD_ID, lastKnownHlc: '' }));
    });

    peerB.on('open', () => {
      peerB.send(JSON.stringify({ type: 'JOIN_BOARD', boardId: BOARD_ID, lastKnownHlc: '' }));
    });

    peerA.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'SYNC_STATE') {
        peerAReady = true;
        if (peerAReady && peerBReady) pushOfflineEvent();
      }
    });

    peerB.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'SYNC_STATE') {
        peerBReady = true;
        if (peerAReady && peerBReady) pushOfflineEvent();
      }
      if (msg.type === 'REMOTE_EVENTS' && msg.events.some((e) => e.eventId === 'crdt_split_test_1')) {
        peerBReceivedPatch = true;
        clearTimeout(timeout);
        peerA.close();
        peerB.close();
        resolve();
      }
    });

    function pushOfflineEvent() {
      peerA.send(JSON.stringify({
        type: 'PUSH_EVENTS',
        events: [{
          eventId: 'crdt_split_test_1',
          boardId: BOARD_ID,
          clientId: 'peer_A_offline',
          hlcTimestamp: `${Date.now().toString().padStart(15, '0')}:0001:peer_A`,
          encryptedPayload: 'U2FsdGVkX1+MockCiphertextBase64Payload=='
        }]
      }));
    }
  });
  console.log('[PASS] Test 4: Multi-Peer WebSocket CRDT Encrypted Patch Fan-Out Verified');

  console.log('\n====================================================================');
  console.log('ALL 4 TRIALS PASSED! XAVIER\'S FAITH IS STRONGER THAN ALL THE SAINTS!');
  console.log('====================================================================\n');
}

runJudgment().catch((err) => {
  console.error('\nJUDGMENT FAILED:', err.message);
  process.exit(1);
});
