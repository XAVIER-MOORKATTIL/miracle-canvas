import { useState, useEffect, useRef, useCallback } from 'react';
import { deriveBoardKey, encryptPayload, decryptPayload } from './cryptoVault';
import {
  saveLocalEvents,
  getLocalBoardEvents,
  enqueueOutboxEvent,
  getOutboxEvents,
  removeOutboxEvents,
  getLastHlc,
  setLastHlc
} from './localStore';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:4000';
const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:4000/ws';

// Unique identifier per browser tab so two open tabs act as independent distributed peers
const CLIENT_ID = `peer_${Math.random().toString(36).slice(2, 9)}`;

// Hybrid Logical Clock (HLC) generator for deterministic CRDT total ordering
let lastPhysicalTime = 0;
let logicalCounter = 0;

function nextHlc() {
  const now = Date.now();
  if (now > lastPhysicalTime) {
    lastPhysicalTime = now;
    logicalCounter = 0;
  } else {
    logicalCounter += 1;
  }
  return `${lastPhysicalTime.toString().padStart(15, '0')}:${logicalCounter.toString().padStart(4, '0')}:${CLIENT_ID}`;
}

export default function App() {
  const [boardId, setBoardId] = useState('sanctuary-board-1');
  const [passphrase, setPassphrase] = useState('faith-moves-mountains');
  const [cryptoKey, setCryptoKey] = useState(null);

  const [cards, setCards] = useState({});
  const [balance, setBalance] = useState(0);
  const [newTitle, setNewTitle] = useState('');
  const [newNote, setNewNote] = useState('');

  const [isConnected, setIsConnected] = useState(false);
  const [simulatedOffline, setSimulatedOffline] = useState(false);
  const [pendingOutboxCount, setPendingOutboxCount] = useState(0);
  const [eventCount, setEventCount] = useState(0);

  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const cryptoKeyRef = useRef(null);

  // 1. Derive AES-GCM key whenever boardId or passphrase changes
  useEffect(() => {
    let active = true;
    deriveBoardKey(passphrase, boardId).then((key) => {
      if (active) {
        cryptoKeyRef.current = key;
        setCryptoKey(key);
      }
    });
    return () => {
      active = false;
    };
  }, [passphrase, boardId]);

  // 2. Materialize CRDT State from Ordered Immutable Event Log
  const rebuildStateFromEvents = useCallback(async (key) => {
    if (!key) return;
    const allEvents = await getLocalBoardEvents(boardId);
    setEventCount(allEvents.length);

    const outbox = await getOutboxEvents(boardId);
    setPendingOutboxCount(outbox.length);

    // LWW-Element-Set CRDT Fold over HLC-sorted events
    const materialized = {};
    let latestHlc = '';

    for (const ev of allEvents) {
      if (ev.hlcTimestamp > latestHlc) {
        latestHlc = ev.hlcTimestamp;
      }
      if (ev.eventType !== 'ENCRYPTED_PATCH' || !ev.encryptedPayload) continue;

      const op = await decryptPayload(key, ev.encryptedPayload);
      if (!op || !op.cardId) continue;

      const existing = materialized[op.cardId];
      // Deterministic CRDT tie-breaker: higher HLC timestamp always wins
      if (!existing || ev.hlcTimestamp > existing.hlc) {
        if (op.action === 'DELETE') {
          materialized[op.cardId] = { deleted: true, hlc: ev.hlcTimestamp };
        } else {
          materialized[op.cardId] = {
            id: op.cardId,
            title: op.title,
            note: op.note,
            color: op.color || '#1e293b',
            updatedBy: ev.clientId,
            hlc: ev.hlcTimestamp,
            deleted: false
          };
        }
      }
    }

    if (latestHlc) {
      await setLastHlc(boardId, latestHlc);
    }

    // Filter out tombstoned (deleted) items
    const visibleCards = {};
    for (const [id, item] of Object.entries(materialized)) {
      if (!item.deleted) {
        visibleCards[id] = item;
      }
    }
    setCards(visibleCards);
  }, [boardId]);

  useEffect(() => {
    if (cryptoKey) {
      rebuildStateFromEvents(cryptoKey);
    }
  }, [cryptoKey, rebuildStateFromEvents]);

  // 3. Flush Offline Outbox over WebSocket
  const flushOutbox = useCallback(async () => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    const outbox = await getOutboxEvents(boardId);
    setPendingOutboxCount(outbox.length);
    if (outbox.length > 0) {
      ws.send(JSON.stringify({ type: 'PUSH_EVENTS', events: outbox }));
    }
  }, [boardId]);

  // 4. Stateful WebSocket Connection with Exponential Backoff Auto-Reconnect
  useEffect(() => {
    if (simulatedOffline) {
      if (wsRef.current) wsRef.current.close();
      setIsConnected(false);
      return;
    }

    let retryDelay = 500;
    let unmounted = false;

    async function connect() {
      if (unmounted) return;
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = async () => {
        if (unmounted) return;
        setIsConnected(true);
        retryDelay = 500;

        const lastKnownHlc = await getLastHlc(boardId);
        ws.send(JSON.stringify({
          type: 'JOIN_BOARD',
          boardId,
          lastKnownHlc
        }));

        await flushOutbox();
      };

      ws.onmessage = async (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === 'SYNC_STATE') {
          if (typeof msg.balance === 'number') setBalance(msg.balance);
          if (Array.isArray(msg.events) && msg.events.length > 0) {
            await saveLocalEvents(msg.events);
            await rebuildStateFromEvents(cryptoKeyRef.current);
          }
        }

        if (msg.type === 'REMOTE_EVENTS' && Array.isArray(msg.events)) {
          await saveLocalEvents(msg.events);
          await rebuildStateFromEvents(cryptoKeyRef.current);
        }

        if (msg.type === 'ACK_EVENTS' && Array.isArray(msg.eventIds)) {
          await removeOutboxEvents(msg.eventIds);
          const remaining = await getOutboxEvents(boardId);
          setPendingOutboxCount(remaining.length);
        }

        if (msg.type === 'WALLET_UPDATED' && typeof msg.balance === 'number') {
          setBalance(msg.balance);
        }
      };

      ws.onclose = () => {
        if (unmounted) return;
        setIsConnected(false);
        reconnectTimerRef.current = setTimeout(() => {
          retryDelay = Math.min(retryDelay * 1.5, 5000);
          connect();
        }, retryDelay);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      unmounted = true;
      clearTimeout(reconnectTimerRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, [boardId, simulatedOffline, flushOutbox, rebuildStateFromEvents]);

  // 5. Create or Update an Encrypted CRDT Card (Works identically Online & Offline)
  async function dispatchCardOp(action, cardData) {
    if (!cryptoKey) return;
    const hlc = nextHlc();
    const eventId = `ev_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

    const encryptedPayload = await encryptPayload(cryptoKey, {
      action,
      ...cardData
    });

    const eventObj = {
      eventId,
      boardId,
      clientId: CLIENT_ID,
      hlcTimestamp: hlc,
      eventType: 'ENCRYPTED_PATCH',
      encryptedPayload
    };

    // Persist immediately to IndexedDB outbox + local event log
    await enqueueOutboxEvent(eventObj);
    await rebuildStateFromEvents(cryptoKey);

    // Attempt immediate push if online
    await flushOutbox();
  }

  async function handleCreateCard(e) {
    e.preventDefault();
    if (!newTitle.trim()) return;
    const cardId = `card_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
    const colors = ['#1e3a8a', '#065f46', '#7c2d12', '#581c87', '#1e293b'];
    const color = colors[Math.floor(Math.random() * colors.length)];

    await dispatchCardOp('UPSERT', {
      cardId,
      title: newTitle.trim(),
      note: newNote.trim() || 'Encrypted CRDT node',
      color
    });

    setNewTitle('');
    setNewNote('');
  }

  async function handleEditCard(card) {
    const updatedTitle = prompt('Edit card title (will sync via HLC CRDT):', card.title);
    if (updatedTitle === null || !updatedTitle.trim()) return;

    await dispatchCardOp('UPSERT', {
      cardId: card.id,
      title: updatedTitle.trim(),
      note: card.note,
      color: card.color
    });
  }

  async function handleDeleteCard(cardId) {
    await dispatchCardOp('DELETE', { cardId });
  }

  // Initial REST fallback sync on mount
  useEffect(() => {
    fetch(`${API_URL}/api/boards/${boardId}/sync`)
      .then((r) => r.json())
      .then(async (data) => {
        if (typeof data.balance === 'number') setBalance(data.balance);
        if (Array.isArray(data.events) && data.events.length > 0) {
          await saveLocalEvents(data.events);
          if (cryptoKeyRef.current) {
            await rebuildStateFromEvents(cryptoKeyRef.current);
          }
        }
      })
      .catch(() => {});
  }, [boardId, rebuildStateFromEvents]);

  const cardList = Object.values(cards).sort((a, b) => a.hlc.localeCompare(b.hlc));

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', background: '#0f172a', color: '#f8fafc', minHeight: '100vh', padding: '24px' }}>
      <div style={{ maxWidth: '960px', margin: '0 auto' }}>
        {/* Header & Telemetry Bar */}
        <header style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: '16px', borderBottom: '1px solid #334155', paddingBottom: '16px', marginBottom: '24px' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: '1.6rem', color: '#38bdf8' }}>Miracle Canvas — Zero-Knowledge CRDT Ledger</h1>
            <p style={{ margin: '4px 0 0', fontSize: '0.85rem', color: '#94a3b8' }}>
              Peer ID: <code>{CLIENT_ID}</code> | Events Stored: <strong>{eventCount}</strong> | Offline Outbox: <strong>{pendingOutboxCount}</strong>
            </p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div style={{ background: '#1e293b', padding: '8px 14px', borderRadius: '8px', border: '1px solid #334155' }}>
              <span style={{ fontSize: '0.8rem', color: '#94a3b8' }}>Escrow Balance: </span>
              <strong style={{ color: '#4ade80', fontSize: '1.1rem' }} data-testid="wallet-balance">{balance} Tokens</strong>
            </div>

            <button
              data-testid="toggle-offline-btn"
              onClick={() => setSimulatedOffline((prev) => !prev)}
              style={{
                padding: '10px 16px',
                borderRadius: '8px',
                border: 'none',
                fontWeight: 'bold',
                cursor: 'pointer',
                background: simulatedOffline ? '#ef4444' : isConnected ? '#22c55e' : '#eab308',
                color: '#0f172a'
              }}
            >
              {simulatedOffline ? 'OFFLINE (Click to Reconnect)' : isConnected ? 'ONLINE & SYNCED' : 'RECONNECTING...'}
            </button>
          </div>
        </header>

        {/* Zero-Knowledge Vault Controls */}
        <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '12px', background: '#1e293b', padding: '16px', borderRadius: '10px', marginBottom: '24px' }}>
          <div>
            <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '4px' }}>Workspace Board ID</label>
            <input
              data-testid="board-id-input"
              value={boardId}
              onChange={(e) => setBoardId(e.target.value)}
              style={{ width: '100%', padding: '8px', borderRadius: '6px', border: '1px solid #475569', background: '#0f172a', color: '#fff', boxSizing: 'border-box' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '4px' }}>E2E Encryption Passphrase (Never Sent to Server)</label>
            <input
              data-testid="passphrase-input"
              type="password"
              value={passphrase}
              onChange={(e) => setPassphrase(e.target.value)}
              style={{ width: '100%', padding: '8px', borderRadius: '6px', border: '1px solid #475569', background: '#0f172a', color: '#fff', boxSizing: 'border-box' }}
            />
          </div>
        </section>

        {/* Add Encrypted Card Form */}
        <form onSubmit={handleCreateCard} style={{ display: 'flex', flexWrap: 'wrap', gap: '10px', marginBottom: '24px' }}>
          <input
            data-testid="card-title-input"
            placeholder="Card title (encrypted locally before sync)..."
            value={newTitle}
            onChange={(e) => setNewTitle(e.target.value)}
            style={{ flex: '1 1 240px', padding: '10px 14px', borderRadius: '8px', border: '1px solid #334155', background: '#1e293b', color: '#fff' }}
          />
          <input
            data-testid="card-note-input"
            placeholder="Details or task notes..."
            value={newNote}
            onChange={(e) => setNewNote(e.target.value)}
            style={{ flex: '2 1 300px', padding: '10px 14px', borderRadius: '8px', border: '1px solid #334155', background: '#1e293b', color: '#fff' }}
          />
          <button
            data-testid="add-card-btn"
            type="submit"
            style={{ padding: '10px 20px', borderRadius: '8px', border: 'none', background: '#38bdf8', color: '#0f172a', fontWeight: 'bold', cursor: 'pointer' }}
          >
            + Add Encrypted Card
          </button>
        </form>

        {/* Collaborative CRDT Board */}
        <div data-testid="cards-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: '16px' }}>
          {cardList.length === 0 ? (
            <p style={{ color: '#64748b', gridColumn: '1 / -1', textAlign: 'center', padding: '40px 0' }}>
              No decrypted cards on this board yet (or wrong passphrase). Create one above!
            </p>
          ) : (
            cardList.map((card) => (
              <div
                key={card.id}
                data-testid={`card-${card.id}`}
                style={{ background: card.color, padding: '16px', borderRadius: '10px', border: '1px solid rgba(255,255,255,0.12)', display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}
              >
                <div>
                  <h3 style={{ margin: '0 0 8px', fontSize: '1.1rem' }}>{card.title}</h3>
                  <p style={{ margin: '0 0 12px', fontSize: '0.9rem', color: '#e2e8f0' }}>{card.note}</p>
                </div>
                <div>
                  <div style={{ fontSize: '0.7rem', color: '#cbd5e1', marginBottom: '10px' }}>
                    HLC: <code>{card.hlc.slice(0, 20)}...</code>
                  </div>
                  <div style={{ display: 'flex', gap: '8px' }}>
                    <button
                      onClick={() => handleEditCard(card)}
                      style={{ flex: 1, padding: '6px', borderRadius: '6px', border: 'none', background: 'rgba(255,255,255,0.2)', color: '#fff', cursor: 'pointer', fontSize: '0.8rem' }}
                    >
                      Edit
                    </button>
                    <button
                      onClick={() => handleDeleteCard(card.id)}
                      style={{ padding: '6px 10px', borderRadius: '6px', border: 'none', background: 'rgba(239,68,68,0.8)', color: '#fff', cursor: 'pointer', fontSize: '0.8rem' }}
                    >
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}