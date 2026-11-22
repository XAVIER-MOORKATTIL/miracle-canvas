import { openDB } from 'idb';

const DB_NAME = 'miracle-canvas-crdt-db';
const DB_VERSION = 1;

async function getDb() {
  return openDB(DB_NAME, DB_VERSION, {
    upgrade(db) {
      if (!db.objectStoreNames.contains('events')) {
        db.createObjectStore('events', { keyPath: 'eventId' });
      }
      if (!db.objectStoreNames.contains('outbox')) {
        db.createObjectStore('outbox', { keyPath: 'eventId' });
      }
      if (!db.objectStoreNames.contains('meta')) {
        db.createObjectStore('meta', { keyPath: 'key' });
      }
    }
  });
}

export async function saveLocalEvents(events) {
  const db = await getDb();
  const tx = db.transaction('events', 'readwrite');
  for (const ev of events) {
    await tx.store.put(ev);
  }
  await tx.done;
}

export async function getLocalBoardEvents(boardId) {
  const db = await getDb();
  const all = await db.getAll('events');
  return all
    .filter((e) => e.boardId === boardId)
    .sort((a, b) => a.hlcTimestamp.localeCompare(b.hlcTimestamp));
}

export async function enqueueOutboxEvent(event) {
  const db = await getDb();
  await db.put('outbox', event);
  await db.put('events', event);
}

export async function getOutboxEvents(boardId) {
  const db = await getDb();
  const all = await db.getAll('outbox');
  return all
    .filter((e) => e.boardId === boardId)
    .sort((a, b) => a.hlcTimestamp.localeCompare(b.hlcTimestamp));
}

export async function removeOutboxEvents(eventIds) {
  const db = await getDb();
  const tx = db.transaction('outbox', 'readwrite');
  for (const id of eventIds) {
    await tx.store.delete(id);
  }
  await tx.done;
}

export async function getLastHlc(boardId) {
  const db = await getDb();
  const record = await db.get('meta', `hlc:${boardId}`);
  return record ? record.value : '';
}

export async function setLastHlc(boardId, hlc) {
  const db = await getDb();
  await db.put('meta', { key: `hlc:${boardId}`, value: hlc });
}
