import mongoose from 'mongoose';

// 1. Immutable Event Log (Every CRDT operation or token transfer is appended, never overwritten)
const eventSchema = new mongoose.Schema({
  eventId: { type: String, required: true, unique: true },
  boardId: { type: String, required: true, index: true },
  clientId: { type: String, required: true },
  hlcTimestamp: { type: String, required: true, index: true }, // Hybrid Logical Clock for CRDT ordering
  eventType: { type: String, enum: ['ENCRYPTED_PATCH', 'TOKEN_CREDIT'], required: true },
  encryptedPayload: { type: String }, // Ciphertext + IV (Server never sees plaintext)
  amount: { type: Number, default: 0 },
  createdAt: { type: Date, default: Date.now }
});

// 2. Materialized Read View for Token Balances (Updated transactionally)
const walletSchema = new mongoose.Schema({
  boardId: { type: String, required: true, unique: true },
  balance: { type: Number, default: 0 },
  processedWebhooks: [{ type: String }] // Prevents double-crediting on duplicate webhooks
});

export const EventLog = mongoose.model('EventLog', eventSchema);
export const Wallet = mongoose.model('Wallet', walletSchema);