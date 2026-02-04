import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { Kafka } from 'kafkajs';
import { Event } from './types';
import { initProducer, sendEvent, sendEventsBatch, disconnectProducer } from './producer';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

// TODO: Kafka admin for backpressure lag checking
const kafka = new Kafka({
  clientId: 'event-api-admin',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

// TODO: Implement backpressure check
// - If Kafka lag for group 'event-processors' > threshold, return 503
// - Include Retry-After header
async function checkKafkaLag(): Promise<{ overThreshold: boolean; lag: number }> {
  // TODO: Implement
  return { overThreshold: false, lag: 0 };
}

// TODO: Implement IP extraction from x-forwarded-for header
function getClientIp(req: Request): string {
  // TODO: Handle multi-value x-forwarded-for
  return req.ip || '127.0.0.1';
}

/**
 * POST /events
 *
 * TODO:
 * - Input validation (userId, type, payload, payload.seq)
 * - Backpressure check (503 if lag > threshold)
 * - Rate limiting (100/sec per userId, 500/sec per IP)
 * - Idempotency-Key header support
 * - Idempotency key conflict detection (409 if same key, different payload)
 */
app.post('/events', async (req: Request, res: Response) => {
  // TODO: Implement
  throw new Error('Not implemented');
});

/**
 * POST /events/batch
 *
 * TODO:
 * - Input validation per event
 * - Backpressure check
 * - Rate limiting (partial batch results)
 * - idempotencyKey per event support
 * - Return per-item results with status
 */
app.post('/events/batch', async (req: Request, res: Response) => {
  // TODO: Implement
  throw new Error('Not implemented');
});

/**
 * GET /metrics - diagnostic only, NOT source of truth
 */
app.get('/metrics', async (_req: Request, res: Response) => {
  // TODO: Implement
  res.json({ processed: 0, failed: 0, rateLimited: 0, duplicate: 0 });
});

/**
 * POST /metrics/reset
 */
app.post('/metrics/reset', async (_req: Request, res: Response) => {
  // TODO: Implement
  res.json({ status: 'ok' });
});

/**
 * GET /health
 */
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

/**
 * GET /lag - for debugging
 */
app.get('/lag', async (_req: Request, res: Response) => {
  const lagCheck = await checkKafkaLag();
  res.json(lagCheck);
});

async function start(): Promise<void> {
  await initProducer();

  app.listen(PORT, () => {
    console.log(`API server listening on port ${PORT}`);
  });
}

async function shutdown(): Promise<void> {
  console.log('Shutting down API...');
  await disconnectProducer();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start().catch((error) => {
  console.error('Failed to start API:', error);
  process.exit(1);
});
