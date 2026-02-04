import { Kafka, Consumer } from 'kafkajs';
import { Event, ProcessingResult } from './types';
import {
  disconnect as disconnectRedis,
  redis,
} from './redis-client';

const kafka = new Kafka({
  clientId: 'event-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

let consumer: Consumer;
let isShuttingDown = false;

// TODO: Track in-flight tasks for graceful shutdown
// TODO: Per-user ordering buffers: userId -> Map<seq, Event>
// TODO: Per-user next expected seq tracking

export async function initConsumer(): Promise<void> {
  consumer = kafka.consumer({
    groupId: 'event-processors',
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'events', fromBeginning: false });

  console.log('Consumer connected and subscribed');
}

/**
 * Process a single event (business logic)
 *
 * TODO:
 * - If payload.fail = true, throw error to trigger retry
 */
async function processEvent(event: Event): Promise<void> {
  // TODO: Implement business logic
  // Check if payload.fail is set - simulate failure for DLQ test
}

/**
 * Process event with retry logic
 *
 * TODO:
 * - Retry up to 3 times on failure
 * - On max retries exceeded, send to DLQ
 * - Save result to Redis with proper status
 * - Add to order list if seq is present
 * - Failed events must not block subsequent seq
 */
async function processEventWithRetry(event: Event): Promise<void> {
  // TODO: Implement retry logic with DLQ
  throw new Error('Not implemented');
}

/**
 * Try to acquire event for processing (idempotency)
 *
 * TODO:
 * - Use Redis atomic acquire
 * - Handle 'acquired', 'processed', 'inflight' states
 * - Implement retry wait for inflight events
 */
async function waitForAcquire(eventId: string): Promise<'acquired' | 'processed' | 'inflight'> {
  // TODO: Implement atomic acquire with retry
  throw new Error('Not implemented');
}

/**
 * Handle ordering - get next expected seq from Redis
 *
 * TODO:
 * - Read current order list from Redis
 * - Calculate next expected seq
 */
async function getNextExpectedSeqFromRedis(userId: string): Promise<number> {
  // TODO: Implement
  return 1;
}

/**
 * Try to process buffered events in order
 *
 * TODO:
 * - Process events in sequence order
 * - Handle gap timeout (>2000ms skip missing seq)
 * - Handle late arrivals (seq < nextExpected -> status=skipped)
 */
async function tryProcessBufferedEvents(userId: string): Promise<void> {
  // TODO: Implement ordering buffer processing
}

/**
 * Handle a single Kafka message
 *
 * TODO:
 * - Parse message
 * - Check idempotency (already processed?)
 * - Handle ordering if seq is present
 * - Process or buffer based on seq order
 */
async function handleMessage(
  message: { value: Buffer | null; offset: string },
  resolveOffset: (offset: string) => void
): Promise<boolean> {
  // TODO: Implement message handling
  throw new Error('Not implemented');
}

export async function startConsuming(): Promise<void> {
  await consumer.run({
    autoCommit: true,
    autoCommitInterval: 500,
    autoCommitThreshold: 500,
    partitionsConsumedConcurrently: 8,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
      commitOffsetsIfNecessary,
    }) => {
      // TODO: Implement batch processing with concurrency control
      // - Process messages with configurable concurrency
      // - Handle shutdown gracefully
      // - Commit offsets properly
      throw new Error('Not implemented');
    },
  });

  console.log('Consumer started');
}

/**
 * Graceful shutdown
 *
 * TODO:
 * - Stop accepting new messages
 * - Wait for in-flight tasks
 * - Clean up inflight markers
 * - Disconnect from Kafka and Redis
 */
export async function shutdown(): Promise<void> {
  console.log('Shutting down consumer...');
  isShuttingDown = true;

  if (consumer) {
    try {
      await consumer.stop();
    } catch (error) {
      console.error('Error stopping consumer:', error);
    }
  }

  // TODO: Wait for inflight tasks
  // TODO: Clean up inflight markers for events we were processing

  if (consumer) {
    await consumer.disconnect();
  }
  await disconnectRedis();

  console.log('Consumer shutdown complete');
}

process.on('SIGTERM', async () => {
  await shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  await shutdown();
  process.exit(0);
});

async function start(): Promise<void> {
  await initConsumer();
  await startConsuming();
}

start().catch((error) => {
  console.error('Failed to start consumer:', error);
  process.exit(1);
});
