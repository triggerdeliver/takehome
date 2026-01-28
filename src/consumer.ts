import { Kafka, Consumer } from 'kafkajs';
import { Event, ProcessingResult } from './types';
import {
  tryAcquireEvent,
  saveProcessingResult,
  disconnect as disconnectRedis,
} from './redis-client';

const kafka = new Kafka({
  clientId: 'event-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

let consumer: Consumer;
let isShuttingDown = false;
const inFlightTasks = new Set<Promise<unknown>>();

const CONCURRENCY = parseInt(process.env.CONSUMER_CONCURRENCY || '384', 10);
const PARTITIONS_CONCURRENCY = parseInt(process.env.CONSUMER_PARTITIONS || '8', 10);
const MAX_PROCESSING_RETRIES = parseInt(process.env.PROCESSING_RETRIES || '2', 10);
const INFLIGHT_RETRY_ATTEMPTS = parseInt(process.env.INFLIGHT_RETRY_ATTEMPTS || '6', 10);
const INFLIGHT_RETRY_DELAY_MS = parseInt(process.env.INFLIGHT_RETRY_DELAY_MS || '30', 10);
const PROCESSING_DELAY_MS = parseInt(process.env.PROCESSING_DELAY_MS || '0', 10);

export async function initConsumer(): Promise<void> {
  consumer = kafka.consumer({
    groupId: 'event-processors',
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'events', fromBeginning: false });

  console.log('Consumer connected and subscribed');
}

async function processEvent(_event: Event): Promise<void> {
  if (PROCESSING_DELAY_MS > 0) {
    await new Promise((resolve) => setTimeout(resolve, Math.random() * PROCESSING_DELAY_MS));
  }
}

async function delay(ms: number): Promise<void> {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForAcquire(eventId: string): Promise<'acquired' | 'processed' | 'inflight'> {
  let status = await tryAcquireEvent(eventId);
  if (status !== 'inflight') {
    return status;
  }

  for (let i = 0; i < INFLIGHT_RETRY_ATTEMPTS; i += 1) {
    await delay(INFLIGHT_RETRY_DELAY_MS);
    status = await tryAcquireEvent(eventId);
    if (status !== 'inflight') {
      return status;
    }
  }

  return 'inflight';
}

async function handleMessage(
  message: { value: Buffer | null; offset: string },
  resolveOffset: (offset: string) => void
): Promise<boolean> {
  if (isShuttingDown) return false;
  if (!message.value) {
    resolveOffset(message.offset);
    return true;
  }

  let event: Event;
  try {
    event = JSON.parse(message.value.toString());
  } catch (error) {
    console.error('Failed to parse message', error);
    resolveOffset(message.offset);
    return true;
  }

  const acquireStatus = await waitForAcquire(event.id);
  if (acquireStatus === 'processed') {
    resolveOffset(message.offset);
    return true;
  }

  if (acquireStatus === 'inflight') {
    return false;
  }

  for (let attempt = 0; attempt <= MAX_PROCESSING_RETRIES; attempt += 1) {
    try {
      await processEvent(event);
      const result: ProcessingResult = {
        eventId: event.id,
        status: 'processed',
        processedAt: Date.now(),
      };
      await saveProcessingResult(result);
      resolveOffset(message.offset);
      return true;
    } catch (error) {
      const lastError = error instanceof Error ? error.message : 'Unknown error';
      if (attempt < MAX_PROCESSING_RETRIES) {
        await delay(5 * (attempt + 1));
        continue;
      }
      const failedResult: ProcessingResult = {
        eventId: event.id,
        status: 'failed',
        processedAt: Date.now(),
        error: lastError,
      };
      await saveProcessingResult(failedResult);
      resolveOffset(message.offset);
      return true;
    }
  }

  return false;
}

export async function startConsuming(): Promise<void> {
  await consumer.run({
    autoCommit: true,
    autoCommitInterval: 500,
    autoCommitThreshold: 500,
    partitionsConsumedConcurrently: PARTITIONS_CONCURRENCY,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
      commitOffsetsIfNecessary,
    }) => {
      const localInFlight = new Set<Promise<boolean>>();
      let scheduled = 0;
      let allResolved = true;

      for (const message of batch.messages) {
        if (!isRunning() || isStale() || isShuttingDown) {
          allResolved = false;
          break;
        }

        const task = handleMessage(message, resolveOffset).catch((error) => {
          console.error('Error handling message:', error);
          return false;
        });

        inFlightTasks.add(task);
        localInFlight.add(task);
        task.finally(() => {
          inFlightTasks.delete(task);
          localInFlight.delete(task);
        });

        scheduled += 1;
        if (localInFlight.size >= CONCURRENCY) {
          const result = await Promise.race(localInFlight);
          if (!result) {
            allResolved = false;
          }
        }
        if (scheduled % 200 === 0) {
          await heartbeat();
        }
      }

      const results = await Promise.allSettled(localInFlight);
      for (const result of results) {
        if (result.status === 'fulfilled' && result.value === false) {
          allResolved = false;
        }
      }

      if (allResolved) {
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset: batch.highWatermark,
          },
        ]);
      } else {
        await commitOffsetsIfNecessary();
      }

      await heartbeat();
    },
  });

  console.log('Consumer started');
}

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

  if (inFlightTasks.size > 0) {
    await Promise.race([
      Promise.allSettled(Array.from(inFlightTasks)),
      delay(2000),
    ]);
  }

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
