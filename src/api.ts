import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { Kafka } from 'kafkajs';
import { Event } from './types';
import { initProducer, sendEvent, sendEventsBatch, disconnectProducer } from './producer';
import { checkBothRateLimits, getMetrics, resetMetrics } from './redis-client';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const BACKPRESSURE_LAG_THRESHOLD = 5000;
const RETRY_AFTER_SECONDS = 5;

// Kafka admin for lag checking
const kafka = new Kafka({
  clientId: 'event-api-admin',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});
const admin = kafka.admin();
let adminConnected = false;

async function connectAdmin(): Promise<void> {
  if (!adminConnected) {
    await admin.connect();
    adminConnected = true;
  }
}

function getClientIp(req: Request): string {
  // Check x-forwarded-for header first (may contain comma-separated list)
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) {
    const ips = typeof forwarded === 'string' ? forwarded : forwarded[0];
    // Take the first IP in the list (original client)
    const firstIp = ips.split(',')[0].trim();
    if (firstIp) return firstIp;
  }
  // Fallback to remote address
  return req.ip || req.socket.remoteAddress || '127.0.0.1';
}

async function checkKafkaLag(): Promise<{ overThreshold: boolean; lag: number }> {
  try {
    await connectAdmin();

    const groupId = 'event-processors';
    const topic = 'events';

    // Get consumer group offsets
    const groupOffsets = await admin.fetchOffsets({ groupId, topics: [topic] });

    // Get topic offsets (latest)
    const topicOffsets = await admin.fetchTopicOffsets(topic);

    let totalLag = 0;
    for (const partition of topicOffsets) {
      const groupPartition = groupOffsets.find(
        (g) => g.topic === topic
      )?.partitions.find((p) => p.partition === partition.partition);

      const latestOffset = parseInt(partition.high, 10) || 0;
      const committedOffset = parseInt(groupPartition?.offset || '0', 10);

      if (latestOffset > committedOffset) {
        totalLag += latestOffset - committedOffset;
      }
    }

    return {
      overThreshold: totalLag > BACKPRESSURE_LAG_THRESHOLD,
      lag: totalLag,
    };
  } catch (error) {
    // If we can't check lag (e.g., consumer group doesn't exist yet), allow requests
    console.error('Error checking Kafka lag:', error);
    return { overThreshold: false, lag: 0 };
  }
}

/**
 * POST /events
 */
app.post('/events', async (req: Request, res: Response) => {
  try {
    // Check backpressure first
    const lagCheck = await checkKafkaLag();
    if (lagCheck.overThreshold) {
      res.set('Retry-After', RETRY_AFTER_SECONDS.toString());
      res.status(503).json({
        error: 'Service temporarily unavailable - backpressure',
        lag: lagCheck.lag,
        retryAfter: RETRY_AFTER_SECONDS,
      });
      return;
    }

    const { userId, type, payload } = req.body;

    if (!userId || !type) {
      res.status(400).json({ error: 'userId and type are required' });
      return;
    }

    const event: Event = {
      id: uuidv4(),
      userId,
      type,
      payload: payload || {},
      timestamp: Date.now(),
    };

    const ip = getClientIp(req);
    const rateLimitResult = await checkBothRateLimits(userId, ip, event.id);

    if (!rateLimitResult.allowed) {
      // Determine which limit was hit
      const reason = !rateLimitResult.userResult.allowed ? 'user' : 'ip';
      const resetAt = !rateLimitResult.userResult.allowed
        ? rateLimitResult.userResult.resetAt
        : rateLimitResult.ipResult.resetAt;

      res.status(429).json({
        error: 'Rate limit exceeded',
        reason,
        remaining: 0,
        resetAt,
      });
      return;
    }

    await sendEvent(event);

    res.status(202).json({
      eventId: event.id,
      status: 'accepted',
    });
  } catch (error) {
    console.error('Error handling event:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * POST /events/batch
 */
app.post('/events/batch', async (req: Request, res: Response) => {
  try {
    // Check backpressure first
    const lagCheck = await checkKafkaLag();
    if (lagCheck.overThreshold) {
      res.set('Retry-After', RETRY_AFTER_SECONDS.toString());
      res.status(503).json({
        error: 'Service temporarily unavailable - backpressure',
        lag: lagCheck.lag,
        retryAfter: RETRY_AFTER_SECONDS,
      });
      return;
    }

    const { events } = req.body;

    if (!Array.isArray(events) || events.length === 0) {
      res.status(400).json({ error: 'events array is required' });
      return;
    }

    if (events.length > 1000) {
      res.status(400).json({ error: 'Maximum 1000 events per batch' });
      return;
    }

    const ip = getClientIp(req);
    const results: Array<{ eventId: string; status: string }> = new Array(events.length);
    const acceptedEvents: Event[] = [];

    const CONCURRENCY = parseInt(process.env.RATE_CHECK_CONCURRENCY || '200', 10);

    for (let i = 0; i < events.length; i += CONCURRENCY) {
      const chunk = events.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (eventData, idx) => {
          const { userId, type, payload } = eventData;
          const resultIndex = i + idx;

          if (!userId || !type) {
            results[resultIndex] = { eventId: 'unknown', status: 'invalid' };
            return;
          }

          const event: Event = {
            id: uuidv4(),
            userId,
            type,
            payload: payload || {},
            timestamp: Date.now(),
          };

          const rateLimitResult = await checkBothRateLimits(userId, ip, event.id);
          if (!rateLimitResult.allowed) {
            results[resultIndex] = { eventId: event.id, status: 'rate_limited' };
            return;
          }

          acceptedEvents.push(event);
          results[resultIndex] = { eventId: event.id, status: 'accepted' };
        })
      );
    }

    if (acceptedEvents.length > 0) {
      await sendEventsBatch(acceptedEvents);
    }

    res.status(202).json({ results });
  } catch (error) {
    console.error('Error handling batch:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /metrics
 */
app.get('/metrics', async (_req: Request, res: Response) => {
  try {
    const metrics = await getMetrics();
    res.json(metrics);
  } catch (error) {
    console.error('Error getting metrics:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * POST /metrics/reset
 */
app.post('/metrics/reset', async (_req: Request, res: Response) => {
  try {
    await resetMetrics();
    res.json({ status: 'ok' });
  } catch (error) {
    console.error('Error resetting metrics:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
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
  try {
    const lagCheck = await checkKafkaLag();
    res.json(lagCheck);
  } catch (error) {
    console.error('Error checking lag:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

async function start(): Promise<void> {
  await initProducer();

  app.listen(PORT, () => {
    console.log(`API server listening on port ${PORT}`);
  });
}

async function shutdown(): Promise<void> {
  console.log('Shutting down API...');
  if (adminConnected) {
    await admin.disconnect();
  }
  await disconnectProducer();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start().catch((error) => {
  console.error('Failed to start API:', error);
  process.exit(1);
});
