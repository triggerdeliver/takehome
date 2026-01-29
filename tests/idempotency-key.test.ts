import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function waitForEvent(eventId: string, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const exists = await redis.exists(`event:${eventId}`);
    if (exists) return true;
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
}

async function runIdempotencyKeyTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY KEY TEST V2.3');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  const userId = `idem-key-user-${Date.now()}`;
  const ip = '10.8.0.1';
  const key = `idem-key-${Date.now()}`;

  console.log('\nSending initial event with Idempotency-Key...');
  const firstResponse = await axios.post(
    `${API_URL}/events`,
    { userId, type: 'idempotency_key', payload: { index: 1 } },
    { timeout: 5000, headers: { 'Idempotency-Key': key, 'x-forwarded-for': ip } }
  );

  const eventId = firstResponse.data?.eventId as string | undefined;
  if (!eventId) {
    failureReasons.push('Initial response missing eventId');
    return { passed: false, failureReasons };
  }

  const processed = await waitForEvent(eventId, 10000);
  if (!processed) {
    failureReasons.push('Initial event was not finalized in Redis');
  }

  const initialProcessedAt = await redis.hget(`event:${eventId}`, 'processedAt');
  if (!initialProcessedAt) {
    failureReasons.push('Initial processedAt missing in Redis');
  }

  const userCountBefore = await redis.zcard(`rate:${userId}`);
  const ipCountBefore = await redis.zcard(`rate:ip:${ip}`);

  console.log('\nSending duplicate with same Idempotency-Key (same payload)...');
  const dupResponse = await axios.post(
    `${API_URL}/events`,
    { userId, type: 'idempotency_key', payload: { index: 1 } },
    { timeout: 5000, headers: { 'Idempotency-Key': key, 'x-forwarded-for': ip } }
  );

  const dupEventId = dupResponse.data?.eventId as string | undefined;
  const dupStatus = dupResponse.data?.status as string | undefined;

  if (!dupEventId || dupEventId !== eventId) {
    failureReasons.push('Duplicate did not return the same eventId');
  }
  if (dupStatus !== 'duplicate') {
    failureReasons.push(`Duplicate status ${dupStatus || 'missing'} != duplicate`);
  }

  const userCountAfter = await redis.zcard(`rate:${userId}`);
  const ipCountAfter = await redis.zcard(`rate:ip:${ip}`);
  if (userCountAfter !== userCountBefore || ipCountAfter !== ipCountBefore) {
    failureReasons.push('Duplicate request consumed rate limit capacity');
  }

  const finalProcessedAt = await redis.hget(`event:${eventId}`, 'processedAt');
  if (finalProcessedAt && initialProcessedAt && finalProcessedAt !== initialProcessedAt) {
    failureReasons.push('processedAt changed after duplicate request');
  }

  console.log('\nTesting batch idempotency keys...');
  const batchUser = `idem-batch-${Date.now()}`;
  const batch = [
    { userId: batchUser, type: 'idem_batch', payload: { index: 1 }, idempotencyKey: `${key}-b1` },
    { userId: batchUser, type: 'idem_batch', payload: { index: 2 }, idempotencyKey: `${key}-b2` },
  ];

  const batchResponse = await axios.post(
    `${API_URL}/events/batch`,
    { events: batch },
    { timeout: 10000, headers: { 'x-forwarded-for': ip } }
  );

  const batchResults = batchResponse.data.results as Array<{ eventId: string; status: string }>;
  if (!batchResults || batchResults.length !== batch.length) {
    failureReasons.push('Batch results missing or wrong length');
  }

  const batchEventIds = batchResults.map((r) => r.eventId);
  for (const id of batchEventIds) {
    if (!id) failureReasons.push('Batch accepted event missing eventId');
  }

  for (const id of batchEventIds) {
    await waitForEvent(id, 10000);
  }

  const batchDupResponse = await axios.post(
    `${API_URL}/events/batch`,
    {
      events: [
        { userId: batchUser, type: 'idem_batch', payload: { index: 1 }, idempotencyKey: `${key}-b1` },
        { userId: batchUser, type: 'idem_batch', payload: { index: 2 }, idempotencyKey: `${key}-b2` },
      ],
    },
    { timeout: 10000, headers: { 'x-forwarded-for': ip } }
  );

  const batchDupResults = batchDupResponse.data.results as Array<{ eventId: string; status: string }>;
  for (let i = 0; i < batchDupResults.length; i += 1) {
    const r = batchDupResults[i];
    if (r.status !== 'duplicate') {
      failureReasons.push(`Batch duplicate status ${r.status} != duplicate`);
    }
    if (r.eventId !== batchEventIds[i]) {
      failureReasons.push('Batch duplicate returned different eventId');
    }
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY KEY TEST V2.3 RESULTS');
  console.log('═'.repeat(60));

  if (result.passed) {
    console.log('PASSED');
  } else {
    console.log('FAILED');
    for (const reason of result.failureReasons) {
      console.log(`   - ${reason}`);
    }
  }
  console.log('═'.repeat(60) + '\n');
}

runIdempotencyKeyTest()
  .then(async (result) => {
    printResult(result);
    await redis.quit();
    process.exit(result.passed ? 0 : 1);
  })
  .catch(async (error) => {
    console.error('Test failed with error:', error);
    await redis.quit();
    process.exit(1);
  });
