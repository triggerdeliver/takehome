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

async function runIdempotencyKeyMismatchTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY KEY MISMATCH TEST V2.3');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  const userId = `idem-mismatch-user-${Date.now()}`;
  const ip = '10.8.2.1';
  const key = `idem-mismatch-${Date.now()}`;

  console.log('\nSending initial event with Idempotency-Key...');
  const firstResponse = await axios.post(
    `${API_URL}/events`,
    { userId, type: 'idempotency_key_mismatch', payload: { value: 1 } },
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
  const userCountBefore = await redis.zcard(`rate:${userId}`);
  const ipCountBefore = await redis.zcard(`rate:ip:${ip}`);

  console.log('\nSending conflicting event with same Idempotency-Key...');
  let conflictStatus = 0;
  let conflictEventId: string | undefined;
  try {
    const conflictResponse = await axios.post(
      `${API_URL}/events`,
      { userId, type: 'idempotency_key_mismatch', payload: { value: 2 } },
      { timeout: 5000, headers: { 'Idempotency-Key': key, 'x-forwarded-for': ip } }
    );
    conflictStatus = conflictResponse.status;
    conflictEventId = conflictResponse.data?.eventId;
    failureReasons.push('Conflict request did not return 409');
  } catch (error) {
    const axiosError = axios.isAxiosError(error) ? error : null;
    conflictStatus = axiosError?.response?.status || 0;
    conflictEventId = axiosError?.response?.data?.eventId as string | undefined;
    if (conflictStatus !== 409) {
      failureReasons.push(`Conflict returned status ${conflictStatus}`);
    }
  }

  if (conflictEventId && conflictEventId !== eventId) {
    failureReasons.push('Conflict response eventId does not match original');
  }

  const finalProcessedAt = await redis.hget(`event:${eventId}`, 'processedAt');
  if (finalProcessedAt && initialProcessedAt && finalProcessedAt !== initialProcessedAt) {
    failureReasons.push('processedAt changed after conflict request');
  }

  const keys = await redis.keys('event:*');
  if (keys.length !== 1) {
    failureReasons.push(`Expected 1 event hash after conflict, got ${keys.length}`);
  }

  const userCountAfter = await redis.zcard(`rate:${userId}`);
  const ipCountAfter = await redis.zcard(`rate:ip:${ip}`);
  if (userCountAfter !== userCountBefore || ipCountAfter !== ipCountBefore) {
    failureReasons.push('Conflict request consumed rate limit capacity');
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY KEY MISMATCH TEST V2.3 RESULTS');
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

runIdempotencyKeyMismatchTest()
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
