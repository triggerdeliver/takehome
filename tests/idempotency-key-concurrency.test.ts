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

async function runIdempotencyKeyConcurrencyTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY KEY CONCURRENCY TEST V2.3');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  const userId = `idem-concurrent-user-${Date.now()}`;
  const ip = '10.8.1.1';
  const key = `idem-concurrent-${Date.now()}`;

  console.log('\nSending concurrent requests with the same Idempotency-Key...');
  const requests = Array.from({ length: 20 }, () =>
    axios
      .post(
        `${API_URL}/events`,
        { userId, type: 'idempotency_key_concurrency', payload: { index: 1 } },
        { timeout: 5000, headers: { 'Idempotency-Key': key, 'x-forwarded-for': ip } }
      )
      .then((res) => ({ ok: true as const, res }))
      .catch((err) => ({ ok: false as const, err }))
  );

  const results = await Promise.all(requests);
  const failures = results.filter((r) => !r.ok);
  if (failures.length > 0) {
    failureReasons.push(`Concurrent requests had ${failures.length} failures`);
    return { passed: false, failureReasons };
  }

  const responses = results.map((r) => (r as { ok: true; res: any }).res);
  const eventIds = responses.map((r) => r.data?.eventId).filter(Boolean) as string[];
  const statuses = responses.map((r) => r.data?.status as string | undefined);

  if (eventIds.length !== responses.length) {
    failureReasons.push('Some responses missing eventId');
  }

  const uniqueEventIds = Array.from(new Set(eventIds));
  if (uniqueEventIds.length !== 1) {
    failureReasons.push(`Expected 1 unique eventId, got ${uniqueEventIds.length}`);
  }

  const acceptedCount = statuses.filter((s) => s === 'accepted').length;
  const invalidStatuses = statuses.filter((s) => s !== 'accepted' && s !== 'duplicate');
  if (acceptedCount !== 1) {
    failureReasons.push(`Expected exactly 1 accepted response, got ${acceptedCount}`);
  }
  if (invalidStatuses.length > 0) {
    failureReasons.push(`Unexpected statuses: ${invalidStatuses.join(', ')}`);
  }

  const eventId = uniqueEventIds[0];
  if (eventId) {
    const processed = await waitForEvent(eventId, 10000);
    if (!processed) {
      failureReasons.push('Event was not finalized in Redis');
    } else {
      const attempts = await redis.hget(`event:${eventId}`, 'attempts');
      if (attempts && attempts !== '1') {
        failureReasons.push(`Event attempts ${attempts} != 1`);
      }
    }
  }

  const userCount = await redis.zcard(`rate:${userId}`);
  const ipCount = await redis.zcard(`rate:ip:${ip}`);
  if (userCount !== 1 || ipCount !== 1) {
    failureReasons.push(`Rate limit counts incorrect: user=${userCount}, ip=${ipCount}`);
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY KEY CONCURRENCY TEST V2.3 RESULTS');
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

runIdempotencyKeyConcurrencyTest()
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
