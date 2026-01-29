import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const GAP_TIMEOUT_MS = 2000;

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function waitForOrderLength(userId: string, expected: number, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const length = await redis.llen(`order:${userId}`);
    if (length >= expected) return true;
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
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

async function runOrderingGapTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('ORDERING GAP TEST V2.3: gap timeout + late arrival');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  const userId = `gap-user-${Date.now()}`;
  const ip = '10.7.0.1';

  console.log('\nSending out-of-order events with a gap (missing seq=3)...');
  const events = [
    { userId, type: 'ordering_gap', payload: { seq: 4 } },
    { userId, type: 'ordering_gap', payload: { seq: 1 } },
    { userId, type: 'ordering_gap', payload: { seq: 5 } },
    { userId, type: 'ordering_gap', payload: { seq: 2 } },
  ];

  await axios.post(
    `${API_URL}/events/batch`,
    { events },
    { timeout: 10000, headers: { 'x-forwarded-for': ip } }
  );

  const gotFirstTwo = await waitForOrderLength(userId, 2, 10000);
  if (!gotFirstTwo) {
    failureReasons.push('Did not process initial seq 1-2 in time');
  }

  await new Promise((r) => setTimeout(r, GAP_TIMEOUT_MS + 800));

  const gotAll = await waitForOrderLength(userId, 4, 10000);
  if (!gotAll) {
    failureReasons.push('Gap timeout did not allow buffered seqs to proceed');
  }

  const list = await redis.lrange(`order:${userId}`, 0, -1);
  const expected = ['1', '2', '4', '5'];
  if (list.length !== expected.length) {
    failureReasons.push(`Order list length ${list.length} != ${expected.length}`);
  } else {
    for (let i = 0; i < expected.length; i += 1) {
      if (list[i] !== expected[i]) {
        failureReasons.push(`Order list mismatch at ${i}: got ${list[i]}, expected ${expected[i]}`);
        break;
      }
    }
  }

  console.log('\nSending late seq=3 after gap timeout...');
  const lateResponse = await axios.post(
    `${API_URL}/events`,
    { userId, type: 'ordering_gap', payload: { seq: 3 } },
    { timeout: 5000, headers: { 'x-forwarded-for': ip } }
  );

  const lateEventId = lateResponse.data?.eventId as string | undefined;
  if (!lateEventId) {
    failureReasons.push('Late arrival response missing eventId');
  } else {
    const lateExists = await waitForEvent(lateEventId, 10000);
    if (!lateExists) {
      failureReasons.push('Late arrival was not finalized in Redis');
    } else {
      const data = await redis.hgetall(`event:${lateEventId}`);
      if (!data || data.status !== 'skipped') {
        failureReasons.push(`Late arrival status ${data?.status || 'missing'} != skipped`);
      }
      if (data && data.attempts !== '0') {
        failureReasons.push(`Late arrival attempts ${data.attempts} != 0`);
      }
    }
  }

  const finalList = await redis.lrange(`order:${userId}`, 0, -1);
  if (finalList.length !== expected.length || finalList.join(',') !== expected.join(',')) {
    failureReasons.push('Order list changed after late arrival');
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('ORDERING GAP TEST V2.3 RESULTS');
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

runOrderingGapTest()
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
