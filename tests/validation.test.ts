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

async function runValidationTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('VALIDATION TEST V2.3');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nTesting invalid single event...');
  let singleInvalidStatus = 0;
  try {
    await axios.post(
      `${API_URL}/events`,
      { type: 'invalid_event', payload: {} },
      { timeout: 5000, headers: { 'x-forwarded-for': '10.9.0.1' } }
    );
    failureReasons.push('Invalid single event did not return 400');
  } catch (error) {
    const axiosError = axios.isAxiosError(error) ? error : null;
    singleInvalidStatus = axiosError?.response?.status || 0;
    if (singleInvalidStatus !== 400) {
      failureReasons.push(`Invalid single event returned ${singleInvalidStatus}`);
    }
  }

  const invalidKeys = await redis.keys('event:*');
  if (invalidKeys.length !== 1) {
    failureReasons.push(`Expected 1 invalid event in Redis, got ${invalidKeys.length}`);
  } else {
    const data = await redis.hgetall(invalidKeys[0]);
    if (!data || data.status !== 'invalid') {
      failureReasons.push(`Invalid event status ${data?.status || 'missing'} != invalid`);
    }
    if (data && data.attempts !== '0') {
      failureReasons.push(`Invalid event attempts ${data.attempts} != 0`);
    }
    if (!data || !data.error) {
      failureReasons.push('Invalid event missing error field');
    }
  }

  console.log('\nTesting invalid seq in single event...');
  await cleanupRedis();
  try {
    await axios.post(
      `${API_URL}/events`,
      { userId: 'val-user', type: 'invalid_seq', payload: { seq: 0 } },
      { timeout: 5000, headers: { 'x-forwarded-for': '10.9.0.2' } }
    );
    failureReasons.push('Invalid seq event did not return 400');
  } catch (error) {
    const axiosError = axios.isAxiosError(error) ? error : null;
    const status = axiosError?.response?.status || 0;
    if (status !== 400) {
      failureReasons.push(`Invalid seq event returned ${status}`);
    }
  }

  const invalidSeqKeys = await redis.keys('event:*');
  if (invalidSeqKeys.length !== 1) {
    failureReasons.push(`Expected 1 invalid seq event in Redis, got ${invalidSeqKeys.length}`);
  } else {
    const data = await redis.hgetall(invalidSeqKeys[0]);
    if (!data || data.status !== 'invalid') {
      failureReasons.push(`Invalid seq event status ${data?.status || 'missing'} != invalid`);
    }
    if (data && data.attempts !== '0') {
      failureReasons.push(`Invalid seq event attempts ${data.attempts} != 0`);
    }
  }

  console.log('\nTesting batch validation (mixed valid/invalid)...');
  await cleanupRedis();

  const batch = [
    { userId: 'val-batch-user', type: 'valid', payload: { index: 1 }, idempotencyKey: `val-${Date.now()}-1` },
    { type: 'invalid_missing_user', payload: {} },
    { userId: 'val-batch-user', type: 'invalid_seq', payload: { seq: -1 } },
  ];

  const batchResponse = await axios.post(
    `${API_URL}/events/batch`,
    { events: batch },
    { timeout: 10000, headers: { 'x-forwarded-for': '10.9.0.3' } }
  );

  const results = batchResponse.data.results as Array<{ eventId: string; status: string; error?: string }>;
  if (!results || results.length !== batch.length) {
    failureReasons.push('Batch results missing or wrong length');
  } else {
    const validResult = results[0];
    if (validResult.status !== 'accepted') {
      failureReasons.push(`Valid batch event status ${validResult.status} != accepted`);
    }

    for (let i = 1; i < results.length; i += 1) {
      const r = results[i];
      if (r.status !== 'invalid') {
        failureReasons.push(`Batch invalid status ${r.status} != invalid`);
      }
      if (!r.eventId) {
        failureReasons.push('Batch invalid missing eventId');
      }
      if (!r.error) {
        failureReasons.push('Batch invalid missing error');
      }
    }

    const invalidIds = results.slice(1).map((r) => r.eventId).filter(Boolean) as string[];
    for (const id of invalidIds) {
      const exists = await waitForEvent(id, 10000);
      if (!exists) {
        failureReasons.push(`Invalid batch event ${id} not finalized in Redis`);
        continue;
      }
      const data = await redis.hgetall(`event:${id}`);
      if (!data || data.status !== 'invalid') {
        failureReasons.push(`Invalid batch event status ${data?.status || 'missing'} != invalid`);
      }
      if (data && data.attempts !== '0') {
        failureReasons.push(`Invalid batch event attempts ${data.attempts} != 0`);
      }
    }
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('VALIDATION TEST V2.3 RESULTS');
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

runValidationTest()
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
