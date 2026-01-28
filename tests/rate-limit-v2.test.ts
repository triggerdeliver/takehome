import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const USER_RATE_LIMIT = 100;
const IP_RATE_LIMIT = 500;
const ACCURACY_THRESHOLD = 0.98;
const MAX_DRIFT = 2;

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  const keys = await redis.keys('rate:*');
  if (keys.length > 0) {
    await redis.del(...keys);
  }
}

async function testUserLimit(): Promise<{ allowed: number; rejected: number; accuracy: number; userId: string; ip: string }>{
  const userId = `rate-user-${Date.now()}`;
  const ip = '10.10.10.10';
  let allowed = 0;
  let rejected = 0;

  const requests = Array.from({ length: 150 }, async () => {
    try {
      await axios.post(
        `${API_URL}/events`,
        { userId, type: 'rate_user', payload: {} },
        { timeout: 5000, headers: { 'x-forwarded-for': ip } }
      );
      allowed++;
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 429) {
        rejected++;
      }
    }
  });

  await Promise.all(requests);

  const accuracy = 1 - Math.abs(allowed - USER_RATE_LIMIT) / 150;
  return { allowed, rejected, accuracy, userId, ip };
}

async function testIpLimit(): Promise<{ allowed: number; rejected: number; accuracy: number; ip: string }>{
  const ip = '10.10.10.20';
  let allowed = 0;
  let rejected = 0;

  const requests = Array.from({ length: 600 }, async (_, idx) => {
    try {
      await axios.post(
        `${API_URL}/events`,
        { userId: `rate-ip-user-${idx}`, type: 'rate_ip', payload: {} },
        { timeout: 5000, headers: { 'x-forwarded-for': ip } }
      );
      allowed++;
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 429) {
        rejected++;
      }
    }
  });

  await Promise.all(requests);

  const accuracy = 1 - Math.abs(allowed - IP_RATE_LIMIT) / 600;
  return { allowed, rejected, accuracy, ip };
}

async function testBatchLimit(): Promise<{ allowed: number; rejected: number; accuracy: number }>{
  const userId = `rate-batch-${Date.now()}`;
  const ip = '10.10.10.30';
  const events = Array.from({ length: 150 }, (_, i) => ({
    userId,
    type: 'rate_batch',
    payload: { index: i },
  }));

  let allowed = 0;
  let rejected = 0;

  const response = await axios.post(
    `${API_URL}/events/batch`,
    { events },
    { timeout: 10000, headers: { 'x-forwarded-for': ip } }
  );

  const results = response.data.results as Array<{ eventId: string; status: string }>;
  for (const r of results) {
    if (r.status === 'accepted') allowed++;
    if (r.status === 'rate_limited') rejected++;
  }

  const accuracy = 1 - Math.abs(allowed - USER_RATE_LIMIT) / 150;
  return { allowed, rejected, accuracy };
}

async function expectRateKeys(userId: string, ip: string): Promise<boolean> {
  const [userExists, ipExists] = await Promise.all([
    redis.exists(`rate:${userId}`),
    redis.exists(`rate:ip:${ip}`),
  ]);
  return userExists === 1 && ipExists === 1;
}

async function testIpEnforcedAfterBurst(ip: string): Promise<boolean> {
  try {
    await axios.post(
      `${API_URL}/events`,
      { userId: `rate-ip-check-${Date.now()}`, type: 'rate_ip_check', payload: {} },
      { timeout: 5000, headers: { 'x-forwarded-for': ip } }
    );
    return false;
  } catch (error) {
    return axios.isAxiosError(error) && error.response?.status === 429;
  }
}

async function testSlidingWindow(): Promise<boolean> {
  const userId = `rate-sliding-${Date.now()}`;
  const ip = '10.10.10.40';

  let firstBatchAllowed = 0;
  const firstBatch = Array.from({ length: 80 }, async () => {
    try {
      await axios.post(`${API_URL}/events`, {
        userId,
        type: 'rate_sliding',
        payload: {},
      }, { headers: { 'x-forwarded-for': ip } });
      firstBatchAllowed++;
    } catch {}
  });
  await Promise.all(firstBatch);

  await new Promise((r) => setTimeout(r, 500));

  let secondBatchAllowed = 0;
  const secondBatch = Array.from({ length: 40 }, async () => {
    try {
      await axios.post(`${API_URL}/events`, {
        userId,
        type: 'rate_sliding',
        payload: {},
      }, { headers: { 'x-forwarded-for': ip } });
      secondBatchAllowed++;
    } catch {}
  });
  await Promise.all(secondBatch);

  return secondBatchAllowed > 0 && secondBatchAllowed < 40;
}

async function runRateLimitTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('RATE LIMIT V2.1 TEST');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up rate limit keys...');
  await cleanupRedis();

  console.log('\nTesting per-user limit...');
  const userResult = await testUserLimit();
  if (userResult.rejected === 0 || userResult.accuracy < ACCURACY_THRESHOLD) {
    failureReasons.push(`User limit accuracy ${(userResult.accuracy * 100).toFixed(1)}%`);
  }
  if (
    userResult.allowed < USER_RATE_LIMIT - MAX_DRIFT ||
    userResult.allowed > USER_RATE_LIMIT + MAX_DRIFT
  ) {
    failureReasons.push(`User limit allowed ${userResult.allowed} out of bounds`);
  }
  const userKeysOk = await expectRateKeys(userResult.userId, userResult.ip);
  if (!userKeysOk) {
    failureReasons.push('User/IP rate keys not created for per-user test');
  }

  await new Promise((r) => setTimeout(r, 1500));
  await cleanupRedis();

  console.log('\nTesting per-ip limit...');
  const ipResult = await testIpLimit();
  if (ipResult.rejected === 0 || ipResult.accuracy < ACCURACY_THRESHOLD) {
    failureReasons.push(`IP limit accuracy ${(ipResult.accuracy * 100).toFixed(1)}%`);
  }
  if (ipResult.allowed < IP_RATE_LIMIT - MAX_DRIFT || ipResult.allowed > IP_RATE_LIMIT + MAX_DRIFT) {
    failureReasons.push(`IP limit allowed ${ipResult.allowed} out of bounds`);
  }
  const ipKeysOk = await expectRateKeys(`rate-ip-user-0`, ipResult.ip);
  if (!ipKeysOk) {
    failureReasons.push('User/IP rate keys not created for per-ip test');
  }
  const ipEnforced = await testIpEnforcedAfterBurst(ipResult.ip);
  if (!ipEnforced) {
    failureReasons.push('IP limit not enforced after burst');
  }

  await new Promise((r) => setTimeout(r, 1500));
  await cleanupRedis();

  console.log('\nTesting batch limit...');
  const batchResult = await testBatchLimit();
  if (batchResult.rejected === 0 || batchResult.accuracy < ACCURACY_THRESHOLD) {
    failureReasons.push(`Batch limit accuracy ${(batchResult.accuracy * 100).toFixed(1)}%`);
  }
  if (
    batchResult.allowed < USER_RATE_LIMIT - MAX_DRIFT ||
    batchResult.allowed > USER_RATE_LIMIT + MAX_DRIFT
  ) {
    failureReasons.push(`Batch allowed ${batchResult.allowed} out of bounds`);
  }

  console.log('\nTesting sliding window behavior...');
  const isSliding = await testSlidingWindow();
  if (!isSliding) {
    failureReasons.push('Sliding window not detected');
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('RATE LIMIT V2.1 RESULTS');
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

runRateLimitTest()
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
