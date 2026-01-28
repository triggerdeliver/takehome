import axios from 'axios';
import Redis from 'ioredis';
import { spawn } from 'child_process';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const TOTAL_EVENTS = 5000;
const BATCH_SIZE = 250;

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

function startConsumer(): ReturnType<typeof spawn> {
  const child = spawn('npm', ['run', 'start:consumer'], {
    cwd: process.cwd(),
    env: process.env,
    stdio: 'ignore',
  });
  return child;
}

async function stopConsumer(child: ReturnType<typeof spawn>): Promise<void> {
  if (child.killed) return;
  child.kill('SIGTERM');
  await new Promise((resolve) => {
    const timeout = setTimeout(resolve, 3000);
    child.on('exit', () => {
      clearTimeout(timeout);
      resolve(undefined);
    });
  });
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function sendEvents(): Promise<string[]> {
  const eventIds: string[] = [];
  for (let i = 0; i < TOTAL_EVENTS; i += BATCH_SIZE) {
    const batch = Array.from({ length: Math.min(BATCH_SIZE, TOTAL_EVENTS - i) }, (_, j) => ({
      userId: `multi-user-${(i + j) % 200}`,
      type: 'multi_consumer',
      payload: { index: i + j },
    }));
    const ip = `10.6.0.${Math.floor(i / BATCH_SIZE) % 250}`;
    const response = await axios.post(
      `${API_URL}/events/batch`,
      { events: batch },
      { timeout: 10000, headers: { 'x-forwarded-for': ip } }
    );
    const results = response.data.results as Array<{ eventId: string; status: string }>;
    results.forEach((r) => {
      if (r.status === 'accepted') eventIds.push(r.eventId);
    });
  }

  return eventIds;
}

async function waitForProcessing(targetCount: number, timeoutMs: number): Promise<void> {
  const startTime = Date.now();
  while (Date.now() - startTime < timeoutMs) {
    const processedCount = await redis.keys('event:*').then((keys) => keys.length);
    if (processedCount >= targetCount) return;
    await new Promise((r) => setTimeout(r, 300));
  }
}

async function runMultiConsumerTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('MULTI-CONSUMER TEST V2.1');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nStarting two consumers...');
  const consumerA = startConsumer();
  const consumerB = startConsumer();
  await new Promise((r) => setTimeout(r, 3000));

  try {
    console.log('\nSending events...');
    const eventIds = await sendEvents();

    console.log('\nWaiting for processing...');
    await waitForProcessing(eventIds.length, 30000);

    const processedCount = await redis.keys('event:*').then((keys) => keys.length);
    if (processedCount < eventIds.length) {
      failureReasons.push(`Processed ${processedCount}/${eventIds.length} events`);
    }

    let duplicateCount = 0;
    for (const eventId of eventIds) {
      const attempts = await redis.hget(`event:${eventId}`, 'attempts');
      if (!attempts) {
        failureReasons.push(`Missing attempts for ${eventId}`);
        break;
      }
      if (parseInt(attempts, 10) > 1) {
        duplicateCount += 1;
      }
    }
    if (duplicateCount > 0) {
      failureReasons.push(`Detected ${duplicateCount} duplicates (attempts > 1)`);
    }

    const inflightKeys = await redis.keys('inflight:*');
    if (inflightKeys.length > 0) {
      failureReasons.push(`Found ${inflightKeys.length} inflight keys after processing`);
    }
  } finally {
    await stopConsumer(consumerA);
    await stopConsumer(consumerB);
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('MULTI-CONSUMER TEST V2.1 RESULTS');
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

runMultiConsumerTest()
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
