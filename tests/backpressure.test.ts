import axios from 'axios';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

const LAG_THRESHOLD = parseInt(process.env.LAG_THRESHOLD || '5000', 10);
const BACKLOG_EVENTS = 8000;

const redis = new Redis(REDIS_URL);
const kafka = new Kafka({ clientId: 'backpressure-test', brokers: [KAFKA_BROKER] });

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function getKafkaLag(): Promise<number> {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const offsets = await admin.fetchOffsets({ groupId: 'event-processors', topics: ['events'] });
    const topicOffsets = await admin.fetchTopicOffsets('events');

    let totalLag = 0;
    for (const partition of offsets) {
      for (const p of partition.partitions) {
        const latestOffset = topicOffsets.find((t) => t.partition === p.partition);
        if (latestOffset) {
          const lag = parseInt(latestOffset.offset) - parseInt(p.offset);
          totalLag += Math.max(0, lag);
        }
      }
    }
    return totalLag;
  } finally {
    await admin.disconnect();
  }
}

async function produceBacklog(): Promise<void> {
  const producer = kafka.producer();
  await producer.connect();

  const messages = Array.from({ length: BACKLOG_EVENTS }, (_, i) => ({
    key: `bp-${i % 1000}`,
    value: JSON.stringify({
      id: `bp-${Date.now()}-${i}`,
      userId: `bp-user-${i % 100}`,
      type: 'backpressure',
      payload: { idx: i },
      timestamp: Date.now(),
    }),
  }));

  const batchSize = 1000;
  for (let i = 0; i < messages.length; i += batchSize) {
    await producer.send({ topic: 'events', messages: messages.slice(i, i + batchSize) });
  }

  await producer.disconnect();
}

async function stopConsumers(): Promise<void> {
  try {
    await execAsync('pkill -TERM -f "consumer"');
  } catch {}
}

async function startConsumer(): Promise<void> {
  exec('npm run start:consumer', { cwd: process.cwd(), env: process.env });
  await new Promise((r) => setTimeout(r, 3000));
}

async function runBackpressureTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('BACKPRESSURE TEST V2.1');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nStopping consumers to build lag...');
  await stopConsumers();

  console.log('\nProducing backlog directly to Kafka...');
  await produceBacklog();

  console.log('\nWaiting for lag to exceed threshold...');
  const start = Date.now();
  let lag = 0;
  while (Date.now() - start < 30000) {
    lag = await getKafkaLag();
    if (lag >= LAG_THRESHOLD) break;
    await new Promise((r) => setTimeout(r, 500));
  }

  if (lag < LAG_THRESHOLD) {
    failureReasons.push(`Lag ${lag} did not reach threshold ${LAG_THRESHOLD}`);
  }

  console.log('\nChecking API backpressure response...');
  let retryAfterOk = false;
  try {
    await axios.post(`${API_URL}/events`, {
      userId: 'bp-user',
      type: 'backpressure',
      payload: {},
    }, { timeout: 5000 });
    failureReasons.push('API did not return 503 under high lag');
  } catch (error) {
    if (!axios.isAxiosError(error) || error.response?.status !== 503) {
      failureReasons.push('API did not return 503 under high lag');
    } else {
      const retryAfter = error.response.headers['retry-after'];
      if (retryAfter && parseInt(retryAfter, 10) >= 1) {
        retryAfterOk = true;
      }
    }
  }
  if (!retryAfterOk) {
    failureReasons.push('Missing or invalid Retry-After header on 503');
  }

  console.log('\nChecking API batch backpressure response...');
  try {
    await axios.post(`${API_URL}/events/batch`, {
      events: Array.from({ length: 10 }, () => ({
        userId: 'bp-user',
        type: 'backpressure',
        payload: {},
      })),
    }, { timeout: 5000 });
    failureReasons.push('Batch API did not return 503 under high lag');
  } catch (error) {
    if (!axios.isAxiosError(error) || error.response?.status !== 503) {
      failureReasons.push('Batch API did not return 503 under high lag');
    }
  }

  console.log('\nRestarting consumer to drain lag...');
  await startConsumer();

  const drainStart = Date.now();
  while (Date.now() - drainStart < 60000) {
    lag = await getKafkaLag();
    if (lag === 0) break;
    await new Promise((r) => setTimeout(r, 1000));
  }

  console.log('\nChecking API accepts after lag drained...');
  try {
    const response = await axios.post(`${API_URL}/events`, {
      userId: 'bp-user',
      type: 'backpressure',
      payload: {},
    }, { timeout: 5000 });
    if (response.status !== 202) {
      failureReasons.push(`API returned status ${response.status} after lag drained`);
    }
  } catch {
    failureReasons.push('API did not accept after lag drained');
  }

  console.log('\nChecking batch API accepts after lag drained...');
  try {
    const response = await axios.post(`${API_URL}/events/batch`, {
      events: Array.from({ length: 10 }, (_, i) => ({
        userId: `bp-user-${i}`,
        type: 'backpressure',
        payload: { idx: i },
      })),
    }, { timeout: 5000 });
    if (response.status !== 202) {
      failureReasons.push(`Batch API returned status ${response.status} after lag drained`);
    }
  } catch {
    failureReasons.push('Batch API did not accept after lag drained');
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('BACKPRESSURE TEST V2.1 RESULTS');
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

runBackpressureTest()
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
