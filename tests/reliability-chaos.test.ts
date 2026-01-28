import axios from 'axios';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import { exec, execSync } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

const EVENTS_PHASE_A = 2000;
const EVENTS_PHASE_B = 2000;
const EVENTS_PHASE_C = 2000;
const WAIT_TIMEOUT_MS = 60000;

const redis = new Redis(REDIS_URL);
const kafka = new Kafka({ clientId: 'reliability-chaos-test', brokers: [KAFKA_BROKER] });

interface TestResult {
  sentEventIds: string[];
  foundInRedis: string[];
  lostEvents: string[];
  duplicateEvents: string[];
  kafkaLag: number;
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function sendEventsAndCollectIds(
  count: number,
  prefix: string
): Promise<{ sent: number; eventIds: string[] }> {
  const eventIds: string[] = [];
  let sent = 0;
  const batchSize = 200;
  const phaseTag = prefix === 'phase_a' ? 1 : prefix === 'phase_b' ? 2 : 3;

  for (let i = 0; i < count; i += batchSize) {
    const batch = Array.from({ length: Math.min(batchSize, count - i) }, (_, j) => ({
      userId: `reliability-user-${(i + j) % 50}`,
      type: 'reliability_chaos_test',
      payload: { prefix, index: i + j },
    }));
    const ip = `10.2.${phaseTag}.${Math.floor(i / batchSize) % 250}`;

    try {
      const response = await axios.post(
        `${API_URL}/events/batch`,
        { events: batch },
        { timeout: 10000, headers: { 'x-forwarded-for': ip } }
      );
      const results = response.data.results as Array<{ eventId: string; status: string }>;
      for (const r of results) {
        if (r.status === 'accepted') {
          eventIds.push(r.eventId);
          sent++;
        }
      }
    } catch (error) {
      console.error('Error sending batch:', error);
    }
  }

  return { sent, eventIds };
}

async function restartConsumer(): Promise<void> {
  console.log('   Sending SIGTERM to consumer...');
  try {
    await execAsync('pkill -TERM -f "consumer"');
  } catch {}

  await new Promise((r) => setTimeout(r, 2000));

  console.log('   Starting consumer...');
  exec('npm run start:consumer', { cwd: process.cwd(), env: process.env });

  await new Promise((r) => setTimeout(r, 3000));
}

async function checkEventsInRedis(eventIds: string[]): Promise<{
  found: string[];
  notFound: string[];
  duplicates: string[];
}> {
  const found: string[] = [];
  const notFound: string[] = [];
  const duplicates: string[] = [];

  for (const eventId of eventIds) {
    const exists = await redis.exists(`event:${eventId}`);
    if (exists) {
      found.push(eventId);
      const attempts = await redis.hget(`event:${eventId}`, 'attempts');
      if (attempts && parseInt(attempts, 10) > 1) {
        duplicates.push(eventId);
      }
    } else {
      notFound.push(eventId);
    }
  }

  return { found, notFound, duplicates };
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

async function waitForAllProcessed(eventIds: string[], timeoutMs: number): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    const processedCount = await redis.keys('event:*').then((keys) => keys.length);
    if (processedCount >= eventIds.length) {
      return;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
}

async function runReliabilityTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('RELIABILITY CHAOS TEST V2.1');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];
  const allEventIds: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log(`\nPhase A: Sending ${EVENTS_PHASE_A} events...`);
  const phaseA = await sendEventsAndCollectIds(EVENTS_PHASE_A, 'phase_a');
  allEventIds.push(...phaseA.eventIds);
  console.log(`   Sent: ${phaseA.sent} events`);

  console.log('\nWaiting for partial processing (2s)...');
  await new Promise((r) => setTimeout(r, 2000));

  console.log('\nRestarting consumer (SIGTERM)...');
  await restartConsumer();

  console.log(`\nPhase B: Sending ${EVENTS_PHASE_B} events...`);
  const phaseB = await sendEventsAndCollectIds(EVENTS_PHASE_B, 'phase_b');
  allEventIds.push(...phaseB.eventIds);
  console.log(`   Sent: ${phaseB.sent} events`);

  console.log('\nWaiting for partial processing (2s)...');
  await new Promise((r) => setTimeout(r, 2000));

  console.log('\nRestarting consumer again (SIGTERM)...');
  await restartConsumer();

  console.log(`\nPhase C: Sending ${EVENTS_PHASE_C} events...`);
  const phaseC = await sendEventsAndCollectIds(EVENTS_PHASE_C, 'phase_c');
  allEventIds.push(...phaseC.eventIds);
  console.log(`   Sent: ${phaseC.sent} events`);

  console.log('\nWaiting for all events to be processed...');
  await waitForAllProcessed(allEventIds, WAIT_TIMEOUT_MS);

  console.log('\nVerifying events in Redis...');
  const { found, notFound, duplicates } = await checkEventsInRedis(allEventIds);

  let kafkaLag = 0;
  try {
    kafkaLag = await getKafkaLag();
  } catch {
    console.log('   Warning: Could not check Kafka lag');
  }

  if (notFound.length > 0) {
    failureReasons.push(`Lost ${notFound.length} events`);
  }
  if (duplicates.length > 0) {
    failureReasons.push(`${duplicates.length} duplicate events detected (attempts > 1)`);
  }
  if (kafkaLag > 0) {
    failureReasons.push(`Kafka lag is ${kafkaLag}, should be 0`);
  }

  const inflightKeys = await redis.keys('inflight:*');
  if (inflightKeys.length > 0) {
    failureReasons.push(`Found ${inflightKeys.length} inflight keys after processing`);
  }

  const passed = failureReasons.length === 0;

  return {
    sentEventIds: allEventIds,
    foundInRedis: found,
    lostEvents: notFound,
    duplicateEvents: duplicates,
    kafkaLag,
    passed,
    failureReasons,
  };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('RELIABILITY CHAOS TEST V2.1 RESULTS');
  console.log('═'.repeat(60));
  console.log(`  Events sent:           ${result.sentEventIds.length}`);
  console.log(`  Events found in Redis: ${result.foundInRedis.length}`);
  console.log(`  Lost events:           ${result.lostEvents.length}`);
  console.log(`  Duplicate events:      ${result.duplicateEvents.length}`);
  console.log(`  Kafka lag:             ${result.kafkaLag}`);
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

runReliabilityTest()
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
