import Redis from 'ioredis';
import { ProcessingResult, RateLimitResult } from './types';

const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
});

// TTL constants (см. REDIS_SCHEMA.md)
const EVENT_TTL_SEC = 86400; // 24 hours
const DEDUP_TTL_MS = 86400000; // 24 hours
const INFLIGHT_TTL_MS = 30000; // 30 seconds

// Rate limit constants
const RATE_LIMIT = 100;
const RATE_WINDOW_MS = 1000;
const RATE_KEY_TTL_SEC = 2;

export type AcquireStatus = 'acquired' | 'inflight' | 'processed';

// Lua scripts for atomic ops
(redis as any).defineCommand('acquireEvent', {
  numberOfKeys: 2,
  lua: `
    if redis.call('EXISTS', KEYS[1]) == 1 then
      return 2
    end
    if redis.call('EXISTS', KEYS[2]) == 1 then
      return 0
    end
    redis.call('SET', KEYS[2], '1', 'PX', ARGV[1])
    return 1
  `,
});

(redis as any).defineCommand('finalizeEvent', {
  numberOfKeys: 3,
  lua: `
    if redis.call('EXISTS', KEYS[1]) == 1 then
      redis.call('DEL', KEYS[3])
      return 0
    end
    local ttl = tonumber(ARGV[1])
    local dedupTtl = tonumber(ARGV[#ARGV])
    for i = 2, (#ARGV - 1), 2 do
      redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
    end
    redis.call('EXPIRE', KEYS[1], ttl)
    redis.call('SET', KEYS[2], '1', 'PX', dedupTtl)
    redis.call('DEL', KEYS[3])
    return 1
  `,
});

(redis as any).defineCommand('rateLimitCheck', {
  numberOfKeys: 1,
  lua: `
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    local member = ARGV[4]
    local ttl = tonumber(ARGV[5])

    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
    local count = redis.call('ZCARD', KEYS[1])

    if count >= limit then
      local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
      local resetAt = now + window
      if oldest[2] ~= nil then
        resetAt = tonumber(oldest[2]) + window
      end
      return {0, 0, resetAt}
    end

    redis.call('ZADD', KEYS[1], now, member)
    redis.call('EXPIRE', KEYS[1], ttl)

    local remaining = limit - count - 1
    local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    local resetAt = now + window
    if oldest[2] ~= nil then
      resetAt = tonumber(oldest[2]) + window
    end
    return {1, remaining, resetAt}
  `,
});

export async function tryAcquireEvent(eventId: string): Promise<AcquireStatus> {
  const result = await (redis as any).acquireEvent(
    `event:${eventId}`,
    `inflight:${eventId}`,
    INFLIGHT_TTL_MS
  );

  const code = typeof result === 'number' ? result : parseInt(result, 10);
  if (code === 1) return 'acquired';
  if (code === 2) return 'processed';
  return 'inflight';
}

export async function saveProcessingResult(result: ProcessingResult): Promise<boolean> {
  const key = `event:${result.eventId}`;
  const dedupKey = `dedup:${result.eventId}`;
  const inflightKey = `inflight:${result.eventId}`;

  const args: string[] = [
    EVENT_TTL_SEC.toString(),
    'status',
    result.status,
    'processedAt',
    result.processedAt.toString(),
  ];

  if (result.error) {
    args.push('error', result.error);
  }

  args.push(DEDUP_TTL_MS.toString());

  const saved = await (redis as any).finalizeEvent(key, dedupKey, inflightKey, ...args);
  return Number(saved) === 1;
}

export async function checkRateLimit(userId: string, member?: string): Promise<RateLimitResult> {
  const now = Date.now();
  const uniqueMember = member || `${now}:${Math.random().toString(36).slice(2, 10)}`;

  const result = await (redis as any).rateLimitCheck(
    `rate:${userId}`,
    now,
    RATE_WINDOW_MS,
    RATE_LIMIT,
    uniqueMember,
    RATE_KEY_TTL_SEC
  );

  const allowed = result[0] === 1 || result[0] === '1';
  const remaining = parseInt(result[1], 10) || 0;
  const resetAt = parseInt(result[2], 10) || now + RATE_WINDOW_MS;

  return { allowed, remaining, resetAt };
}

export async function isEventProcessed(eventId: string): Promise<boolean> {
  const exists = await redis.exists(`event:${eventId}`);
  return exists === 1;
}

export async function isDuplicate(eventId: string): Promise<boolean> {
  const exists = await redis.exists(`dedup:${eventId}`);
  return exists === 1;
}

// ============ Метрики (только для диагностики, НЕ источник истины!) ============

export async function getMetrics(): Promise<{
  processed: number;
  failed: number;
  rateLimited: number;
  duplicate: number;
}> {
  const [processed, failed, rateLimited, duplicate] = await Promise.all([
    redis.get('metrics:processed'),
    redis.get('metrics:failed'),
    redis.get('metrics:rate_limited'),
    redis.get('metrics:duplicate'),
  ]);

  return {
    processed: parseInt(processed || '0'),
    failed: parseInt(failed || '0'),
    rateLimited: parseInt(rateLimited || '0'),
    duplicate: parseInt(duplicate || '0'),
  };
}

export async function incrementMetric(
  metric: 'processed' | 'failed' | 'rate_limited' | 'duplicate'
): Promise<void> {
  await redis.incr(`metrics:${metric}`);
}

export async function resetMetrics(): Promise<void> {
  await redis.del(
    'metrics:processed',
    'metrics:failed',
    'metrics:rate_limited',
    'metrics:duplicate'
  );
}

// ============ Утилиты для тестов (используются в tests/) ============

export async function countProcessedEvents(): Promise<number> {
  const keys = await redis.keys('event:*');
  return keys.length;
}

export async function getAllProcessedEventIds(): Promise<string[]> {
  const keys = await redis.keys('event:*');
  return keys.map((k) => k.replace('event:', ''));
}

export async function flushAll(): Promise<void> {
  await redis.flushall();
}

export async function disconnect(): Promise<void> {
  await redis.quit();
}

export { redis };
