# Backend Take-Home: Real-Time Event Processing Pipeline (Hard v2.1)

## Overview

You are given a simplified CDP prototype: API receives events, producer publishes to Kafka, consumer processes events and writes status to Redis.

The current implementation is intentionally incomplete. Your task is to make it production-ready under **Hard v2.1** acceptance tests.

---

## What is included

```
takehome/
├── README.md
├── REDIS_SCHEMA.md           # Redis schema spec (mandatory)
├── docker-compose.yml        # Kafka + Redis + Zookeeper
├── src/
│   ├── api.ts
│   ├── producer.ts
│   ├── consumer.ts
│   ├── redis-client.ts
│   └── types.ts
├── tests/
│   ├── load-hard.test.ts
│   ├── reliability-chaos.test.ts
│   ├── ordering.test.ts
│   ├── dlq.test.ts
│   ├── idempotency-hard.test.ts
│   ├── rate-limit-v2.test.ts
│   ├── multi-consumer.test.ts
│   ├── backpressure.test.ts
│   └── submission-hard.test.ts
└── package.json
```

---

## Requirements

### 1) Performance (End-to-End)
- **1,000,000 events in 30 seconds**
- Successfully processed >= 95% by **Redis truth source**
- Kafka consumer lag returns to **0**

### 2) Reliability + Chaos
- Consumer can be SIGTERM-killed and restarted **multiple times** during processing
- **0 lost events**
- **0 duplicates** (no second processing, no processedAt rewrite)
- No stale `inflight:*` keys after processing completes
- Kafka lag returns to **0**

### 3) Idempotency
- Same `eventId` delivered again (including **direct Kafka duplicates**) must not re-run business logic
- `processedAt` must not change
- `attempts` must remain **1** for successfully processed events
- `dedup:{eventId}` must exist for all processed events

### 4) Ordering per userId (out-of-order arrival)
- Events have `payload.seq`
- For each `userId`, events must be processed in **strict order** of `seq`
- **Do not assume arrival order**; events may arrive out of order and must be buffered
- Output order must be stored in Redis list `order:{userId}`

### 5) Retry + DLQ (non-blocking)
- If `payload.fail = true`, you must retry **3 times**, then move to DLQ
- Failed events must be finalized in `event:{eventId}` with `status=failed` and attempts >= 3
- **Failed events must not block subsequent seq** for the same user (ordering list should skip failed seq)
- DLQ must be stored as:
  - `dlq:list` list and `dlq:{eventId}` hash (see schema)

### 6) Rate Limiting v2.1 (Sliding Window)
- **100 events/sec per userId**
- **500 events/sec per IP**
- Must work on both `/events` and `/events/batch`
- Must accept partial batch results
- Accuracy >= 99%
- IP should be determined from `x-forwarded-for` header (fallback to remote IP)

### 7) Backpressure
- If Kafka lag for group `event-processors` > **5000**, API must return **503**
- Must apply to **both** `/events` and `/events/batch`
- 503 responses must include `Retry-After` header (seconds)
- When lag is below threshold, API returns **202**

### 8) Multi-consumer correctness
- Tests will run **2 consumer processes**
- No duplicates, no lost events, ordering must still hold

---

## Source of truth

Redis keys defined in `REDIS_SCHEMA.md` are the **only** acceptance source.
Metrics are **diagnostic only**.

---

## Constraints

1. **Do not change tests/**
2. **Do not change docker-compose.yml**
3. **No external queues or databases** (Kafka + Redis only)
4. You may add npm packages

---

## How to run

```bash
npm install

docker-compose up -d

# run API + consumer
npm run dev:api
npm run dev:consumer

# tests
npm run test:submission
```

For a non-default port, set `API_URL` and `PORT`.

---

## What to submit

1. Fixed code in `src/`
2. `SOLUTION.md` describing:
   - Problems found
   - Fixes
   - Trade-offs
   - Next steps

---

## Expected time

Hard v2.1 is designed to require **2–3 hours** for a solid solution.

---

## Acceptance tests

All tests in `tests/` must pass. Any failure = submission rejected.
