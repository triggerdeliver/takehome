# Backend Take-Home: Real-Time Event Processing Pipeline (Hard v2.3)

## Overview

You are given a simplified CDP prototype: API receives events, producer publishes to Kafka, consumer processes events and writes status to Redis.

The current implementation is intentionally incomplete. Your task is to make it production-ready under **Hard v2.3** acceptance tests.

---

## New in v2.3

- Idempotency key **conflict detection** (payload mismatch)
- **Concurrent** idempotency key safety (atomic)
- Backpressure threshold aligned with load test to remove false conflicts
- Custom tests requirement increased
- Additional hidden edge cases (see notes below)

---

## What is included

```
takehome/
├── README.md
├── SOLUTION_TEMPLATE.md
├── docker-compose.yml
├── docs/
│   └── internal/REDIS_SCHEMA_INTERNAL.md   # internal reference (do not ship to candidate)
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
│   ├── ordering-gap.test.ts
│   ├── dlq.test.ts
│   ├── idempotency-hard.test.ts
│   ├── idempotency-key.test.ts
│   ├── idempotency-key-concurrency.test.ts
│   ├── idempotency-key-mismatch.test.ts
│   ├── rate-limit-v2.test.ts
│   ├── multi-consumer.test.ts
│   ├── backpressure.test.ts
│   ├── validation.test.ts
│   ├── custom-runner.test.ts
│   └── submission-hard.test.ts
└── package.json
```

> **Important:** `REDIS_SCHEMA.md` is intentionally **not** provided. You must design it and include it in your submission.

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

### 3) Idempotency (eventId-level)
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

### 6) Rate Limiting v2.3 (Sliding Window)
- **100 events/sec per userId**
- **500 events/sec per IP**
- Must work on both `/events` and `/events/batch`
- Must accept partial batch results
- Accuracy >= 99%
- IP should be determined from `x-forwarded-for` header (fallback to remote IP)

### 7) Backpressure
- If Kafka lag for group `event-processors` > **500000**, API must return **503**
- Must apply to **both** `/events` and `/events/batch`
- 503 responses must include `Retry-After` header (seconds)
- When lag is below threshold, API returns **202**

### 8) Multi-consumer correctness
- Tests will run **2 consumer processes**
- No duplicates, no lost events, ordering must still hold

### 9) Input validation + invalid events
- Validate request shape: `userId` (non-empty string), `type` (string), `payload` (object)
- When `payload.seq` is present, it must be a **positive integer**
- Invalid events **must not be sent to Kafka**
- `/events` should return **400** with a clear error
- `/events/batch` must return per-item results with `status=invalid`, `eventId`, and `error`
- Invalid events must be finalized in Redis with `status=invalid`, `attempts=0`, and `error`

### 10) Idempotency keys (API-level)
- `/events` must accept an `Idempotency-Key` header
- `/events/batch` must accept `idempotencyKey` per event
- If a key is reused within 24h, return the **same eventId** and **do not reprocess or re-enqueue**
- Duplicates must return `status=duplicate` and must **not count** against rate limits
- Concurrency: multiple simultaneous requests with the same key must be **atomic** and resolve to a single event
- Batch: duplicate `idempotencyKey` values within the same batch must resolve to a single eventId

### 11) Idempotency key conflicts
- If the same key is reused with **different userId/type/payload**, return **409 Conflict**
- Response must include the **original eventId** and must **not** enqueue new work
- Conflicts must **not** consume rate limit capacity

### 12) Ordering gap timeout + late arrivals
- If `nextExpectedSeq` for a user does not arrive within **2000 ms**, you must **skip the gap** and continue with buffered events
- Late arrivals (`seq < nextExpectedSeq`) must be finalized with `status=skipped` and must **not** alter `order:{userId}`

### 13) Custom tests (required)
- Add **at least 3** meaningful tests in `tests/custom/`
- They must run with `npm run test:custom`
- Describe your tests in `SOLUTION.md`

---

## Source of truth

Redis keys (that you design) are the **only** acceptance source.
Metrics are **diagnostic only**.

---

## Constraints

1. **Do not change tests/** (except adding your own under `tests/custom/`)
2. **Do not change docker-compose.yml**
3. **No external queues or databases** (Kafka + Redis only)
4. You may add npm packages

---

## Hidden edge cases

Not every edge case is listed in this README. Expect hidden tests for:
- Idempotency key payload mismatch
- Concurrent duplicate idempotency keys
- Multi-value `x-forwarded-for`
- Late-arriving out-of-order events
- Partial failures in batches
- Consumer restarts during in-flight ordering buffers

Treat this like a production system and handle edge cases defensively.

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
2. **Your own `REDIS_SCHEMA.md`** describing your schema
3. `SOLUTION.md` describing:
   - Problems found
   - Fixes
   - Trade-offs
   - New tests you added
   - Next steps

---

## Expected time

Hard v2.3 is designed to require **4-5 hours** for a solid solution.

---

## Acceptance tests

All tests in `tests/` must pass. Any failure = submission rejected.
