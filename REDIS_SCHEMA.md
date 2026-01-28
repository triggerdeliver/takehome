# Redis Schema Specification (Hard v2.1)

This schema is mandatory. Tests validate Redis directly.

---

## 1) Event status (source of truth)

### `event:{eventId}` — Hash

Final status of event processing.

**Required fields:**
| Field | Type | Description |
|------|------|-------------|
| `status` | string | `processed` \| `failed` \| `rate_limited` |
| `processedAt` | int | Unix ms timestamp |
| `attempts` | int | Total attempts (must be `1` for successful events) |
| `userId` | string | User ID |
| `type` | string | Event type |
| `seq` | int | Sequence number when `payload.seq` is provided |

**Optional fields:**
| Field | Type | Description |
|------|------|-------------|
| `error` | string | Error text for failed events |

**TTL:** >= 1 hour (recommended 24h)

Presence of `event:{eventId}` means the event is finalized.
For failed events: `status=failed`, `attempts >= 3`, and `error` should be set.

---

## 2) Deduplication

### `dedup:{eventId}` — String

Atomic protection from duplicate processing.

```
SET dedup:{eventId} 1 NX PX 86400000
```

If exists, the event was already processed or is in-flight.

---

## 3) In-flight marker

### `inflight:{eventId}` — String

Marker for active processing. TTL 30–60 seconds.

---

## 4) Ordering per user

### `order:{userId}` — List

Strict order of processed `seq` values per user.

Example:
```
LPUSH order:{userId} <seq>
RPUSH order:{userId} <seq>
```

**Required:** The final list must be `[1,2,3,...]` for each user in tests.
If a seq is DLQ'd, it must be skipped and subsequent seq values must continue.

---

## 5) Dead Letter Queue (DLQ)

### `dlq:list` — List

Append eventIds that exceeded retry limit.

### `dlq:{eventId}` — Hash

Fields:
| Field | Type | Description |
|------|------|-------------|
| `eventId` | string | Event ID |
| `attempts` | int | Attempts made (>= 3) |
| `error` | string | Error message |
| `queuedAt` | int | Unix ms timestamp |

---

## 6) Rate limiting (sliding window)

### User limit
`rate:{userId}` — ZSET

### IP limit
`rate:ip:{ip}` — ZSET

Algorithm:
```
ZREMRANGEBYSCORE rate:{userId} 0 now-1000
count = ZCARD rate:{userId}
if count >= 100 -> reject
ZADD rate:{userId} now <member>
EXPIRE rate:{userId} 2
```

IP limit is identical with threshold 500.

---

## 7) Metrics (diagnostic only)

Optional, not required for acceptance.

---

## Acceptance rules

- Lost events = missing `event:{eventId}`
- Duplicates = `attempts > 1` or `processedAt` changed
- Ordering = `order:{userId}` list is strictly increasing (skips failed seq)
- DLQ = every failing event ends in `dlq:list` and `dlq:{eventId}`
- Cleanup = no leftover `inflight:*` keys after processing
