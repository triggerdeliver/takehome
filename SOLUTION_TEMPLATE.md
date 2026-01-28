# Solution (Hard v2.1)

## Problems found

1. ...
2. ...

## Fixes

### 1) ...
**File:** `src/...`
**Before:**
```ts
...
```
**After:**
```ts
...
```
**Why:** ...

## Ordering
- How you handle out-of-order arrival
- How you guarantee per-user order
- How you store `order:{userId}`

## Idempotency
- How you prevent duplicates
- How you handle direct Kafka duplicates
- How `attempts` is computed

## Retry + DLQ
- Retry policy
- DLQ schema and enqueue logic
- How failed seq does not block subsequent events

## Backpressure
- How lag is measured
- When API returns 503
- `Retry-After` behavior

## Rate limit v2
- Algorithm for user and IP limits
- Sliding window accuracy

## Trade-offs
1. ...

## Test results
```
Load test:        ...
Reliability:      ...
Ordering:         ...
DLQ:              ...
Idempotency:      ...
Rate limit:       ...
Multi-consumer:   ...
Backpressure:     ...
```

## Next steps
1. ...
