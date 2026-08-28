# ADR-003: Leaky Bucket Optimization for Semaphore Capacity Management

## Status
Proposed

## Context

The current MVP implementation (ADR-002) successfully demonstrates traffic shaping for AWS Bedrock API rate limits, but uses a conservative approach that limits throughput unnecessarily. With a 100 RPM (requests per minute) limit, the system currently processes requests at approximately 2 requests/second, resulting in test execution times of 100+ seconds for 100 requests.

### Current Limitations
- Burst capacity is too conservative relative to the RPM limit
- Queue processor operates inefficiently with small batch sizes
- No mechanism for parallel processing of queued requests
- Test performance doesn't scale well with realistic workloads

### Key Observations
- 100 RPM = 100 requests per 60 seconds = 1.667 tokens/second refill rate
- Jamba model inference time: ~300ms per request
- Theoretical capacity allows for significant burst traffic (up to 100 concurrent requests in a minute)
- Current implementation treats RPM as strict rate limiting rather than budget over a time window

## Decision

Implement a **consumption-based capacity tracking** system with the following enhancements:

### 1. Burst Capacity for Direct Requests
- **Burst capacity: 50 tokens** (50% of RPM limit, configurable)
- Budget Manager only checks burst capacity consumption
- Allows up to 50 requests to process immediately in parallel
- Natural refill: capacity becomes available as consumption records expire (60-second TTL)

### 2. Consumption Record Tracking
Instead of maintaining a mutable burst state record (which causes write contention), track individual consumption records:

**DynamoDB Schema:**
```
Configuration Record (static):
  PK: "budget#{model_id}"
  SK: "config"
  burst_capacity: 50
  rpm_limit: 100

Consumption Records (time-based, auto-expire):
  PK: "budget#{model_id}"
  SK: "consumption#{timestamp}#{request_id}"
  count: 1
  source: "burst" | "queue"
  ttl: timestamp + 300  # 5 minutes - records must exist from previous 60 seconds
```

**Key Benefits:**
- No write contention (each consumption record has unique sort key)
- No conditional writes needed
- Automatic cleanup via DynamoDB TTL
- Scales well under high concurrency

### 3. Budget Manager Logic

The Budget Manager operates with a regeneration-aware capacity model and uses an **optimistic write/query pattern** to handle race conditions during burst traffic.

#### Race Condition Challenge

When multiple requests arrive simultaneously (e.g., 55 requests at T=0), a naive query-then-write approach creates a race condition:

1. All 55 requests query DynamoDB at the same time
2. All 55 see the same state (e.g., 0 consumption records, 50 tokens available)
3. All 55 think they can proceed and write consumption records
4. Result: 55 tokens consumed from a 50-token bucket ❌

The gap between reading consumption records (query) and writing new consumption records (put_item) allows multiple requests to make decisions based on stale data.

#### Optimistic Write/Query Pattern (Solution)

To handle this race condition, the Budget Manager uses an **optimistic write followed by verification** pattern:

```python
def try_reserve_and_process(request):
    now = current_time()
    now_ms = int(now * 1000)
    request_id = request['id']
    
    # Step 1: Optimistically write consumption record
    # All concurrent requests will succeed (unique sort keys)
    single_table.put_item(
        Item={
            'pk': f'MODEL#{model_id}#burst',
            'sk': f'CONSUMPTION#{now_ms}#{request_id}',
            'request_id': request_id,
            'count': 1,
            'source': 'burst',
            'consumed_at': datetime.fromtimestamp(now).isoformat(),
            'ttl': int(now) + 300  # 5 minutes - records must exist from previous 60 seconds
        }
    )
    
    # Step 2: Immediately verify total consumption
    # Use ConsistentRead to ensure we see our own write
    sixty_seconds_ago_ms = now_ms - 60000
    burst_records = single_table.query(
        KeyConditionExpression=Key('pk').eq(f'MODEL#{model_id}#burst') & 
                              Key('sk').between(
                                  f'CONSUMPTION#{sixty_seconds_ago_ms}#',
                                  f'CONSUMPTION#{now_ms}#~'
                              ),
        ConsistentRead=True  # Critical: ensures we see our own write
    )['Items']
    
    # Step 3: Calculate available capacity with regeneration
    available_burst = calculate_available_tokens(
        capacity=50,
        consumption_records=burst_records,
        current_time=now
    )
    
    # Step 4: Self-correct if we over-consumed
    if available_burst < 0:
        # We contributed to over-consumption - rollback and enqueue
        single_table.delete_item(
            Key={
                'pk': f'MODEL#{model_id}#burst',
                'sk': f'CONSUMPTION#{now_ms}#{request_id}'
            }
        )
        enqueue_request(request)
        trigger_queue_processor_if_needed()
        return {"status": "queued", "reason": "capacity_exceeded"}
    else:
        # Capacity available - proceed with processing
        process_request(request)
        return {"status": "processed"}
```

#### How This Handles the Race Condition

**Scenario: 55 requests arrive at T=0**

```
T=0-5ms:   All 55 write consumption records (all succeed, unique sort keys)
T=5-10ms:  All 55 query with ConsistentRead=True
           - First ~50 requests see: consumed ≈ 50, available ≈ 0, proceed ✓
           - Last ~5 requests see: consumed ≈ 55, available < 0, rollback ✓
T=10-15ms: Last ~5 delete their records and enqueue
T=15ms:    Final state: ~50 consumed (burst), ~5 queued
```

**Key Properties:**
- **No write contention:** All writes succeed independently (unique sort keys)
- **Self-correcting:** Over-consumption is detected and rolled back
- **Fair ordering:** Requests that write earlier are more likely to proceed
- **Conservative:** Prefers queueing over over-consumption
- **Worst case:** 1-2 extra requests may slip through during extreme contention (acceptable with buffer)

#### Error Handling

**Delete Failure (< 0.01% probability):**
- If `delete_item()` fails, Lambda propagates the exception
- Client receives 500 error and retries
- Orphaned consumption record reduces capacity by 1 token temporarily
- TTL cleans up the record after 5 minutes
- Impact is minimal and self-healing

**Enqueue Failure:**
- If `enqueue_request()` fails after successful delete, Lambda returns error
- Client receives 500 error and retries
- Request is not lost (client retry is the safety net)

Both failure scenarios are extremely rare and handled by standard client retry logic.

**Separation of Concerns:**
- Budget Manager only knows about its 50-token burst allocation
- Queue Processor handles the 40-token queue allocation
- 10-token buffer (10% of RPM) provides safety margin for race conditions and edge cases
- Both components track consumption independently via DynamoDB records
- Both components use the same regeneration-aware capacity calculation

### 4. Queue Processor Capacity Management

**Capacity Allocation Strategy:**
- **Burst capacity: 50 tokens** (50% of RPM limit) - for immediate processing via Budget Manager
- **Queue capacity: 40 tokens** (40% of RPM limit) - for queued request processing
- **Buffer: 10 tokens** (10% of RPM limit) - safety margin for burst race conditions and edge cases
- Total: 50 + 40 + 10 = 100 tokens (100% of RPM limit)

**Key Principles:**
1. **Independent Capacity Tracking:** Queue processor tracks consumption separately from burst in `MODEL#{model_id}#queue` partition
2. **FIFO Queue Processing:** Queue depth represents total requests waiting (not time-bounded), processed in insertion order
3. **Capacity-Limited Batching:** Batch size determined by available capacity in 60-second sliding window
4. **Aggressive Consumption:** Uses all available capacity before scheduling next invocation
5. **Regeneration-Aware:** Uses actual timestamps and calculates regeneration based on oldest consumption

**Queue Processing Logic:**

```python
def process_queue_batch():
    """
    Process requests from queue in FIFO order.
    Uses actual timestamps for each batch to track consumption accurately.
    Accounts for continuous token regeneration.
    """
    # TODO: This refill_rate needs to be multiplied by the queue capacity allocation percentage
    # For Queue Processor (40 tokens / 100 RPM): refill_rate = (100 / 60.0) * 0.40 = 0.667 tokens/second
    # Current implementation uses global rate which over-estimates regeneration for queue component
    refill_rate = 100 / 60.0  # 1.667 tokens/second
    
    # Get configuration
    config = single_table.get_item(
        Key={'pk': f'MODEL#{model_id}', 'sk': 'CONFIG'}
    )['Item']
    
    queue_capacity = 40  # 40% of RPM limit
    batch_size_config = config['queue_batch_size']  # e.g., 15
    
    processed_batches = []
    
    while True:  # Loop to process multiple batches in same invocation
        # Get current time for THIS iteration
        now = current_time()
        now_ms = int(now * 1000)
        sixty_seconds_ago_ms = now_ms - 60000
        
        # Query queue consumption in last 60 seconds
        queue_records = single_table.query(
            KeyConditionExpression=Key('pk').eq(f'MODEL#{model_id}#queue') & 
                                  Key('sk').between(
                                      f'CONSUMPTION#{sixty_seconds_ago_ms}#',
                                      f'CONSUMPTION#{now_ms}#~'
                                  )
        )['Items']
        
        # Calculate available capacity with regeneration
        available_capacity = calculate_available_tokens(
            capacity=queue_capacity,  # 40 tokens
            consumption_records=queue_records,
            current_time=now
        )
        
        if available_capacity <= 0:
            # Calculate when we'll have enough tokens for next batch
            tokens_consumed = sum(record['count'] for record in queue_records)
            queue_depth = get_queue_depth()
            
            if queue_depth == 0:
                return {"status": "queue_empty", "batches_processed": processed_batches}
            
            tokens_needed = min(batch_size_config, queue_depth)
            
            # Solve: capacity - tokens_consumed + (n * refill_rate) >= tokens_needed
            # n = (tokens_needed - capacity + tokens_consumed) / refill_rate
            deficit = tokens_needed - (queue_capacity - tokens_consumed)
            wait_seconds = max(1, math.ceil(deficit / refill_rate))
            
            schedule_self_invocation(delay_seconds=wait_seconds)
            return {
                "status": "capacity_exhausted",
                "next_run": now + wait_seconds,
                "batches_processed": processed_batches,
                "total_consumed": tokens_consumed,
                "wait_seconds": wait_seconds
            }
        
        # Get FULL queue depth (FIFO, not time-bounded)
        queue_depth = get_queue_depth()
        
        if queue_depth == 0:
            return {
                "status": "queue_empty",
                "batches_processed": processed_batches
            }
        
        # Determine batch size: limited by capacity (60s window), config, and queue depth
        batch_size = min(
            int(available_capacity),     # Can't exceed capacity in 60s window (with regeneration)
            batch_size_config,            # Can't exceed configured batch size (15)
            queue_depth                   # Can't exceed total requests in queue
        )
        
        # Dequeue batch in FIFO order and process in parallel
        requests = dequeue_batch(batch_size)  # Gets oldest requests first
        process_requests_parallel(requests)
        
        # Record consumption using actual current time
        # Each batch gets its own timestamp
        for request in requests:
            single_table.put_item(
                Item={
                    'pk': f'MODEL#{model_id}#queue',
                    'sk': f'CONSUMPTION#{now_ms}#{request["id"]}',
                    'request_id': request['id'],
                    'count': 1,
                    'source': 'queue',
                    'consumed_at': datetime.fromtimestamp(now).isoformat(),
                    'ttl': int(now) + 300  # 5 minutes - records must exist from previous 60 seconds
                }
            )
        
        processed_batches.append({
            "batch_size": batch_size,
            "capacity_before": available_capacity,
            "queue_depth_before": queue_depth
        })
        
        # Continue loop to check if more batches can be processed
        # Loop will query again and see the consumption records we just wrote
```

**Why Actual Timestamps Matter:**

Each batch writes consumption records with the actual time it occurred. This allows accurate tracking of when tokens were consumed and enables proper regeneration calculation:

- **Batch 1 (T=0s):** Write 15 records with timestamp=0
- **Batch 2 (T=0.3s):** Write 15 records with timestamp=0.3
- **Batch 3 (T=0.6s):** Write 10 records with timestamp=0.6

When calculating available capacity, we find the oldest timestamp (T=0) and calculate regeneration based on total elapsed time. This accounts for all tokens regenerated during the entire consumption period.

**Queue Depth vs Capacity Distinction:**

- **Queue Depth:** Total number of requests waiting in FIFO order (could be 1000 requests)
  - Not time-bounded
  - Represents actual work remaining
  - Determines if queue is empty

- **Available Capacity:** Tokens available in 60-second sliding window (max 45 for queue)
  - Time-bounded (last 60 seconds)
  - Determines how many requests can be processed now
  - Limits batch size

- **Batch Size:** `min(available_capacity, batch_size_config, queue_depth)`
  - Limited by capacity (can't exceed 60s window limit)
  - Limited by config (e.g., 15 requests per batch)
  - Limited by actual queue depth (can't process more than waiting)

**Example Execution Timeline (100 RPM, 40 queued requests):**

**T=0s (Invocation 1 starts):**
- Queue depth: 40 requests
- Query consumption (last 60s): 0 records
- Tokens consumed: 0
- Tokens regenerated: 0 (no prior consumption)
- Available capacity: 40 - 0 + 0 = 40 tokens
- **Batch 1:** `min(40, 15, 40) = 15` requests processed
- Write 15 consumption records with timestamp=0

**T=0.3s (Still Invocation 1):**
- Queue depth: 25 requests
- Query consumption (last 60s): 15 records (at T=0)
- Tokens consumed: 15
- Oldest timestamp: T=0
- Time elapsed: 0.3s
- Tokens regenerated: 0.3s × 1.667 = 0.5 tokens
- Available capacity: 40 - 15 + 0.5 = 25.5 tokens
- **Batch 2:** `min(25, 15, 25) = 15` requests processed
- Write 15 consumption records with timestamp=0.3

**T=0.6s (Still Invocation 1):**
- Queue depth: 10 requests
- Query consumption (last 60s): 30 records (15 at T=0, 15 at T=0.3)
- Tokens consumed: 30
- Oldest timestamp: T=0
- Time elapsed: 0.6s
- Tokens regenerated: 0.6s × 1.667 = 1.0 tokens
- Available capacity: 40 - 30 + 1.0 = 11.0 tokens
- **Batch 3:** `min(11, 15, 10) = 10` requests processed
- Write 10 consumption records with timestamp=0.6
- Queue empty, invocation completes

**Total processing time: ~0.9 seconds for 40 queued requests**

**Example with Capacity Exhaustion (100 RPM, 50 queued requests):**

**T=0s-0.9s:** Same as above (processes 40 requests in 3 batches)
- 15 records at T=0, 15 at T=0.3, 10 at T=0.6
- Remaining queue: 10 requests

**T=0.9s (Still Invocation 1):**
- Queue depth: 10 requests
- Query consumption (last 60s): 40 records
- Tokens consumed: 40
- Oldest timestamp: T=0
- Time elapsed: 0.9s
- Tokens regenerated: 0.9s × 1.667 = 1.5 tokens
- Available capacity: 40 - 40 + 1.5 = 1.5 tokens
- **Batch 4:** `min(1, 15, 10) = 1` request processed
- Write 1 consumption record with timestamp=0.9
- Remaining queue: 9 requests
- Remaining capacity: ~0.5 tokens
- **Calculate next invocation time:**
  - Tokens needed: min(15, 9) = 9
  - Current available: 40 - 41 + 1.5 = 0.5
  - Need 8.5 more tokens
  - Wait time: 8.5 / 1.667 = 5.1 seconds
  - **Schedule for T=6.0s**

**T=6.0s (Invocation 2 starts):**
- Queue depth: 9 requests
- Query consumption (last 60s): 41 records (all from T=0 to T=0.9)
- Tokens consumed: 41
- Oldest timestamp: T=0
- Time elapsed: 6.0s
- Tokens regenerated: 6.0s × 1.667 = 10.0 tokens
- Available capacity: 40 - 41 + 10.0 = 9.0 tokens
- **Batch 1:** `min(9, 15, 9) = 9` requests processed
- Queue empty

**Total processing time: ~6.3 seconds for 50 queued requests**

This is significantly faster than waiting until T=60s for tokens to expire!

**Key Behaviors:**
1. **Aggressive consumption:** Uses all available capacity before waiting
2. **Immediate re-processing:** Loops within same invocation if capacity remains
3. **Smart scheduling:** Calculates exact wait time based on token regeneration rate (not fixed 10-second delay)
4. **Batch optimization:** Processes up to batch_size (15) per batch, limited by capacity and queue depth
5. **Independent tracking:** Queue consumption tracked separately from burst consumption
6. **FIFO guarantee:** Always processes oldest requests first, regardless of capacity constraints
7. **Regeneration-aware:** Accounts for continuous token regeneration at 1.667 tokens/second

### 5. Token Regeneration Model

Instead of waiting for consumption records to expire (TTL = 60 seconds), the system accounts for **continuous token regeneration** at the RPM refill rate.

**Regeneration-Aware Capacity Calculation:**

```python
def calculate_available_tokens(capacity, consumption_records, current_time):
    """
    Calculate available tokens accounting for continuous regeneration.
    
    Formula: tokens_available = capacity - tokens_consumed + tokens_regenerated
    """
    # TODO: This refill_rate needs to be multiplied by the capacity allocation percentage
    # For Budget Manager (50 tokens / 100 RPM): refill_rate = (100 / 60.0) * 0.50 = 0.833 tokens/second
    # For Queue Processor (40 tokens / 100 RPM): refill_rate = (100 / 60.0) * 0.40 = 0.667 tokens/second
    # Current implementation uses global rate which over-estimates regeneration for individual components
    refill_rate = 100 / 60.0  # RPM / 60 = 1.667 tokens/second
    
    if not consumption_records:
        return capacity
    
    # Count total tokens consumed in last 60 seconds
    tokens_consumed = sum(record['count'] for record in consumption_records)
    
    # Find oldest consumption timestamp in the window
    oldest_timestamp = min(
        extract_timestamp_from_sk(record['sk']) 
        for record in consumption_records
    )
    
    # Calculate time elapsed since oldest consumption
    time_elapsed = current_time - oldest_timestamp
    
    # Calculate tokens regenerated since oldest consumption
    tokens_regenerated = time_elapsed * refill_rate
    
    # Calculate available tokens
    tokens_available = capacity - tokens_consumed + tokens_regenerated
    
    return max(0, tokens_available)  # Can't be negative
```

**Why Regeneration Matters:**

The sliding window alone doesn't account for continuous token regeneration. Consider this scenario:

**Without Regeneration Awareness:**
- T=0s: 50 burst requests consume all burst capacity
- T=5s: New request arrives
- Tokens consumed in last 60s: 50
- Available capacity: 50 - 50 = 0 ❌ Request queued

**With Regeneration Awareness:**
- T=0s: 50 burst requests consume all burst capacity
- T=5s: New request arrives
- Tokens consumed in last 60s: 50
- Tokens regenerated: 5 seconds × 1.667 tokens/second = 8.33 tokens
- Available capacity: 50 - 50 + 8.33 = 8.33 ✓ Request processed immediately

**Key Benefits:**
- More accurate capacity tracking
- Better utilization of available RPM budget
- Smoother traffic handling (no artificial waiting for TTL expiration)
- Natural rate limiting that respects the continuous nature of RPM limits

## Performance Analysis

### Example: 125 Requests @ 100 RPM Limit

**T=0s:**
- 125 requests arrive
- First 50 consume burst bucket → process immediately
- Remaining 75 queued

**T=0.3s:**
- First 50 complete (300ms inference time)

**T=1s:**
- Queue processor invoked
- Dequeues 15 requests → processes in parallel
- Remaining: 60 queued

**T=1.3s:**
- First queue batch completes
- Schedules next invocation at T=11s (10s from initial T=1s)

**T=11s:**
- Second queue batch (15 requests)
- Remaining: 45 queued

**T=21s:**
- Third queue batch (15 requests)
- Remaining: 30 queued

**T=31s:**
- Fourth queue batch (15 requests)
- Remaining: 15 queued

**T=41s:**
- Fifth queue batch (15 requests)
- Remaining: 0 queued

**T=41.3s:**
- All 125 requests complete

**Total time: ~41 seconds** (vs. 100+ seconds currently)

### Efficiency Gains
- **Current:** 100 requests in 100+ seconds
- **Optimized:** 125 requests in ~41 seconds
- **Effective throughput:** ~183 RPM burst capacity while respecting 100 RPM limit over 60s window
- **Improvement:** ~60% reduction in total processing time

## Implementation Components

### 1. DynamoDB Schema Updates

**New Single Table Design:**
Create a new table `semaphore-single-table` using generic `pk` and `sk` attributes to support multiple entity types and enable gradual migration from multi-table design.

**Table Schema:**
```python
Table: semaphore-single-table
  Partition Key: pk (STRING)
  Sort Key: sk (STRING)
  TTL: ttl (NUMBER)
  Billing: PAY_PER_REQUEST
```

**Entity Types:**

**1. Consumption Records:**
```python
{
  "pk": "MODEL#{model_id}#{capacity_mode}",    # e.g., "MODEL#jamba-mini#burst" or "MODEL#jamba-mini#queue"
  "sk": "CONSUMPTION#{timestamp}#{request_id}", # e.g., "CONSUMPTION#1704110400000#req-123"
  "request_id": "req-123",
  "count": 1,                                  # Number of tokens consumed
  "source": "burst",                           # "burst" or "queue" (redundant with pk but useful for filtering)
  "consumed_at": "2024-01-01T12:00:00",       # ISO timestamp (for readability)
  "ttl": 1704110760                            # Unix timestamp + 300 seconds (5 minutes)
}
```

**2. Configuration Records:**
```python
{
  "pk": "MODEL#{model_id}",                    # e.g., "MODEL#jamba-mini"
  "sk": "CONFIG",                              # Static sort key for config
  "burst_capacity": 50,                        # Configurable, default 50% of RPM
  "rpm_limit": 100,
  "queue_batch_size": 15,
  "max_allocations": 100,                      # Keep for backward compatibility
  "refresh_rate": 1.667                        # Keep for backward compatibility
}
```

**Key Design Decisions:**
- **Generic pk/sk attributes:** Enables single-table design patterns and future entity types
- **Entity type prefixes:** `MODEL#`, `CONSUMPTION#`, `CONFIG` provide clear namespacing
- **Separate partitions by capacity mode:** `MODEL#{model_id}#burst` and `MODEL#{model_id}#queue` isolate burst and queue consumption for independent scaling
- **Sort key with timestamp prefix:** Enables efficient range queries for consumption records
- **Configuration uses model-only PK:** `MODEL#{model_id}` / `CONFIG` for shared configuration
- **TTL enabled:** Automatic cleanup of consumption records after 60 seconds
- **Future-proof:** Can add other entity types (e.g., `QUEUE#`, `ALLOCATION#`) without schema changes

**Query Patterns:**

```python
# Get configuration for a model
response = table.get_item(
    Key={
        'pk': 'MODEL#jamba-mini',
        'sk': 'CONFIG'
    }
)

# Query burst consumption in last 60 seconds
now_ms = int(time.time() * 1000)
sixty_seconds_ago_ms = now_ms - 60000

response = table.query(
    KeyConditionExpression=Key('pk').eq('MODEL#jamba-mini#burst') & 
                          Key('sk').between(
                              f'CONSUMPTION#{sixty_seconds_ago_ms}#',
                              f'CONSUMPTION#{now_ms}#~'  # '~' sorts after all request_ids
                          )
)

# Query queue consumption in last 60 seconds (separate partition)
response = table.query(
    KeyConditionExpression=Key('pk').eq('MODEL#jamba-mini#queue') & 
                          Key('sk').between(
                              f'CONSUMPTION#{sixty_seconds_ago_ms}#',
                              f'CONSUMPTION#{now_ms}#~'
                          )
)

# Query ALL consumption (burst + queue) - requires two queries
burst_response = table.query(
    KeyConditionExpression=Key('pk').eq('MODEL#jamba-mini#burst') & 
                          Key('sk').begins_with('CONSUMPTION#')
)
queue_response = table.query(
    KeyConditionExpression=Key('pk').eq('MODEL#jamba-mini#queue') & 
                          Key('sk').begins_with('CONSUMPTION#')
)
total_consumed = sum(item['count'] for item in burst_response['Items']) + \
                 sum(item['count'] for item in queue_response['Items'])
```

**Migration Path:**
This single-table design allows gradual migration of other entities:
- Phase 1: Consumption tracking (this ADR)
- Phase 2: Queue records (migrate from `semaphore-rate-limiter-queue`)
- Phase 3: Allocation records (migrate from `semaphore-rate-limiter-allocations`)
- Phase 4: Semaphore state (migrate from `semaphore-rate-limiter-semaphore`)

### 2. Budget Manager Updates

**Core Logic with Optimistic Write/Query Pattern:**
```python
def try_reserve_and_process(request):
    """
    Check burst capacity using optimistic write/query pattern.
    Handles race conditions by writing first, then verifying capacity.
    Only checks burst allocation (50 tokens), not full RPM limit.
    """
    now = current_time()
    now_ms = int(now * 1000)
    request_id = request['id']
    
    # Step 1: Optimistically write consumption record
    # All concurrent requests succeed (unique sort keys)
    single_table.put_item(
        Item={
            'pk': f'MODEL#{model_id}#burst',
            'sk': f'CONSUMPTION#{now_ms}#{request_id}',
            'request_id': request_id,
            'count': 1,
            'source': 'burst',
            'consumed_at': datetime.fromtimestamp(now).isoformat(),
            'ttl': int(now) + 300
        }
    )
    
    # Step 2: Verify total consumption with consistent read
    sixty_seconds_ago_ms = now_ms - 60000
    burst_records = single_table.query(
        KeyConditionExpression=Key('pk').eq(f'MODEL#{model_id}#burst') & 
                              Key('sk').between(
                                  f'CONSUMPTION#{sixty_seconds_ago_ms}#',
                                  f'CONSUMPTION#{now_ms}#~'
                              ),
        ConsistentRead=True  # Ensures we see our own write
    )['Items']
    
    # Step 3: Calculate available capacity with regeneration
    available_burst = calculate_available_tokens(
        capacity=50,
        consumption_records=burst_records,
        current_time=now
    )
    
    # Step 4: Self-correct if over-consumed
    if available_burst < 0:
        # Rollback: delete our consumption record and enqueue
        single_table.delete_item(
            Key={
                'pk': f'MODEL#{model_id}#burst',
                'sk': f'CONSUMPTION#{now_ms}#{request_id}'
            }
        )
        enqueue_request(request)
        trigger_queue_processor_if_needed()
        return {"status": "queued", "reason": "capacity_exceeded"}
    else:
        # Capacity available - process request
        process_request(request)
        return {"status": "processed"}
```

**Key Changes from MVP:**
- **Optimistic write first:** Write consumption record before checking capacity
- **Consistent read verification:** Query with ConsistentRead=True to see own write
- **Self-correction:** Delete and enqueue if over-consumed
- **No conditional writes:** Eliminates write contention
- **Regeneration-aware:** Uses calculate_available_tokens() for accurate capacity tracking
- **Error handling:** Delete or enqueue failures propagate as 500 errors (client retries)

### 3. Queue Processor Updates

**TODO:** Queue processor capacity management will be implemented separately from burst capacity.

The queue processor will:
- Have its own independent capacity allocation (separate from the 50-token burst capacity)
- Track consumption in separate partition: `MODEL#{model_id}#queue`
- Use the same consumption tracking pattern as burst (timestamp-based records with TTL)
- Implement smart self-invocation scheduling (10-second intervals)
- Process requests in batches (e.g., 15 at a time)

**Key principle:** Burst capacity and queue capacity are managed independently. The budget manager only checks burst capacity. The queue processor only checks queue capacity. This separation of concerns simplifies the logic and allows independent optimization of each component.

### 4. Configuration Parameters
New environment variables:
- `SINGLE_TABLE_NAME`: `semaphore-single-table` (new table name)
- `BURST_CAPACITY`: 50 (tokens, configurable)
- `RPM_LIMIT`: 100 (requests per minute)
- `QUEUE_PROCESSOR_BATCH_SIZE`: 15 (requests per batch)
- `CONSUMPTION_TTL`: 300 (seconds, 5 minutes - records must exist from previous 60 seconds)

Existing tables remain for backward compatibility during migration:
- `SEMAPHORE_TABLE_NAME`: `semaphore-rate-limiter-semaphore`
- `QUEUE_TABLE_NAME`: `semaphore-rate-limiter-queue`
- `ALLOCATIONS_TABLE_NAME`: `semaphore-rate-limiter-allocations`

## Consequences

### Positive
- Significantly improved throughput for burst traffic
- Eliminates write contention (no shared state updates)
- More efficient queue processing with parallel batch operations
- Better utilization of available RPM capacity
- Predictable performance characteristics
- Self-regulating system that respects rate limits
- Simpler mental model (consumption tracking vs. state management)
- Automatic capacity reclamation via TTL

### Negative
- Requires DynamoDB query on every reserve attempt
- Slightly higher DynamoDB read costs during bursts
- Consumption records accumulate (mitigated by 5-minute TTL)
- Need to track consumption over 60-second sliding window

### Risks & Mitigations
- **Risk:** DynamoDB query latency on reserve attempts
  - **Mitigation:** Queries are fast (<10ms) with proper indexing; consistent reads used only for verification after write
  
- **Risk:** DynamoDB throttling during bursts
  - **Mitigation:** Use on-demand billing (PAY_PER_REQUEST mode)
  
- **Risk:** Lambda concurrency limits during 50-request burst
  - **Mitigation:** Set reserved concurrency for Lambda functions

- **Risk:** Performance analysis may be optimistic due to global regeneration rate
  - **Issue:** The regeneration logic uses a global rate (1.667 tokens/sec) but should use proportional rates based on capacity allocation (Budget Manager: 0.833 tokens/sec, Queue: 0.667 tokens/sec)
  - **Impact:** Actual throughput may be lower than projected; components will exhaust capacity faster than calculated
  - **Mitigation:** Update `calculate_available_tokens()` to accept a capacity_percentage parameter and multiply refill_rate accordingly
  
- **Risk:** Race conditions when multiple requests check capacity simultaneously
  - **Mitigation:** Optimistic write/query pattern with self-correction; 10-token buffer absorbs edge cases where 1-2 extra requests slip through; worst case is temporary over-consumption of 1-2 tokens
  
- **Risk:** Delete or enqueue failures during rollback (< 0.01% probability)
  - **Mitigation:** Return 500 error to client; client retries; orphaned records cleaned up by TTL after 5 minutes

## Future Enhancements

1. **Dynamic burst capacity:** Adjust burst capacity based on historical usage patterns
2. **Multi-model support:** Different burst/batch configurations per model type
3. **Metrics & monitoring:** CloudWatch metrics for capacity utilization, queue depth, processing latency
4. **Query optimization:** Consider caching recent consumption counts with short TTL
5. **Queue processor optimization:** Implement smart scheduling and batch processing (separate ADR)

## References
- ADR-001: Phased Architecture Approach
- ADR-002: MVP Implementation and Beyond
- Token Bucket Algorithm: https://en.wikipedia.org/wiki/Token_bucket
- DynamoDB TTL: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
