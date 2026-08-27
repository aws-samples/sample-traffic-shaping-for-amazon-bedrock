# Queue Processor Trigger Improvements

## Overview

This document outlines the improved queue processor architecture with heartbeat-based locks, multiple processor support, and intelligent triggering strategies.

## System Goals

1. **Continuous Processing:** During high load, ensure queue is always being processed with minimal gaps
2. **Zero Cost Idle:** During low traffic, no processors running (no wasted Lambda invocations)
3. **Self-Healing:** Automatically recover from processor failures without manual intervention
4. **Scalable:** Support multiple concurrent processors (configurable, start with 1)
5. **Race-Condition Free:** Handle edge cases where items are enqueued during processor shutdown

## Configuration

```python
MAX_QUEUE_PROCESSORS = 1  # Start with 1, increase to 2-3 for higher throughput
LOCK_HEARTBEAT_INTERVAL = 30  # Seconds between heartbeat refreshes
LOCK_TTL = 120  # 2 minutes - lock expires if not refreshed
MAX_PROCESSOR_RUNTIME = 13 * 60  # 13 minutes (leave 2 min buffer for 15 min Lambda timeout)
```

## Architecture Components

### 1. Budget Manager: Smart Triggering

**Responsibility:** Ensure adequate processors are running whenever items are enqueued.

**Strategy:**
- On every enqueue, check how many processors are currently active
- Active = lock exists with non-expired TTL
- If active count < MAX_QUEUE_PROCESSORS, trigger additional processor(s)
- Detect and replace stale locks (TTL expired but lock still exists)

**Key Behavior:**
- Always attempts to maintain MAX_QUEUE_PROCESSORS running during load
- Idempotent: Multiple Budget Managers can trigger simultaneously (lock acquisition handles deduplication)
- Efficient: Single DynamoDB query to count active locks

### 2. Queue Processor: Heartbeat-Based Locks

**Responsibility:** Process queue items while maintaining lock ownership via heartbeat.

**Lock Strategy:**
- Multiple lock slots: `lock#queue-processor#0`, `lock#queue-processor#1`, etc.
- Each processor acquires first available slot
- Heartbeat refreshes lock TTL every 30 seconds
- If heartbeat fails (another processor took over), exit immediately

**Exit Conditions:**
1. Queue is empty (after double-check)
2. Lost lock ownership (heartbeat refresh failed)
3. Approaching Lambda timeout (13 minutes elapsed)

**Processing Strategy:**
- Claim items atomically (prevents duplicate work across multiple processors)
- Check queue capacity before processing
- Re-enqueue items if capacity exhausted
- Double-check queue depth before exit (catches race conditions)

### 3. Item Claiming

**Responsibility:** Ensure only one processor handles each queue item.

**Strategy:**
- Atomic claim via conditional write: `SET claimed_by = :processor_id WHERE attribute_not_exists(claimed_by)`
- Multiple processors can claim different items concurrently
- FIFO ordering preserved (oldest items claimed first)
- Claimed items are deleted immediately (simplest approach for MVP)

## Processing Flow Pseudo-Code

### Budget Manager Flow

```
function enqueue_request(request):
    # 1. Add item to queue
    queue_table.put_item(request)
    
    # 2. Ensure processors running
    ensure_processors_running()

function ensure_processors_running():
    # Count active processors (non-expired locks)
    active_locks = query_locks_with_ttl_not_expired()
    active_count = count(active_locks)
    
    # Check for stale locks (TTL expired but record exists)
    stale_locks = query_locks_with_ttl_expired()
    
    # Trigger replacements for stale locks
    for each stale_lock:
        trigger_queue_processor()
    
    # Trigger additional processors if below max
    needed = MAX_QUEUE_PROCESSORS - active_count
    for i in range(needed):
        trigger_queue_processor()
```

### Queue Processor Flow

```
function handler(event, context):
    # Try to acquire available lock slot (0 to MAX-1)
    lock_id = acquire_available_lock(context.request_id)
    
    if not lock_id:
        log("All processor slots occupied")
        return {status: "all_slots_occupied"}
    
    log("Acquired lock: " + lock_id)
    
    try:
        start_time = now()
        last_heartbeat = now()
        processed_count = 0
        
        # Main processing loop
        while (now() - start_time < MAX_PROCESSOR_RUNTIME):
            
            # Heartbeat: Refresh lock ownership every 30 seconds
            if (now() - last_heartbeat > LOCK_HEARTBEAT_INTERVAL):
                if not refresh_lock_heartbeat(lock_id, context.request_id):
                    log("Lost lock ownership, exiting")
                    return {status: "lock_lost"}
                last_heartbeat = now()
            
            # Try to claim next item
            item = claim_next_item(context.request_id)
            
            if not item:
                log("No unclaimed items, exiting")
                break  # Exit condition: queue empty
            
            # Check if we have queue capacity
            if not has_queue_capacity():
                wait_seconds = calculate_wait_time_for_capacity()
                
                if wait_seconds <= 30:
                    # Short wait - sleep and retry
                    sleep(wait_seconds)
                    re_enqueue_item(item)  # Put back in queue
                    continue
                else:
                    # Long wait - re-enqueue and exit
                    re_enqueue_item(item)
                    break
            
            # Process item (send Step Functions callback)
            process_item(item)
            processed_count++
        
        # Exit condition: timeout approaching
        if (now() - start_time >= MAX_PROCESSOR_RUNTIME):
            log("Max runtime reached, exiting")
        
        return {status: "complete", processed: processed_count}
    
    finally:
        # Always release lock
        release_lock(lock_id)
        
        # CRITICAL: Double-check queue depth
        # Catches items enqueued during shutdown (race condition)
        final_queue_depth = get_queue_depth()
        
        if final_queue_depth > 0:
            log("Items remain after shutdown, triggering continuation")
            trigger_queue_processor()
```

### Lock Acquisition Flow

```
function acquire_available_lock(processor_id):
    # Try each slot (0 to MAX_QUEUE_PROCESSORS-1)
    for slot in range(MAX_QUEUE_PROCESSORS):
        lock_id = "lock#queue-processor#" + slot
        
        try:
            # Try to create/overwrite lock
            table.put_item(
                item = {
                    semaphore_id: lock_id,
                    is_locked: true,
                    processor_id: processor_id,
                    slot: slot,
                    locked_at: now(),
                    last_heartbeat: now(),
                    ttl: now() + LOCK_TTL
                },
                condition = "attribute_not_exists(semaphore_id) OR ttl < now()"
            )
            
            log("Acquired lock slot " + slot)
            return lock_id
            
        catch ConditionalCheckFailed:
            # Slot occupied by active processor, try next
            continue
    
    # All slots occupied
    return null
```

### Heartbeat Refresh Flow

```
function refresh_lock_heartbeat(lock_id, processor_id):
    try:
        table.update_item(
            key = {semaphore_id: lock_id},
            update = "SET last_heartbeat = :now, ttl = :ttl",
            condition = "processor_id = :pid",
            values = {
                now: now(),
                ttl: now() + LOCK_TTL,
                pid: processor_id
            }
        )
        
        log("Refreshed heartbeat: " + lock_id)
        return true
        
    catch ConditionalCheckFailed:
        log("Lock taken over by another processor")
        return false  # Lost ownership
```

### Item Claiming Flow

```
function claim_next_item(processor_id):
    max_retries = 5
    
    for attempt in range(max_retries):
        # Query for unclaimed items (FIFO order)
        items = queue_table.query(
            condition = "semaphore_id = :sid",
            filter = "attribute_not_exists(claimed_by)",
            limit = 10,  # Get multiple candidates
            order = ascending  # Oldest first
        )
        
        if items.empty():
            return null
        
        item = items[0]  # Try oldest item
        
        try:
            # Atomically claim item
            queue_table.update_item(
                key = {semaphore_id: item.semaphore_id, sort_key: item.sort_key},
                update = "SET claimed_by = :pid, claimed_at = :now",
                condition = "attribute_not_exists(claimed_by)",
                values = {pid: processor_id, now: now()}
            )
            
            # Successfully claimed - delete from queue
            queue_table.delete_item(
                key = {semaphore_id: item.semaphore_id, sort_key: item.sort_key}
            )
            
            log("Claimed and dequeued: " + item.request_id)
            return item
            
        catch ConditionalCheckFailed:
            # Another processor claimed it, try next item
            continue
    
    log("Failed to claim any items after retries")
    return null
```

## Key Design Decisions

### 1. Heartbeat-Based Lock Ownership

**Why:** Allows processors to run for extended periods (13 minutes) while proving they're alive.

**How:** 
- Lock TTL = 2 minutes
- Heartbeat refresh every 30 seconds
- If processor crashes, lock expires after 2 minutes max
- New processor can overwrite expired lock

### 2. Multiple Lock Slots

**Why:** Enables horizontal scaling to multiple concurrent processors.

**How:**
- Lock IDs: `lock#queue-processor#0`, `lock#queue-processor#1`, etc.
- Each processor tries slots in order until one succeeds
- Budget Manager counts active locks to determine if more processors needed

### 3. Item-Level Claiming

**Why:** Prevents duplicate work when multiple processors run concurrently.

**How:**
- Atomic conditional write: only succeeds if item unclaimed
- Multiple processors can claim different items simultaneously
- FIFO ordering preserved (oldest items claimed first)

### 4. Double-Check on Exit

**Why:** Eliminates race condition where items are enqueued during processor shutdown.

**How:**
- After releasing lock, check queue depth one final time
- If items exist, trigger new processor before exiting
- Ensures no gaps in processing

### 5. Budget Manager Always Triggers

**Why:** Simplest approach that handles all edge cases.

**How:**
- On every enqueue, check active processor count
- Trigger if below MAX_QUEUE_PROCESSORS
- Lock acquisition is idempotent (handles concurrent triggers)
- Cost is negligible (EventBridge PutEvents is free for first 1M/month)

## Behavior Examples

### Scenario 1: Low Traffic (Single Processor Sufficient)

```
T=0s:   Request arrives
T=0s:   Budget Manager enqueues, sees 0 active processors, triggers
T=0.1s: Processor A starts, acquires lock#0
T=0.2s: Processor A claims and processes item
T=0.3s: Processor A checks queue: empty
T=0.4s: Processor A releases lock, double-checks: still empty, exits

T=10s:  New request arrives
T=10s:  Budget Manager enqueues, sees 0 active processors, triggers
T=10.1s: Processor B starts, acquires lock#0
... cycle repeats
```

### Scenario 2: High Traffic (Multiple Processors)

```
T=0s:   100 requests arrive
T=0s:   Budget Manager enqueues, sees 0 active, triggers 2 processors
T=0.1s: Processor A acquires lock#0
T=0.2s: Processor B acquires lock#1
T=0.3s: Both claim items independently (A claims item#1, B claims item#2)
T=0.4s: Both process in parallel
T=30s:  Both refresh heartbeats
T=60s:  Queue empties
T=60.1s: Both exit after double-check
```

### Scenario 3: Processor Crash (Heartbeat Failure)

```
T=0s:   Processor A running with lock#0
T=30s:  Processor A refreshes heartbeat (ttl = T+150s)
T=45s:  Processor A crashes (Lambda timeout, OOM, etc.)
T=60s:  New request arrives
T=60s:  Budget Manager sees 1 active lock (not expired yet), doesn't trigger
T=150s: Lock#0 TTL expires
T=151s: New request arrives
T=151s: Budget Manager sees 0 active locks (lock#0 expired), triggers
T=151.1s: Processor B acquires lock#0 (overwrites expired lock)
```

**Max recovery time: 2 minutes (LOCK_TTL)**

### Scenario 4: Race Condition (Enqueue During Shutdown)

```
T=0s:   Processor A processing last item
T=0.1s: Processor A claims and processes item
T=0.2s: Processor A checks queue: empty
T=0.3s: Processor A enters finally block
T=0.4s: Budget Manager enqueues new item
T=0.5s: Budget Manager sees 1 active lock, doesn't trigger
T=0.6s: Processor A releases lock
T=0.7s: Processor A double-checks queue: 1 item found!
T=0.8s: Processor A triggers new processor
T=0.9s: Processor A exits
T=1.0s: Processor B starts, processes item
```

**Race condition eliminated by double-check pattern**

## Migration Path

### Phase 1: Single Processor with Heartbeat (Current Focus)
- Implement heartbeat-based locks
- Budget Manager triggers on enqueue
- Single processor (MAX = 1)
- Item claiming logic
- Double-check on exit

### Phase 2: Multiple Processors (Future)
- Increase MAX_QUEUE_PROCESSORS to 2-3
- Test concurrent processor behavior
- Monitor for contention issues

### Phase 3: Dynamic Scaling (Future)
- Budget Manager scales based on queue depth
- Heuristic: 1 processor per 50 items
- Auto-scale up during spikes, down during idle
