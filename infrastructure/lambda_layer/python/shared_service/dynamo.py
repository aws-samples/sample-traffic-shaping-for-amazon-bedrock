"""Shared DynamoDB Service Layer - Leaky Bucket Implementation"""

import time
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from boto3.dynamodb.conditions import Key
from decimal import Decimal

# Lock configuration constants
LOCK_TTL = 120  # 2 minutes - lock expires if not refreshed
LOCK_HEARTBEAT_INTERVAL = 30  # seconds between heartbeat refreshes

# Token estimation constants
DEFAULT_BYTES_PER_TOKEN = 4.0  # Conservative default for most models
CLAUDE_BYTES_PER_TOKEN = 3.5   # Claude models use ~3.5 bytes per token
SAFETY_MARGIN = 1.1            # 10% over-estimation for rate limiting safety


class BurstCapacityExceeded(Exception):
    """Raised when atomic burst admission gate rejects the request."""
    pass


def estimate_request_tokens(
    prompt: Optional[str],
    max_tokens: int = 100,
    burndown_rate: float = 1.0,
    bytes_per_token: float = DEFAULT_BYTES_PER_TOKEN
) -> int:
    """
    Estimate TPM cost of a request, matching Bedrock's upfront deduction model.

    Uses byte-length (UTF-8) instead of char-length for accurate multi-byte
    character handling (CJK, emoji, etc.). Includes 10% safety margin since
    over-estimation is preferable to under-estimation for rate limiting.

    At request start, Bedrock deducts: input_tokens + (max_tokens * burndown_rate)
    For Claude 3.7+: burndown_rate = 5 (output tokens cost 5x)
    For all other models: burndown_rate = 1

    Args:
        prompt: The input prompt text (None/empty = 0 input tokens)
        max_tokens: Maximum output tokens requested
        burndown_rate: Output token multiplier (5 for Claude 3.7+, 1 for others)
        bytes_per_token: Bytes per token ratio (3.5 for Claude, 4.0 default)

    Returns:
        Estimated total TPM tokens consumed by this request
    """
    if not prompt:
        input_tokens = 0
    else:
        byte_length = len(prompt.encode('utf-8'))
        input_tokens = max(1, int(byte_length / bytes_per_token * SAFETY_MARGIN))

    output_token_cost = int(max_tokens * burndown_rate)
    return input_tokens + output_token_cost


def estimate_request_tokens_split(
    prompt: Optional[str],
    max_tokens: int = 100,
    bytes_per_token: float = DEFAULT_BYTES_PER_TOKEN,
) -> tuple:
    """
    Estimate (input_tokens, output_tokens) separately for the mantle iTPM/oTPM
    split-quota model.

    On bedrock-mantle, burndown does NOT apply to output estimation — oTPM is
    gated as its own independent quota — so the output estimate is a 1:1 ceiling
    on max_tokens. The input estimate uses the same UTF-8-byte heuristic +10%
    safety margin as estimate_request_tokens() so the two functions agree on the
    input dimension.

    Pre-call admission uses these estimates; bedrock_processor reconciles them to
    the actual usage.input_tokens / usage.output_tokens returned by mantle.

    Args:
        prompt: The input prompt text (None/empty = 0 input tokens)
        max_tokens: Maximum output tokens requested (1:1 oTPM estimate)
        bytes_per_token: Bytes per token ratio (3.5 for Claude, 4.0 default)

    Returns:
        (input_tokens, output_tokens) tuple of ints
    """
    if not prompt:
        input_tokens = 0
    else:
        byte_length = len(prompt.encode('utf-8'))
        input_tokens = max(1, int(byte_length / bytes_per_token * SAFETY_MARGIN))

    output_tokens = int(max_tokens)
    return (input_tokens, output_tokens)


class DynamoService:
    """Shared service for DynamoDB operations using leaky bucket pattern."""

    def __init__(self, single_table_name: str):
        """Initialize DynamoDB resources."""
        self.dynamodb = boto3.resource('dynamodb')
        self.single_table = self.dynamodb.Table(single_table_name)

    # === Configuration Methods ===

    def get_model_config(self, model_id: str) -> Dict[str, Any]:
        """
        Get token allocation config for a model.

        Returns:
            Config with burst_capacity, rpm_limit, regeneration rates
        Raises:
            KeyError if model config not found
        """
        response = self.single_table.get_item(
            Key={'pk': f'MODEL#{model_id}', 'sk': 'CONFIG'}
        )

        if 'Item' not in response:
            raise KeyError(f"Model config not found: {model_id}")

        return response['Item']

    # === Burst Allocation Methods (Budget Manager) ===

    # Sliding-window admission horizons (docs/solution/architecture.md §3).
    #   SHORT — rate smoothing (keeps dispatch under Bedrock's sub-minute bucket)
    #   LONG  — accuracy horizon (long enough that reconciled ACTUALS dominate the
    #           window; Bedrock latency ~7.5s < 15s). The LONG horizon is also the
    #           correctness horizon: any over-admission/crash drift rolls out within
    #           its length, so no reconciliation pass is needed.
    SHORT_WINDOW_SECONDS = 2
    LONG_WINDOW_SECONDS = 15

    def put_allocation(self, model_id: str, request_id: str, estimated_tokens: int = 0,
                       correlation_id: Optional[str] = None,
                       burst_capacity: int = 0, burst_regen_rate: float = 0.0,
                       max_burst_multiplier: float = 2.0,
                       counter_shards: int = 1,
                       tpm_burst_capacity: int = 0,
                       tpm_burst_regen_rate: float = 0.0,
                       backend: str = 'runtime',
                       rpm_quota_enabled: bool = True,
                       estimated_input_tokens: int = 0,
                       estimated_output_tokens: int = 0,
                       itpm_burst_capacity: int = 0, itpm_burst_regen_rate: float = 0.0,
                       otpm_burst_capacity: int = 0, otpm_burst_regen_rate: float = 0.0,
                       short_window_rps: float = 0.0,
                       short_window_sec: float = 0.0,
                       long_window_sec: float = 0.0) -> Dict[str, Any]:
        """
        Admit or reject a request via a CONSUMPTION-RECORD SLIDING-WINDOW READ.

        This is the sliding-window admission gate (docs/solution/architecture.md §3):
        the consumption records are the single source of truth. Admission is a READ
        over recent consumption, NOT an increment of a derived counter. There are no
        counter items, no TransactWriteItems, and no reconciliation Lambda — the
        counters were the DynamoDB single-item hotspot that capped burst throughput.

        RUNTIME backend (default): strongly-consistent query of the last
        LONG_WINDOW_SECONDS of MODEL#{m}#BURST#CONSUMPTION. Admit iff the request's
        estimate, added to recent consumption, fits BOTH windows:
            2s  cap: tpm_burst_regen_rate*2  tokens  AND  short_window_rps*2  reqs
            15s cap: tpm_burst_regen_rate*15 tokens  AND  short_window_rps*15 reqs
        On admit, write the consumption record with the estimate via a single
        put_item. On reject raise BurstCapacityExceeded (the caller enqueues).

        MANTLE backend (backend='mantle'): same window read, but over the split
        iTPM/oTPM token sums (estimated_input_tokens / estimated_output_tokens) with
        each dimension gated independently.

        Correctness contract: this is a SHAPER, not a hard transactional semaphore.
        We accept BOUNDED over-admission — concurrent admits may each read the window
        before the others' writes commit. Overflow is caught downstream by
        requeue-on-throttle (bedrock_processor), not by exact gating. The window
        horizon self-heals: any drift ages out within LONG_WINDOW_SECONDS.

        Args:
            model_id: Bedrock model ID
            request_id: Unique request identifier
            estimated_tokens: Combined TPM cost (runtime) / input+output sum (mantle)
            correlation_id: Optional ID to trace requests across all 3 Lambdas
            burst_capacity: RPM burst capacity (>0 enables the gate; sentinel when
                            rpm_quota_enabled is False)
            burst_regen_rate: RPM burst regeneration rate (req/sec) — the RPS cap source
            tpm_burst_regen_rate: TPM regeneration rate (tokens/sec) — the TPS cap source
            backend: 'runtime' (default) or 'mantle' — selects combined vs split tokens
            rpm_quota_enabled: When False, the request-count dimension is skipped
            short_window_rps: Explicit 2s/15s request-rate source; falls back to
                              burst_regen_rate when 0
            estimated_input_tokens / estimated_output_tokens: split token cost (mantle)
            itpm_burst_regen_rate / otpm_burst_regen_rate: split TPS cap sources (mantle)
            short_window_sec / long_window_sec: window horizons (0 ⇒ class defaults 2/15)
            max_burst_multiplier / counter_shards / *_burst_capacity: accepted for
                              signature compatibility; unused by the window-read gate.

        Returns:
            Dict with timestamp_ms, timestamp, item

        Raises:
            BurstCapacityExceeded: When the sliding-window gate rejects the request
        """
        now = time.time()
        now_ms = int(now * 1000)

        item = {
            'pk': f'MODEL#{model_id}#BURST#CONSUMPTION',
            'sk': f'{now_ms}#{request_id}',  # request_id ensures uniqueness
            'entity_type': 'burst_consumption_token',
            'request_id': request_id,
            'count': 1,
            'estimated_tokens': estimated_tokens,
            'source': 'burst',
            'consumed_at': datetime.fromtimestamp(now).isoformat(),
            'ttl': int(now) + 60  # 60s retention — only needs to outlive the 15s window
        }

        if correlation_id:
            item['correlation_id'] = correlation_id

        # Mantle carries split token fields on the consumption record so iTPM/oTPM
        # windows can be summed independently and reconciled to actuals later.
        if backend == 'mantle':
            item['estimated_input_tokens'] = estimated_input_tokens
            item['estimated_output_tokens'] = estimated_output_tokens
            item['backend'] = 'mantle'

        short_w = short_window_sec if short_window_sec > 0 else self.SHORT_WINDOW_SECONDS
        long_w = long_window_sec if long_window_sec > 0 else self.LONG_WINDOW_SECONDS

        if backend == 'mantle' and (itpm_burst_capacity > 0 or otpm_burst_capacity > 0):
            return self._put_allocation_mantle(
                model_id=model_id, request_id=request_id, item=item, now=now, now_ms=now_ms,
                rpm_quota_enabled=rpm_quota_enabled,
                burst_regen_rate=burst_regen_rate,
                estimated_input_tokens=estimated_input_tokens,
                estimated_output_tokens=estimated_output_tokens,
                itpm_burst_regen_rate=itpm_burst_regen_rate,
                otpm_burst_regen_rate=otpm_burst_regen_rate,
                short_window_rps=short_window_rps,
                short_w=short_w, long_w=long_w,
            )

        if burst_capacity > 0:
            # Atomic admission gate: TransactWriteItems with counter condition
            # ── SLIDING-WINDOW READ GATE (runtime) ─────────────────────────────
            # One strongly-consistent read of the last `long_w` seconds of
            # consumption records is the ENTIRE gate. No counters, no transaction.
            # Admit iff the request's estimate fits BOTH the short (rate-smoothing)
            # and long (accuracy) windows on tokens AND requests, then write the
            # record with a single put_item. This is the fix for the counter-item
            # contention that pinned burst throughput far below its budget.
            effective_short_rps = short_window_rps if short_window_rps > 0 else burst_regen_rate
            cap_short_req = max(1, int(effective_short_rps * short_w)) if effective_short_rps > 0 else 0
            cap_long_req = max(1, int(effective_short_rps * long_w)) if effective_short_rps > 0 else 0
            cap_short_tok = int(tpm_burst_regen_rate * short_w) if tpm_burst_regen_rate > 0 else 0
            cap_long_tok = int(tpm_burst_regen_rate * long_w) if tpm_burst_regen_rate > 0 else 0
            gate_tpm = tpm_burst_regen_rate > 0 and estimated_tokens > 0

            # Oversized-request pre-check: a request that cannot fit even an empty
            # long window can never be admitted — reject up front so the caller
            # enqueues it (parity with the old gate's oversized guard).
            if gate_tpm and cap_long_tok > 0 and int(estimated_tokens) > cap_long_tok:
                raise BurstCapacityExceeded(
                    f"Single-request token cost exceeds long-window TPM cap for {model_id}: "
                    f"estimated_tokens={estimated_tokens}, cap_long_tok={cap_long_tok}, "
                    f"long_window_sec={long_w}"
                )

            self._enforce_window_gate(
                model_id,
                long_window_sec=long_w,
                short_window_sec=short_w,
                token_field='estimated_tokens',  # nosec B106  # 'token_field' names a DynamoDB attribute, not a credential
                est_tokens=estimated_tokens if gate_tpm else 0,
                cap_short_tok=cap_short_tok if gate_tpm else 0,
                cap_long_tok=cap_long_tok if gate_tpm else 0,
                cap_short_req=cap_short_req if rpm_quota_enabled else 0,
                cap_long_req=cap_long_req if rpm_quota_enabled else 0,
            )

            # ── ADMIT: write the consumption record with the ESTIMATE ──────────
            # bedrock_processor reconciles this to actuals ~7.5s later.
            self.single_table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(pk) AND attribute_not_exists(sk)'
            )
            return {
                'timestamp_ms': now_ms,
                'timestamp': now,
                'item': item
            }
        else:
            # burst_capacity <= 0 ⇒ BURST DISABLED: reject every request so it all
            # queues. This is the "0% burst" configuration — used to prove the queue
            # path in isolation (no immediate-path traffic sharing the quota).
            #
            # NOTE: this REPLACES a former legacy admit-all passthrough (a plain
            # put_item with no gating). That old behavior made burst_capacity=0 mean
            # "admit EVERYTHING", the opposite of intent and a documented footgun —
            # the config workaround was to set burst to ~1% rather than 0. Raising
            # here makes 0% burst safe: the caller catches BurstCapacityExceeded and
            # enqueues, so nothing bypasses the queue.
            raise BurstCapacityExceeded(
                f"Burst disabled (burst_capacity={burst_capacity}) for {model_id}: "
                f"routing request to queue"
            )

    def _put_allocation_mantle(self, *, model_id: str, request_id: str, item: Dict[str, Any],
                               now: float, now_ms: int,
                               rpm_quota_enabled: bool,
                               burst_regen_rate: float,
                               estimated_input_tokens: int, estimated_output_tokens: int,
                               itpm_burst_regen_rate: float,
                               otpm_burst_regen_rate: float,
                               short_window_rps: float = 0.0,
                               short_w: float = 2.0, long_w: float = 15.0) -> Dict[str, Any]:
        """
        Mantle SLIDING-WINDOW READ gate (iTPM + oTPM, RPM optional).

        Same model as the runtime window read, but the token dimensions are the
        split iTPM/oTPM sums instead of the combined TPM. One strongly-consistent
        read of the last `long_w` seconds of BURST#CONSUMPTION is the whole gate:
        admit iff the request's input estimate fits both windows on iTPM AND its
        output estimate fits both windows on oTPM (AND, when rpm_quota_enabled, the
        request count fits). No counters, no transaction.

        Bounded over-admission is accepted and caught downstream by
        requeue-on-throttle; the 15s horizon self-heals any drift.
        """
        effective_short_rps = short_window_rps
        if effective_short_rps <= 0 and rpm_quota_enabled and burst_regen_rate > 0:
            effective_short_rps = burst_regen_rate
        cap_short_req = max(1, int(effective_short_rps * short_w)) if effective_short_rps > 0 else 0
        cap_long_req = max(1, int(effective_short_rps * long_w)) if effective_short_rps > 0 else 0

        cap_short_itok = int(itpm_burst_regen_rate * short_w) if itpm_burst_regen_rate > 0 else 0
        cap_long_itok = int(itpm_burst_regen_rate * long_w) if itpm_burst_regen_rate > 0 else 0
        cap_short_otok = int(otpm_burst_regen_rate * short_w) if otpm_burst_regen_rate > 0 else 0
        cap_long_otok = int(otpm_burst_regen_rate * long_w) if otpm_burst_regen_rate > 0 else 0

        gate_i = itpm_burst_regen_rate > 0 and estimated_input_tokens > 0
        gate_o = otpm_burst_regen_rate > 0 and estimated_output_tokens > 0

        # Oversized-request pre-check on either dimension.
        if gate_i and cap_long_itok > 0 and int(estimated_input_tokens) > cap_long_itok:
            raise BurstCapacityExceeded(
                f"Mantle single-request iTPM exceeds long-window cap for {model_id}: "
                f"est_input={int(estimated_input_tokens)}, cap_long_itok={cap_long_itok}"
            )
        if gate_o and cap_long_otok > 0 and int(estimated_output_tokens) > cap_long_otok:
            raise BurstCapacityExceeded(
                f"Mantle single-request oTPM exceeds long-window cap for {model_id}: "
                f"est_output={int(estimated_output_tokens)}, cap_long_otok={cap_long_otok}"
            )

        self._enforce_window_gate(
            model_id,
            long_window_sec=long_w,
            short_window_sec=short_w,
            # Request-count dimension (optional on mantle).
            cap_short_req=cap_short_req if rpm_quota_enabled else 0,
            cap_long_req=cap_long_req if rpm_quota_enabled else 0,
            # Input-token dimension.
            token_field='estimated_input_tokens',  # nosec B106  # 'token_field' names a DynamoDB attribute, not a credential
            est_tokens=estimated_input_tokens if gate_i else 0,
            cap_short_tok=cap_short_itok if gate_i else 0,
            cap_long_tok=cap_long_itok if gate_i else 0,
            # Output-token dimension (second, independent gate).
            token_field2='estimated_output_tokens',
            est_tokens2=estimated_output_tokens if gate_o else 0,
            cap_short_tok2=cap_short_otok if gate_o else 0,
            cap_long_tok2=cap_long_otok if gate_o else 0,
        )

        # ADMIT: write the consumption record with the split estimates.
        self.single_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(pk) AND attribute_not_exists(sk)'
        )
        return {
            'timestamp_ms': now_ms,
            'timestamp': now,
            'item': item
        }

    def _enforce_window_gate(
        self,
        model_id: str,
        *,
        long_window_sec: float,
        short_window_sec: float,
        cap_short_req: int = 0,
        cap_long_req: int = 0,
        token_field: str = 'estimated_tokens',
        est_tokens: int = 0,
        cap_short_tok: int = 0,
        cap_long_tok: int = 0,
        token_field2: Optional[str] = None,
        est_tokens2: int = 0,
        cap_short_tok2: int = 0,
        cap_long_tok2: int = 0,
    ) -> None:
        """
        The sliding-window admission read shared by the runtime and mantle gates.

        ONE strongly-consistent query of the last `long_window_sec` seconds of the
        BURST#CONSUMPTION partition is the entire gate. Over that single result set
        we compute request counts and token sums for BOTH the short (rate-smoothing)
        and long (accuracy) horizons, then admit iff the incoming request fits every
        active cap. On any breach we raise BurstCapacityExceeded (the caller
        enqueues). No counters, no transaction — the read replaces both.

        The strongly-consistent read means a Lambda sees its own recent writes, but
        NOT concurrent uncommitted admits: that read-modify-write race is the
        bounded over-admission the shaper accepts (future-state doc §1), caught
        downstream by requeue-on-throttle.

        Args:
            long_window_sec / short_window_sec: horizons (15s / 2s)
            cap_short_req / cap_long_req: request-count caps (0 ⇒ dimension off)
            token_field / est_tokens / cap_*_tok: primary token dimension
                (runtime: estimated_tokens; mantle: estimated_input_tokens)
            token_field2 / est_tokens2 / cap_*_tok2: optional second token dimension
                (mantle oTPM); None ⇒ single-dimension (runtime)

        Raises:
            BurstCapacityExceeded: if the request would breach any active cap.
        """
        gate_req = cap_short_req > 0 or cap_long_req > 0
        gate_tok = est_tokens > 0 and (cap_short_tok > 0 or cap_long_tok > 0)
        gate_tok2 = (token_field2 is not None and est_tokens2 > 0
                     and (cap_short_tok2 > 0 or cap_long_tok2 > 0))
        if not (gate_req or gate_tok or gate_tok2):
            return  # nothing to enforce (e.g. RPM-only model with no request cap)

        # Strongly-consistent read of the full long window — the single source of
        # truth. This is the ONE read that replaced the counter transaction.
        records = self.query_consumption_records(
            model_id, 'BURST',
            window_seconds=int(long_window_sec), consistent_read=True,
        )

        now_ms = int(time.time() * 1000)
        short_cutoff_ms = now_ms - int(short_window_sec * 1000)

        req_short = req_long = 0
        tok_short = tok_long = 0
        tok2_short = tok2_long = 0
        for rec in records:
            try:
                rec_ms = int(str(rec['sk']).split('#')[0])
            except (KeyError, ValueError, IndexError):
                continue
            in_short = rec_ms >= short_cutoff_ms
            req_long += 1
            if in_short:
                req_short += 1
            if gate_tok:
                v = rec.get(token_field, 0)
                v = int(v) if isinstance(v, Decimal) else int(v or 0)
                tok_long += v
                if in_short:
                    tok_short += v
            if gate_tok2:
                v2 = rec.get(token_field2, 0)
                v2 = int(v2) if isinstance(v2, Decimal) else int(v2 or 0)
                tok2_long += v2
                if in_short:
                    tok2_short += v2

        # Request-count caps.
        if cap_short_req > 0 and req_short + 1 > cap_short_req:
            raise BurstCapacityExceeded(
                f"2s request-rate cap exceeded for {model_id}: "
                f"count={req_short}, cap={cap_short_req}"
            )
        if cap_long_req > 0 and req_long + 1 > cap_long_req:
            raise BurstCapacityExceeded(
                f"{long_window_sec:.0f}s request cap exceeded for {model_id}: "
                f"count={req_long}, cap={cap_long_req}"
            )

        # Primary token dimension.
        if gate_tok:
            if cap_short_tok > 0 and tok_short + est_tokens > cap_short_tok:
                raise BurstCapacityExceeded(
                    f"2s token-rate cap exceeded for {model_id} ({token_field}): "
                    f"tokens_in_window={tok_short}, est={est_tokens}, cap={cap_short_tok}"
                )
            if cap_long_tok > 0 and tok_long + est_tokens > cap_long_tok:
                raise BurstCapacityExceeded(
                    f"{long_window_sec:.0f}s token cap exceeded for {model_id} ({token_field}): "
                    f"tokens_in_window={tok_long}, est={est_tokens}, cap={cap_long_tok}"
                )

        # Second token dimension (mantle oTPM).
        if gate_tok2:
            if cap_short_tok2 > 0 and tok2_short + est_tokens2 > cap_short_tok2:
                raise BurstCapacityExceeded(
                    f"2s token-rate cap exceeded for {model_id} ({token_field2}): "
                    f"tokens_in_window={tok2_short}, est={est_tokens2}, cap={cap_short_tok2}"
                )
            if cap_long_tok2 > 0 and tok2_long + est_tokens2 > cap_long_tok2:
                raise BurstCapacityExceeded(
                    f"{long_window_sec:.0f}s token cap exceeded for {model_id} ({token_field2}): "
                    f"tokens_in_window={tok2_long}, est={est_tokens2}, cap={cap_long_tok2}"
                )

    def query_consumption_records(
        self,
        model_id: str,
        capacity_mode: str,
        window_seconds: int = 60,
        consistent_read: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get consumption records in sliding window (Step 2 of write-then-verify).

        Args:
            capacity_mode: 'BURST' or 'QUEUE'
            consistent_read: Use ConsistentRead=True to see own write

        Returns:
            List of consumption records from last N seconds
        """
        now_ms = int(time.time() * 1000)
        window_start_ms = now_ms - (window_seconds * 1000)

        response = self.single_table.query(
            KeyConditionExpression=(
                Key('pk').eq(f'MODEL#{model_id}#{capacity_mode}#CONSUMPTION') &
                Key('sk').between(f'{window_start_ms}#', f'{now_ms}#~')
            ),
            ConsistentRead=consistent_read
        )

        return response.get('Items', [])

    def calculate_available_tokens(
        self,
        capacity: int,
        consumption_records: List[Dict[str, Any]],
        regeneration_rate: float,
        current_time: Optional[float] = None
    ) -> float:
        """
        Calculate available tokens with continuous regeneration.

        Formula: capacity - tokens_consumed + tokens_regenerated

        Args:
            capacity: Total capacity (e.g., 50 for burst)
            consumption_records: Records from query_consumption_records()
            regeneration_rate: Tokens/second (e.g., 0.833 for burst)
            current_time: Current timestamp (defaults to time.time())

        Returns:
            Available tokens (can be negative if over-consumed)
        """
        if not consumption_records:
            return float(capacity)

        current_time = current_time or time.time()

        # Count total tokens consumed (convert Decimal to int if needed)
        tokens_consumed = sum(
            int(record['count']) if isinstance(record['count'], Decimal) else record['count']
            for record in consumption_records
        )

        # Calculate per-record regeneration: each record regenerates based on its own age
        tokens_regenerated = sum(
            (current_time - int(record['sk'].split('#')[0]) / 1000.0) * regeneration_rate
            for record in consumption_records
        )
        # Can't regenerate more tokens than were consumed
        tokens_regenerated = min(tokens_regenerated, tokens_consumed)

        # Calculate available tokens
        available_tokens = capacity - tokens_consumed + tokens_regenerated

        return available_tokens

    def calculate_available_tpm(
        self,
        tpm_capacity: int,
        consumption_records: List[Dict[str, Any]],
        tpm_regeneration_rate: float,
        current_time: Optional[float] = None
    ) -> float:
        """
        Calculate available TPM tokens using a bucket-level regeneration model.

        Unlike calculate_available_tokens() (which uses per-record RPM count), this
        function uses the oldest record's timestamp as the bucket drain anchor. The
        bucket refills at tpm_regeneration_rate tokens/second globally — NOT per
        record. Summing per-record would multiply regen_rate by N (record count),
        causing the bucket to appear full N× sooner than reality under concurrent
        bursts. The oldest-record anchor gives the true time since the drain began.

        Args:
            tpm_capacity: TPM burst or queue capacity (e.g., 2_000_000)
            consumption_records: Records from query_consumption_records()
            tpm_regeneration_rate: TPM tokens/second regeneration rate (bucket-level)
            current_time: Current timestamp (defaults to time.time())

        Returns:
            Available TPM tokens (can be negative if over-consumed)
        """
        if not consumption_records:
            return float(tpm_capacity)

        current_time = current_time or time.time()

        # Sum estimated_tokens (TPM cost) from each record
        tokens_consumed = sum(
            int(record.get('estimated_tokens', 0))
            if isinstance(record.get('estimated_tokens', 0), Decimal)
            else record.get('estimated_tokens', 0)
            for record in consumption_records
        )

        if tokens_consumed == 0:
            return float(tpm_capacity)

        # Token bucket model: the bucket refills at tpm_regeneration_rate tokens/second total.
        # Anchor regeneration on the oldest record's timestamp — this is when the bucket
        # started draining. Per-record summing would multiply regen_rate by N (number of
        # concurrent records), making the bucket appear to refill N× too fast and causing
        # over-admission under thundering herd conditions.
        oldest_record_ms = min(
            int(record['sk'].split('#')[0])
            for record in consumption_records
            if record.get('estimated_tokens', 0)
        )
        time_since_drain_started = current_time - oldest_record_ms / 1000.0
        tokens_regenerated = min(time_since_drain_started * tpm_regeneration_rate, tokens_consumed)

        # Calculate available tokens
        available_tpm = tpm_capacity - tokens_consumed + tokens_regenerated

        return available_tpm

    def calculate_available_split_tpm(
        self,
        tpm_capacity: int,
        consumption_records: List[Dict[str, Any]],
        tpm_regeneration_rate: float,
        dimension: str,
        current_time: Optional[float] = None
    ) -> float:
        """
        Calculate available iTPM or oTPM for the mantle split-quota model.

        Identical bucket-level regeneration math to calculate_available_tpm(), but
        sums the per-dimension token field ('estimated_input_tokens' for INPUT,
        'estimated_output_tokens' for OUTPUT) instead of the combined
        'estimated_tokens'. This lets iTPM and oTPM be gated independently —
        a workload can exhaust oTPM while iTPM has plenty of headroom.

        Args:
            tpm_capacity: iTPM or oTPM burst/queue capacity
            consumption_records: Records from query_consumption_records()
            tpm_regeneration_rate: Tokens/second regeneration (bucket-level)
            dimension: 'INPUT' or 'OUTPUT' — which token field to sum
            current_time: Current timestamp (defaults to time.time())

        Returns:
            Available tokens for the dimension (can be negative if over-consumed)
        """
        if dimension == 'INPUT':
            field_name = 'estimated_input_tokens'
        elif dimension == 'OUTPUT':
            field_name = 'estimated_output_tokens'
        else:
            raise ValueError(f"dimension must be 'INPUT' or 'OUTPUT', got: {dimension}")

        if not consumption_records:
            return float(tpm_capacity)

        current_time = current_time or time.time()

        def _field_val(record):
            v = record.get(field_name, 0)
            return int(v) if isinstance(v, Decimal) else (v or 0)

        tokens_consumed = sum(_field_val(record) for record in consumption_records)

        if tokens_consumed == 0:
            return float(tpm_capacity)

        # Anchor regeneration on the oldest record that consumed this dimension.
        anchor_records = [r for r in consumption_records if _field_val(r)]
        oldest_record_ms = min(int(r['sk'].split('#')[0]) for r in anchor_records)
        time_since_drain_started = current_time - oldest_record_ms / 1000.0
        tokens_regenerated = min(time_since_drain_started * tpm_regeneration_rate, tokens_consumed)

        return tpm_capacity - tokens_consumed + tokens_regenerated

    def delete_consumption_record(
        self,
        model_id: str,
        capacity_mode: str,
        timestamp_ms: int,
        request_id: str
    ) -> None:
        """
        Delete consumption record (rollback for over-consumption).
        Uses condition expression to ensure item exists before deletion.
        """
        self.single_table.delete_item(
            Key={
                'pk': f'MODEL#{model_id}#{capacity_mode}#CONSUMPTION',
                'sk': f'{timestamp_ms}#{request_id}'
            },
            ConditionExpression='attribute_exists(pk) AND attribute_exists(sk)'
        )

    # === Reconciliation Methods ===

    def sweep_orphaned_records(self, model_id: str, capacity_mode: str = 'BURST', max_age_seconds: int = 120) -> int:
        """
        Find and delete consumption records older than max_age_seconds.

        Normal records are within a 60-second sliding window. Records older than
        max_age_seconds are orphans from failed rollbacks (phantom allocations).
        DynamoDB TTL would eventually clean these up (5 min), but this sweep
        reclaims capacity faster.

        Returns: number of orphaned records deleted
        """
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - (max_age_seconds * 1000)

        # Query records older than cutoff (sk < cutoff)
        pk = f'MODEL#{model_id}#{capacity_mode}#CONSUMPTION'
        response = self.single_table.query(
            KeyConditionExpression=(
                Key('pk').eq(pk) &
                Key('sk').lt(f'{cutoff_ms}#')
            ),
            ConsistentRead=True
        )

        orphaned_items = response.get('Items', [])
        if not orphaned_items:
            return 0

        # Batch delete orphaned records
        table_name = self.single_table.table_name
        delete_requests = [
            {'DeleteRequest': {'Key': {'pk': item['pk'], 'sk': item['sk']}}}
            for item in orphaned_items
        ]

        deleted_count = len(delete_requests)

        for i in range(0, len(delete_requests), 25):
            chunk = delete_requests[i:i + 25]
            batch_response = self.dynamodb.meta.client.batch_write_item(
                RequestItems={table_name: chunk}
            )

            # Retry unprocessed items
            unprocessed = batch_response.get('UnprocessedItems', {}).get(table_name, [])
            while unprocessed:
                time.sleep(0.1)  # nosemgrep: arbitrary-sleep -- deliberate backoff before retrying UnprocessedItems
                batch_response = self.dynamodb.meta.client.batch_write_item(
                    RequestItems={table_name: unprocessed}
                )
                unprocessed = batch_response.get('UnprocessedItems', {}).get(table_name, [])

        return deleted_count

    def get_all_configured_models(self) -> List[str]:
        """Query all MODEL#*#CONFIG items and return model_ids."""
        response = self.single_table.scan(
            FilterExpression='entity_type = :et',
            ExpressionAttributeValues={':et': 'model_config'},
            ProjectionExpression='model_id'
        )

        return [item['model_id'] for item in response.get('Items', []) if 'model_id' in item]

    def get_effective_capacity(self, model_id: str) -> Dict[str, Any]:
        """
        Get model config with adaptive capacity adjustment.

        When queue has items, shifts up to adaptive_shift_max of burst capacity
        to queue to accelerate drain rate. Self-correcting: as queue drains,
        burst capacity recovers.

        Returns:
            Config dict with adjusted burst_capacity and queue_capacity
        """
        config = self.get_model_config(model_id)

        adaptive_shift_max = float(config.get('adaptive_shift_max', 0))
        if adaptive_shift_max <= 0:
            return config

        adaptive_threshold = int(config.get('adaptive_queue_threshold', 50))
        queue_depth = self.get_queue_depth(model_id)

        if queue_depth <= 0:
            return config

        # Linear shift: 0 at queue_depth=0, adaptive_shift_max at queue_depth>=threshold
        shift_pct = min(adaptive_shift_max, (queue_depth / adaptive_threshold) * adaptive_shift_max)

        base_burst = int(config['burst_capacity'])
        base_queue = int(config['queue_capacity'])
        shift_amount = int(base_burst * shift_pct)

        # Return adjusted config (don't mutate original)
        adjusted = dict(config)
        adjusted['burst_capacity'] = base_burst - shift_amount
        adjusted['queue_capacity'] = base_queue + shift_amount
        adjusted['_adaptive_shift'] = shift_amount

        return adjusted

    # === Terminal-Status Methods (honest-outcomes layer) ===

    def write_pending_status(self, *, request_id: str, tenant_id: Optional[str],
                             correlation_id: Optional[str], model_id: str,
                             arm: str = "shaper", source: str = "immediate") -> None:
        """
        Write the initial PENDING terminal-status item for a request.

        PutItem gated on attribute_not_exists(pk) (Cato C-7): a Step Functions Retry
        of the first-state PENDING write must never clobber an already-committed
        terminal state back to PENDING. A duplicate PENDING write is therefore a
        no-op — the ConditionalCheckFailedException is swallowed.

        Item: pk=REQUEST#{request_id}, sk=STATUS, keyed on request_id ALONE
        (tenant_id is a stored/checked attribute, not part of the key). state=PENDING,
        http_status=202, ttl = created_at + 24h. Does NOT emit EMF (OutcomeStreamFn
        owns EMF off the DDB stream — Cato C-2).
        """
        now = time.time()
        now_iso = datetime.utcnow().isoformat()

        item = {
            'pk': f'REQUEST#{request_id}',
            'sk': 'STATUS',
            'entity_type': 'request_status',
            'request_id': request_id,
            'correlation_id': correlation_id,
            'tenant_id': tenant_id,
            'model_id': model_id,
            'arm': arm,
            'source': source,
            'state': 'PENDING',
            'reason': None,
            'http_status': 202,
            'attempts': 1,
            'created_at': now_iso,
            'updated_at': now_iso,
            'ttl': int(now) + 86400  # 24h retention
        }

        try:
            self.single_table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(pk)'
            )
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # Item already exists (PENDING already written, or a terminal state has
            # been committed). A Retry must not clobber it — idempotent no-op.
            print(f"PENDING write skipped (item exists) for request_id={request_id}")

    def write_terminal_status(self, *, request_id: str, state: str,
                              reason: Optional[str] = None, http_status: int,
                              tenant_id: Optional[str], correlation_id: Optional[str],
                              model_id: str, arm: str = "shaper", source: str = "queued",
                              output_ref: Optional[str] = None,
                              attempts: int = 1,
                              duration_ms: Optional[int] = None) -> bool:
        """
        Gate the terminal transition exactly-once with one conditional UpdateItem.

        ConditionExpression: attribute_not_exists(pk) OR #state IN (:pending,:queued)
        — permits PENDING|QUEUED -> terminal (and a fresh create when no PENDING row
        exists, e.g. the queue_expired skip path or a FAILED finalizer that races the
        SM first-state write), but blocks terminal -> terminal. The first writer wins;
        every later writer catches ConditionalCheckFailedException and returns False
        (loser swallows).

        Item is keyed on request_id ALONE (pk=REQUEST#{request_id}, sk=STATUS);
        tenant_id is a stored/checked attribute, never part of the key. created_at and
        ttl are set only on create (if_not_exists) so a PENDING->terminal transition
        preserves the original values; a fresh create sets ttl = now + 24h.

        Does NOT emit EMF — the RequestOutcome metric is emitted exactly-from-the-record
        by OutcomeStreamFn off the DDB stream on the PENDING|QUEUED -> terminal
        transition (Cato C-2: an inline writer-emit is only at-most-once).

        Returns:
            True if this call won the terminal transition, False if it lost the race
            (ConditionalCheckFailedException — another writer already went terminal).
        """
        now = time.time()
        now_iso = datetime.utcnow().isoformat()

        set_clauses = [
            '#state = :state',
            'reason = :reason',
            'http_status = :http_status',
            'updated_at = :updated_at',
            'entity_type = :entity_type',
            'request_id = :request_id',
            'correlation_id = :correlation_id',
            'tenant_id = :tenant_id',
            'model_id = :model_id',
            'arm = :arm',
            '#source = :source',
            'attempts = :attempts',
            'created_at = if_not_exists(created_at, :created_at)',
            '#ttl = if_not_exists(#ttl, :ttl)',
        ]

        values = {
            ':state': state,
            ':reason': reason,
            ':http_status': int(http_status),
            ':updated_at': now_iso,
            ':entity_type': 'request_status',
            ':request_id': request_id,
            ':correlation_id': correlation_id,
            ':tenant_id': tenant_id,
            ':model_id': model_id,
            ':arm': arm,
            ':source': source,
            ':attempts': int(attempts),
            ':created_at': now_iso,
            ':ttl': int(now) + 86400,  # 24h retention (create-only)
            ':pending': 'PENDING',
            ':queued': 'QUEUED',
        }

        # Optional attributes: only written when supplied (never overwrite with null).
        if output_ref is not None:
            set_clauses.append('output_ref = :output_ref')
            values[':output_ref'] = output_ref
        if duration_ms is not None:
            set_clauses.append('duration_ms = :duration_ms')
            values[':duration_ms'] = int(duration_ms)

        try:
            self.single_table.update_item(
                Key={'pk': f'REQUEST#{request_id}', 'sk': 'STATUS'},
                UpdateExpression='SET ' + ', '.join(set_clauses),
                ConditionExpression='attribute_not_exists(pk) OR #state IN (:pending, :queued)',
                ExpressionAttributeNames={
                    '#state': 'state',
                    '#source': 'source',
                    '#ttl': 'ttl'
                },
                ExpressionAttributeValues=values
            )
            return True
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # Lost the race: a terminal state is already committed. Swallow and report
            # the loss so the caller (and any TerminalWriteConflict observability) can
            # distinguish "I wrote it" from "someone beat me to it".
            return False

    # === Queue Consumption Methods (Queue Processor) ===

    def put_queue_consumption(self, model_id: str, request_id: str, estimated_tokens: int = 0,
                              correlation_id: Optional[str] = None,
                              estimated_input_tokens: int = 0,
                              estimated_output_tokens: int = 0) -> Dict[str, Any]:
        """
        Write queue consumption record (leaky bucket pattern).
        Similar to put_allocation() but for QUEUE#CONSUMPTION partition.

        Args:
            model_id: Bedrock model ID
            request_id: Unique request identifier
            estimated_tokens: Estimated combined TPM cost of this request (0 = no TPM tracking)
            correlation_id: Optional ID to trace requests across all 3 Lambdas
            estimated_input_tokens: Estimated input tokens (mantle iTPM tracking)
            estimated_output_tokens: Estimated output tokens (mantle oTPM tracking)
        """
        now = time.time()
        now_ms = int(now * 1000)

        item = {
            'pk': f'MODEL#{model_id}#QUEUE#CONSUMPTION',
            'sk': f'{now_ms}#{request_id}',
            'entity_type': 'queue_consumption_token',
            'request_id': request_id,
            'count': 1,
            'estimated_tokens': estimated_tokens,
            'estimated_input_tokens': estimated_input_tokens,
            'estimated_output_tokens': estimated_output_tokens,
            'source': 'queue',
            'consumed_at': datetime.fromtimestamp(now).isoformat(),
            'ttl': int(now) + 300  # 5 min retention
        }

        if correlation_id:
            item['correlation_id'] = correlation_id

        self.single_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(pk) AND attribute_not_exists(sk)'
        )

        return {
            'timestamp_ms': now_ms,
            'timestamp': now,
            'item': item
        }

    def query_queue_consumption_records(
        self,
        model_id: str,
        window_seconds: int = 60,
        consistent_read: bool = True
    ) -> List[Dict[str, Any]]:
        """Get queue consumption records in sliding window."""
        return self.query_consumption_records(
            model_id=model_id,
            capacity_mode='QUEUE',
            window_seconds=window_seconds,
            consistent_read=consistent_read
        )

    def calculate_queue_capacity(
        self,
        model_id: str,
        current_time: Optional[float] = None
    ) -> float:
        """
        Calculate available queue capacity using leaky bucket.
        Uses queue_capacity (40) and queue_regeneration_rate (0.667).
        """
        config = self.get_model_config(model_id)
        queue_capacity = int(config['queue_capacity'])
        queue_regen_rate = float(config['queue_regeneration_rate'])

        consumption_records = self.query_queue_consumption_records(
            model_id=model_id,
            window_seconds=60,
            consistent_read=True
        )

        return self.calculate_available_tokens(
            capacity=queue_capacity,
            consumption_records=consumption_records,
            regeneration_rate=queue_regen_rate,
            current_time=current_time
        )

    def delete_queue_consumption(
        self,
        model_id: str,
        timestamp_ms: int,
        request_id: str
    ) -> None:
        """Delete queue consumption record (rollback for over-consumption)."""
        self.delete_consumption_record(
            model_id=model_id,
            capacity_mode='QUEUE',
            timestamp_ms=timestamp_ms,
            request_id=request_id
        )

    def enqueue_request(
        self,
        model_id: str,
        request_id: str,
        request_payload: Optional[Dict[str, Any]] = None,
        task_token: Optional[str] = None,
        execution_arn: Optional[str] = None,
        correlation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        priority: int = 1,
        expiry_hours: int = 1,
        estimated_tokens: int = 0,
        estimated_input_tokens: int = 0,
        estimated_output_tokens: int = 0,
    ) -> Dict[str, Any]:
        """
        Enqueue request to single table (MODEL#QUEUE#ITEMS).
        Uses condition expression to prevent duplicate enqueues.

        The estimated_* fields are the budget manager's pre-call token estimate
        (it has the prompt in hand at enqueue time; the queue item does NOT carry
        the prompt). Carried onto the queue item so the queue processor can log
        each request's estimated cost at dispatch without re-resolving the prompt.
        Runtime models populate estimated_tokens (combined); mantle populates the
        split input/output fields.
        """
        now = datetime.utcnow()
        timestamp_ms = int(now.timestamp() * 1000)
        expires_at = now + timedelta(hours=expiry_hours)
        ttl_timestamp = int(expires_at.timestamp())

        sort_key = f"{timestamp_ms:015d}#{priority:03d}#{request_id}"

        queue_item = {
            'pk': f'MODEL#{model_id}#QUEUE#ITEMS',
            'sk': sort_key,
            'entity_type': 'queue_item',
            'request_id': request_id,
            'model_id': model_id,
            'priority': priority,
            'queued_at': now.isoformat(),
            'timestamp_ms': timestamp_ms,
            'expires_at': expires_at.isoformat(),
            'ttl': ttl_timestamp
        }

        if estimated_tokens:
            queue_item['estimated_tokens'] = estimated_tokens
        if estimated_input_tokens:
            queue_item['estimated_input_tokens'] = estimated_input_tokens
        if estimated_output_tokens:
            queue_item['estimated_output_tokens'] = estimated_output_tokens

        if task_token:
            queue_item['task_token'] = task_token
        if execution_arn:
            queue_item['execution_arn'] = execution_arn
        if request_payload:
            queue_item['request_payload'] = request_payload
        if correlation_id:
            queue_item['correlation_id'] = correlation_id
        if tenant_id:
            queue_item['tenant_id'] = tenant_id

        self.single_table.put_item(
            Item=queue_item,
            ConditionExpression='attribute_not_exists(pk) AND attribute_not_exists(sk)'
        )

        return queue_item

    def get_queue_depth(self, model_id: str) -> int:
        """Get queue length from single table."""
        response = self.single_table.query(
            KeyConditionExpression=Key('pk').eq(f'MODEL#{model_id}#QUEUE#ITEMS'),
            Select='COUNT'
        )
        return response['Count']

    @staticmethod
    def _queue_item_expired(item: Dict[str, Any], now: datetime) -> bool:
        """
        Decide whether a queue item has logically expired (expires_at < now).

        `expires_at` is an ISO-8601 UTC string written by enqueue_request(). DynamoDB
        TTL deletion lags (up to ~48h), so an item can still be present long after its
        logical expiry — we must never dequeue such an item (it would spend real Bedrock
        quota on a request the SM has already TimedOut, then the idempotency guard blocks
        the SUCCEEDED write → the client sees 504 for a paid success). Fail SAFE: if
        expires_at is missing or unparseable, treat the item as NOT expired so we never
        drop a live request on a parse error.
        """
        raw = item.get('expires_at')
        if not raw:
            return False
        try:
            expires_at = datetime.fromisoformat(str(raw))
        except (ValueError, TypeError):
            return False
        return expires_at < now

    def batch_dequeue_items(self, model_id: str, batch_size: int) -> List[Dict[str, Any]]:
        """
        Dequeue up to batch_size items (oldest first).
        Uses BatchWriteItem for efficient bulk deletion.

        Skip-and-delete any logically-expired item (expires_at < now) BEFORE dequeuing:
        each expired item gets a terminal FAILED/queue_expired/504 status written via the
        shared exactly-once guard, then is deleted — it is never returned to the caller
        and never spends Bedrock quota. Non-expired items keep the exact prior behavior.

        Args:
            model_id: The Bedrock model ID
            batch_size: Maximum number of items to dequeue

        Returns:
            List of live (non-expired) dequeued items (already deleted from table)
        """
        # Query oldest items (FIFO)
        response = self.single_table.query(
            KeyConditionExpression=Key('pk').eq(f'MODEL#{model_id}#QUEUE#ITEMS'),
            Limit=batch_size,
            ScanIndexForward=True  # Oldest first
        )

        items = response.get('Items', [])
        if not items:
            return []

        # Partition into logically-expired vs live. Expired items must never be
        # dequeued/processed — they are terminalized (queue_expired/504) and deleted.
        now = datetime.utcnow()
        expired_items = [item for item in items if self._queue_item_expired(item, now)]
        live_items = [item for item in items if not self._queue_item_expired(item, now)]

        table_name = self.single_table.table_name

        # --- Expired path: terminalize (queue_expired/504) then delete ---
        for item in expired_items:
            request_id = item.get('request_id')
            if request_id:
                # Shared exactly-once guard. If a terminal state already exists (e.g. the
                # SM TimedOut finalizer beat us), write_terminal_status returns False and
                # we still proceed to delete the queue item.
                self.write_terminal_status(
                    request_id=request_id,
                    state='FAILED',
                    reason='queue_expired',
                    http_status=504,
                    tenant_id=item.get('tenant_id'),
                    correlation_id=item.get('correlation_id'),
                    model_id=item.get('model_id', model_id),
                    arm=item.get('arm', 'shaper'),
                    source=item.get('source', 'queued'),
                )

        if expired_items:
            self._batch_delete_items(table_name, expired_items)

        # --- Live path: unchanged behavior (batch delete + return) ---
        if not live_items:
            return []

        self._batch_delete_items(table_name, live_items)

        return live_items

    def _batch_delete_items(self, table_name: str, items: List[Dict[str, Any]]) -> None:
        """Batch-delete queue items (25 per BatchWriteItem call, retrying unprocessed)."""
        delete_requests = [
            {'DeleteRequest': {'Key': {'pk': item['pk'], 'sk': item['sk']}}}
            for item in items
        ]

        # BatchWriteItem supports max 25 items per call
        for i in range(0, len(delete_requests), 25):
            chunk = delete_requests[i:i + 25]
            batch_response = self.dynamodb.meta.client.batch_write_item(
                RequestItems={table_name: chunk}
            )

            # Retry unprocessed items (rare, but handle for robustness)
            unprocessed = batch_response.get('UnprocessedItems', {}).get(table_name, [])
            while unprocessed:
                time.sleep(0.1)  # Brief backoff  # nosemgrep: arbitrary-sleep -- deliberate backoff before retrying UnprocessedItems
                batch_response = self.dynamodb.meta.client.batch_write_item(
                    RequestItems={table_name: unprocessed}
                )
                unprocessed = batch_response.get('UnprocessedItems', {}).get(table_name, [])

    def record_invocation_error(
        self,
        model_id: str,
        request_id: str,
        execution_arn: Optional[str],
        error: str
    ) -> Dict[str, Any]:
        """
        Record a failed Bedrock invocation for debugging.

        Args:
            model_id: The Bedrock model ID
            request_id: Request ID from the queue item
            execution_arn: Step Function execution ARN (if available)
            error: Error message/description

        Returns:
            The created error record item
        """
        now = time.time()
        now_ms = int(now * 1000)

        item = {
            'pk': f'MODEL#{model_id}#INVOCATION#ERRORS',
            'sk': f'{now_ms}#{request_id}',
            'entity_type': 'invocation_error',
            'request_id': request_id,
            'error_message': error,
            'error_type': error.split(':')[0] if ':' in error else 'UnknownError',
            'occurred_at': datetime.fromtimestamp(now).isoformat(),
            'ttl': int(now) + 86400 * 7  # 7 day retention
        }

        if execution_arn:
            item['step_function_execution'] = execution_arn

        self.single_table.put_item(Item=item)

        return item

    # === Heartbeat Lock Methods (Single Table) ===

    def is_processor_lock_active(
        self,
        model_id: str,
        slot: int = 0
    ) -> bool:
        """
        Check if an active (non-stale) processor lock exists.

        Used by Budget Manager to decide whether to trigger Queue Processor.
        Does NOT acquire the lock - only checks status.

        Args:
            model_id: The Bedrock model ID
            slot: Lock slot number (default 0)

        Returns:
            True if lock exists and TTL is not expired (active processor)
            False if no lock or TTL is expired (stale/no processor)
        """
        try:
            response = self.single_table.get_item(
                Key={
                    'pk': f'MODEL#{model_id}#LOCK',
                    'sk': f'PROCESSOR#{slot}'
                }
            )

            if 'Item' not in response:
                # No lock exists
                return False

            item = response['Item']
            lock_ttl = int(item.get('ttl', 0))
            now = int(time.time())

            if lock_ttl < now:
                # Lock exists but TTL expired (stale)
                processor_id = item.get('processor_id', 'unknown')
                print(f"Stale lock detected: processor_id={processor_id}, ttl={lock_ttl}, now={now}")
                return False

            # Lock exists and is active
            return True

        except Exception as e:
            print(f"Error checking processor lock: {e}")
            # On error, assume no active lock to avoid blocking queue processing
            return False

    def acquire_processor_lock(
        self,
        model_id: str,
        processor_id: str,
        slot: int = 0
    ) -> bool:
        """
        Acquire lock in single table, overwriting if TTL expired.

        Uses heartbeat-based locking for self-healing when processors crash.
        Key: pk=MODEL#{model_id}#LOCK, sk=PROCESSOR#{slot}
        Condition: attribute_not_exists(pk) OR ttl < :now

        Args:
            model_id: The Bedrock model ID
            processor_id: Unique ID for this processor (e.g., context.aws_request_id)
            slot: Lock slot number (default 0 for single processor)

        Returns:
            True if lock acquired, False if active lock exists
        """
        now = int(time.time())
        ttl = now + LOCK_TTL

        try:
            self.single_table.put_item(
                Item={
                    'pk': f'MODEL#{model_id}#LOCK',
                    'sk': f'PROCESSOR#{slot}',
                    'entity_type': 'processor_lock',
                    'processor_id': processor_id,
                    'locked_at': datetime.utcnow().isoformat(),
                    'ttl': ttl
                },
                ConditionExpression='attribute_not_exists(pk) OR #ttl < :now',
                ExpressionAttributeNames={
                    '#ttl': 'ttl'
                },
                ExpressionAttributeValues={
                    ':now': now
                }
            )
            return True
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # Lock exists and is not expired
            return False

    def refresh_processor_heartbeat(
        self,
        model_id: str,
        processor_id: str,
        slot: int = 0
    ) -> bool:
        """
        Refresh lock TTL (heartbeat). Returns False if we lost ownership.

        Should be called every LOCK_HEARTBEAT_INTERVAL seconds to keep lock alive.
        Key: pk=MODEL#{model_id}#LOCK, sk=PROCESSOR#{slot}
        Condition: processor_id = :pid

        Args:
            model_id: The Bedrock model ID
            processor_id: Must match the processor_id that acquired the lock
            slot: Lock slot number (default 0)

        Returns:
            True if heartbeat successful, False if lost ownership
        """
        now = int(time.time())
        ttl = now + LOCK_TTL

        try:
            self.single_table.update_item(
                Key={
                    'pk': f'MODEL#{model_id}#LOCK',
                    'sk': f'PROCESSOR#{slot}'
                },
                UpdateExpression='SET #ttl = :ttl, heartbeat_at = :now',
                ConditionExpression='processor_id = :pid',
                ExpressionAttributeNames={
                    '#ttl': 'ttl'
                },
                ExpressionAttributeValues={
                    ':ttl': ttl,
                    ':now': datetime.utcnow().isoformat(),
                    ':pid': processor_id
                }
            )
            return True
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # Lost ownership - another processor acquired the lock
            return False

    def release_processor_lock(
        self,
        model_id: str,
        processor_id: str,
        slot: int = 0
    ) -> bool:
        """
        Release lock only if we own it.

        Key: pk=MODEL#{model_id}#LOCK, sk=PROCESSOR#{slot}
        Condition: processor_id = :pid

        Args:
            model_id: The Bedrock model ID
            processor_id: Must match the processor_id that acquired the lock
            slot: Lock slot number (default 0)

        Returns:
            True if released, False if we didn't own it
        """
        try:
            self.single_table.delete_item(
                Key={
                    'pk': f'MODEL#{model_id}#LOCK',
                    'sk': f'PROCESSOR#{slot}'
                },
                ConditionExpression='processor_id = :pid',
                ExpressionAttributeValues={
                    ':pid': processor_id
                }
            )
            return True
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # We don't own the lock
            return False
