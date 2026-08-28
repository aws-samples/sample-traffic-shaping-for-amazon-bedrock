#!/bin/bash
# Window-aligned test launcher
# Waits until second 0 of the next minute window, then launches the test.
# This eliminates window-position sensitivity from the burst admission count.
#
# Usage: ./scripts/run_window_aligned.sh [run_number]

RUN=${1:-1}
echo "=== Window-Aligned Baseline Test — Run $RUN ==="
echo ""

# Calculate seconds until next minute boundary
CURRENT_SEC=$(date +%S | sed 's/^0//')
if [ "$CURRENT_SEC" -eq 0 ]; then
    WAIT=0
else
    WAIT=$((60 - CURRENT_SEC))
fi

echo "Current time: $(date '+%H:%M:%S')"
echo "Seconds until next window: $WAIT"

if [ "$WAIT" -gt 0 ]; then
    echo "Waiting ${WAIT}s for window alignment..."
    sleep "$WAIT"
fi

echo ""
echo ">>> Launching at window boundary: $(date '+%H:%M:%S')"
echo ""

# Run the test with council-recommended parameters:
# 2000 requests, burst_capacity=50, 100 workers, 15s submission, Nova Lite
make test-budget-manager ARGS="--model nova-lite --num-requests 2000 --max-workers 100 --submission-duration 15 --prompt-size 4000 --max-tokens 4096"
