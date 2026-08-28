#!/usr/bin/env python3
"""
Soak Test — Sustained traffic at target RPM with adversarial injection.

Sprint 2: Prove the system survives sustained and adversarial load.

Usage:
  # Quick validation (1 hour at 70% of Jamba RPM)
  make soak-test ARGS="--model jamba --target-rpm 70 --duration-hours 1"

  # Full 72-hour soak
  make soak-test ARGS="--model jamba --target-rpm 70 --duration-hours 72"

  # With adversarial injection (5% bad requests)
  make soak-test ARGS="--model jamba --target-rpm 70 --duration-hours 1 --adversarial-pct 5"
"""

import argparse
import boto3
import json
import os
import random
import signal
import sys
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# Add lambda layer to Python path
layer_path = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'lambda_layer', 'python')
sys.path.insert(0, layer_path)

import config_loader
from shared_service import DynamoService

# Model aliases
MODEL_ALIASES = {
    'opus': 'us.anthropic.claude-opus-5',
    'jamba': 'us.amazon.nova-2-lite-v1:0',
    'nova-lite': 'us.amazon.nova-lite-v1:0',
}

# Prompt templates for payload variety
PROMPTS = [
    "What is 2+2?",
    "Explain quantum computing in one paragraph.",
    "Write a haiku about cloud infrastructure.",
    "List 5 benefits of serverless architecture.",
    "Summarize the theory of relativity in 3 sentences.",
    "What are the main differences between SQL and NoSQL databases?",
    "Describe the water cycle in simple terms.",
    "What is the capital of France and why is it historically significant?",
]


def parse_args():
    parser = argparse.ArgumentParser(description='Soak test for Bedrock Traffic Shaper')
    parser.add_argument('--model', type=str, default='nova-2-lite', help='Model alias or ID (default: nova-2-lite)')
    parser.add_argument('--target-rpm', type=int, default=70, help='Target requests per minute (default: 70)')
    parser.add_argument('--duration-hours', type=float, default=1, help='Soak duration in hours (default: 1)')
    parser.add_argument('--adversarial-pct', type=float, default=5, help='Percentage of adversarial requests (default: 5)')
    parser.add_argument('--checkpoint-min', type=int, default=5, help='Minutes between checkpoint reports (default: 5)')
    parser.add_argument('--max-tokens', type=int, default=100, help='Default max_tokens per request (default: 100)')
    parser.add_argument('--output', type=str, default=None, help='Output JSON file for results (default: soak_results_<timestamp>.json)')
    return parser.parse_args()


class SoakMetrics:
    """Thread-safe metrics collector."""

    def __init__(self):
        self.lock = threading.Lock()
        self.total_sent = 0
        self.total_succeeded = 0
        self.total_queued = 0
        self.total_failed = 0
        self.total_rejected = 0  # Input validation rejections (expected for adversarial)
        self.total_running = 0
        self.latencies = []  # (timestamp, latency_ms) tuples
        self.errors = defaultdict(int)  # error_type -> count
        self.checkpoints = []  # Periodic snapshots
        self.adversarial_sent = 0
        self.adversarial_rejected = 0
        self.queue_depth_samples = []  # (timestamp, depth) tuples
        self.dlq_depth_samples = []  # (timestamp, depth) tuples
        self.start_time = time.time()

    def record_send(self, is_adversarial=False):
        with self.lock:
            self.total_sent += 1
            if is_adversarial:
                self.adversarial_sent += 1

    def record_result(self, status, latency_ms=None, error_type=None, is_adversarial=False):
        with self.lock:
            if status == 'succeeded':
                self.total_succeeded += 1
            elif status == 'queued':
                self.total_queued += 1
            elif status == 'rejected':
                self.total_rejected += 1
                if is_adversarial:
                    self.adversarial_rejected += 1
            elif status == 'failed':
                self.total_failed += 1
                if error_type:
                    self.errors[error_type] += 1
            if latency_ms is not None:
                self.latencies.append((time.time(), latency_ms))

    def record_queue_depth(self, depth):
        with self.lock:
            self.queue_depth_samples.append((time.time(), depth))

    def record_dlq_depth(self, depth):
        with self.lock:
            self.dlq_depth_samples.append((time.time(), depth))

    def snapshot(self):
        """Take a point-in-time snapshot for checkpoint reporting."""
        with self.lock:
            elapsed = time.time() - self.start_time
            recent_latencies = [lat for ts, lat in self.latencies if ts > time.time() - 300]
            recent_latencies.sort()

            snap = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'elapsed_min': elapsed / 60,
                'total_sent': self.total_sent,
                'succeeded': self.total_succeeded,
                'queued': self.total_queued,
                'failed': self.total_failed,
                'rejected': self.total_rejected,
                'adversarial_sent': self.adversarial_sent,
                'adversarial_rejected': self.adversarial_rejected,
                'success_rate': (self.total_succeeded + self.total_queued) / max(1, self.total_succeeded + self.total_queued + self.total_failed) * 100,
                'effective_rpm': self.total_sent / max(1, elapsed / 60),
                'queue_depth': self.queue_depth_samples[-1][1] if self.queue_depth_samples else 0,
                'dlq_depth': self.dlq_depth_samples[-1][1] if self.dlq_depth_samples else 0,
                'errors': dict(self.errors),
            }

            if recent_latencies:
                snap['latency_p50'] = recent_latencies[len(recent_latencies) // 2]
                snap['latency_p95'] = recent_latencies[int(len(recent_latencies) * 0.95)]
                snap['latency_p99'] = recent_latencies[int(len(recent_latencies) * 0.99)]
            else:
                snap['latency_p50'] = snap['latency_p95'] = snap['latency_p99'] = 0

            self.checkpoints.append(snap)
            return snap

    def final_report(self):
        """Generate final report dict."""
        with self.lock:
            elapsed = time.time() - self.start_time
            all_latencies = sorted([lat for _, lat in self.latencies])
            total_valid = self.total_sent - self.total_rejected
            return {
                'duration_hours': elapsed / 3600,
                'total_sent': self.total_sent,
                'total_valid': total_valid,
                'succeeded': self.total_succeeded,
                'queued': self.total_queued,
                'failed': self.total_failed,
                'rejected': self.total_rejected,
                'success_rate': (self.total_succeeded + self.total_queued) / max(1, total_valid) * 100,
                'effective_rpm': self.total_sent / max(1, elapsed / 60),
                'adversarial_sent': self.adversarial_sent,
                'adversarial_rejected': self.adversarial_rejected,
                'errors': dict(self.errors),
                'latency_p50': all_latencies[len(all_latencies) // 2] if all_latencies else 0,
                'latency_p95': all_latencies[int(len(all_latencies) * 0.95)] if all_latencies else 0,
                'latency_p99': all_latencies[int(len(all_latencies) * 0.99)] if all_latencies else 0,
                'max_queue_depth': max((d for _, d in self.queue_depth_samples), default=0),
                'max_dlq_depth': max((d for _, d in self.dlq_depth_samples), default=0),
                'checkpoints': self.checkpoints,
            }


class SoakTest:
    """Sustained-rate soak test with adversarial injection."""

    def __init__(self, args, config):
        self.model_id = MODEL_ALIASES.get(args.model.lower(), args.model)
        self.target_rpm = args.target_rpm
        self.duration_seconds = args.duration_hours * 3600
        self.adversarial_pct = args.adversarial_pct / 100.0
        self.checkpoint_interval = args.checkpoint_min * 60
        self.default_max_tokens = args.max_tokens
        self.output_file = args.output or f"soak_results_{datetime.now().strftime('%Y%m%dT%H%M%S')}.json"

        self.region = config.get('AWS_REGION', 'us-east-1')
        self.state_machine_arn = config.get('STATE_MACHINE_ARN', '')
        self.single_table = config.get('SINGLE_TABLE_NAME', 'semaphore-single-table')
        self.dlq_url = config.get('DLQ_URL', '')

        self.sfn_client = boto3.client('stepfunctions', region_name=self.region)
        self.sqs_client = boto3.client('sqs', region_name=self.region)
        self.dynamo_service = DynamoService(single_table_name=self.single_table)
        self.metrics = SoakMetrics()
        self.shutdown = threading.Event()

        # Request interval for target RPM
        self.interval = 60.0 / self.target_rpm

        # Pending executions to poll
        self.pending_lock = threading.Lock()
        self.pending = {}  # execution_arn -> {start_time, is_adversarial}

        # Record baseline DLQ depth (from prior tests) so we only count new messages
        self.dlq_baseline = 0
        if self.dlq_url:
            try:
                resp = self.sqs_client.get_queue_attributes(
                    QueueUrl=self.dlq_url,
                    AttributeNames=['ApproximateNumberOfMessages']
                )
                self.dlq_baseline = int(resp['Attributes'].get('ApproximateNumberOfMessages', 0))
            except Exception:
                pass  # nosec B110  # best-effort DLQ baseline capture, failure non-fatal

    def build_request(self, seq_num):
        """Build a normal or adversarial request."""
        is_adversarial = random.random() < self.adversarial_pct  # nosec B311  # non-crypto: load-test sampling

        if is_adversarial:
            # Pick an adversarial scenario
            scenario = random.choice(['oversized_tokens', 'large_prompt'])  # nosec B311  # non-crypto: load-test sampling
            if scenario == 'oversized_tokens':
                return {
                    'request_id': f'soak_{seq_num}_adv_tokens',
                    'model_id': self.model_id,
                    'prompt': random.choice(PROMPTS),  # nosec B311  # non-crypto: load-test sampling
                    'max_tokens': 10000,  # Exceeds default 4096 limit
                }, True
            else:  # large_prompt
                # Generate a ~200KB prompt (under SFN 256KB input limit, tests large payload handling)
                # Note: 1MB prompt size validation is defense-in-depth for direct Lambda invocation
                # and can't be tested via SFN (256KB input limit is hit first)
                return {
                    'request_id': f'soak_{seq_num}_adv_prompt',
                    'model_id': self.model_id,
                    'prompt': 'x' * 200_000,
                    'max_tokens': self.default_max_tokens,
                }, True
        else:
            # Normal request with variable prompt
            return {
                'request_id': f'soak_{seq_num}',
                'model_id': self.model_id,
                'prompt': random.choice(PROMPTS),  # nosec B311  # non-crypto: load-test sampling
                'max_tokens': self.default_max_tokens,
            }, False

    def send_request(self, seq_num):
        """Send a single request via Step Functions."""
        payload, is_adversarial = self.build_request(seq_num)
        self.metrics.record_send(is_adversarial=is_adversarial)
        start_time = time.time()

        try:
            result = self.sfn_client.start_execution(
                stateMachineArn=self.state_machine_arn,
                input=json.dumps(payload)
            )
            exec_arn = result['executionArn']
            with self.pending_lock:
                self.pending[exec_arn] = {
                    'start_time': start_time,
                    'is_adversarial': is_adversarial,
                    'request_id': payload['request_id'],
                }
        except Exception as e:
            self.metrics.record_result('failed', error_type=type(e).__name__, is_adversarial=is_adversarial)

    def poll_executions(self):
        """Background thread: poll pending SFN executions for completion."""
        while not self.shutdown.is_set():
            with self.pending_lock:
                arns = list(self.pending.keys())

            for arn in arns:
                if self.shutdown.is_set():
                    break
                try:
                    desc = self.sfn_client.describe_execution(executionArn=arn)
                    status = desc['status']
                    if status in ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'):
                        with self.pending_lock:
                            info = self.pending.pop(arn, {})
                        latency_ms = (time.time() - info.get('start_time', time.time())) * 1000
                        is_adv = info.get('is_adversarial', False)

                        if status == 'SUCCEEDED':
                            output = json.loads(desc.get('output', '{}'))
                            if output.get('budget_result', {}).get('source') == 'queued':
                                self.metrics.record_result('queued', latency_ms=latency_ms, is_adversarial=is_adv)
                            else:
                                self.metrics.record_result('succeeded', latency_ms=latency_ms, is_adversarial=is_adv)
                        elif status == 'FAILED':
                            # Check if it's an expected input validation rejection
                            error = desc.get('error', '')
                            cause = desc.get('cause', '')
                            # Get error from execution history
                            try:
                                history = self.sfn_client.get_execution_history(
                                    executionArn=arn,
                                    maxResults=10,
                                    reverseOrder=True
                                )
                                for event in history.get('events', []):
                                    if event.get('type') == 'TaskFailed':
                                        error = event.get('taskFailedEventDetails', {}).get('error', '')
                                        break
                            except Exception:
                                pass  # nosec B110  # best-effort error-detail lookup, failure non-fatal

                            if error == 'InputValidationError':
                                self.metrics.record_result('rejected', is_adversarial=is_adv)
                            else:
                                self.metrics.record_result('failed', error_type=error or status, is_adversarial=is_adv)
                        else:
                            self.metrics.record_result('failed', error_type=status, is_adversarial=is_adv)
                except Exception:
                    pass  # nosec B110  # best-effort status poll, transient API errors retried next cycle

            self.shutdown.wait(2)  # Poll every 2 seconds

    def monitor_health(self):
        """Background thread: sample queue depth and DLQ depth."""
        while not self.shutdown.is_set():
            try:
                depth = self.dynamo_service.get_queue_depth(self.model_id)
                self.metrics.record_queue_depth(depth)
            except Exception:
                pass  # nosec B110  # best-effort health sampling, failure non-fatal

            if self.dlq_url:
                try:
                    resp = self.sqs_client.get_queue_attributes(
                        QueueUrl=self.dlq_url,
                        AttributeNames=['ApproximateNumberOfMessages']
                    )
                    raw_dlq = int(resp['Attributes'].get('ApproximateNumberOfMessages', 0))
                    dlq_delta = max(0, raw_dlq - self.dlq_baseline)
                    self.metrics.record_dlq_depth(dlq_delta)
                except Exception:
                    pass  # nosec B110  # best-effort DLQ sampling, failure non-fatal

            self.shutdown.wait(10)  # Sample every 10 seconds

    def print_checkpoint(self, snap):
        """Print a checkpoint summary."""
        print(f"\n{'─' * 70}")
        print(f"CHECKPOINT @ {snap['elapsed_min']:.1f} min | {snap['timestamp']}")
        print(f"{'─' * 70}")
        print(f"  Sent: {snap['total_sent']} | Succeeded: {snap['succeeded']} | "
              f"Queued: {snap['queued']} | Failed: {snap['failed']} | Rejected: {snap['rejected']}")
        print(f"  Success rate: {snap['success_rate']:.1f}% | Effective RPM: {snap['effective_rpm']:.1f}")
        print(f"  Latency (5min window): p50={snap['latency_p50']:.0f}ms p95={snap['latency_p95']:.0f}ms p99={snap['latency_p99']:.0f}ms")
        print(f"  Queue depth: {snap['queue_depth']} | DLQ depth: {snap['dlq_depth']}")
        if snap['errors']:
            print(f"  Errors: {snap['errors']}")
        if snap['adversarial_sent'] > 0:
            print(f"  Adversarial: {snap['adversarial_sent']} sent, {snap['adversarial_rejected']} correctly rejected")
        print(f"{'─' * 70}")

    def print_final_report(self, report):
        """Print the final soak test report."""
        print(f"\n{'═' * 70}")
        print(f"SOAK TEST FINAL REPORT")
        print(f"{'═' * 70}")
        print(f"  Model:          {self.model_id}")
        print(f"  Target RPM:     {self.target_rpm}")
        print(f"  Duration:       {report['duration_hours']:.2f} hours")
        print(f"  Total sent:     {report['total_sent']}")
        print(f"  Valid requests: {report['total_valid']}")
        print(f"{'─' * 70}")
        print(f"  Succeeded:      {report['succeeded']}")
        print(f"  Queued:         {report['queued']}")
        print(f"  Failed:         {report['failed']}")
        print(f"  Rejected:       {report['rejected']} (input validation)")
        print(f"  Success rate:   {report['success_rate']:.2f}%")
        print(f"  Effective RPM:  {report['effective_rpm']:.1f}")
        print(f"{'─' * 70}")
        print(f"  Latency p50:    {report['latency_p50']:.0f} ms")
        print(f"  Latency p95:    {report['latency_p95']:.0f} ms")
        print(f"  Latency p99:    {report['latency_p99']:.0f} ms")
        print(f"{'─' * 70}")
        print(f"  Max queue depth: {report['max_queue_depth']}")
        print(f"  Max DLQ depth:   {report['max_dlq_depth']}")
        if report['errors']:
            print(f"  Errors:          {report['errors']}")
        if report['adversarial_sent'] > 0:
            print(f"{'─' * 70}")
            print(f"  Adversarial:     {report['adversarial_sent']} sent")
            print(f"  Correctly rejected: {report['adversarial_rejected']}")
        print(f"{'═' * 70}")

        # Pass/fail verdict
        target_success = 99.9
        if report['success_rate'] >= target_success and report['max_dlq_depth'] == 0:
            print(f"\n✅ SOAK TEST PASSED: {report['success_rate']:.2f}% success rate (target: {target_success}%), 0 DLQ messages")
        elif report['max_dlq_depth'] > 0:
            print(f"\n❌ SOAK TEST FAILED: {report['max_dlq_depth']} DLQ messages detected")
        else:
            print(f"\n❌ SOAK TEST FAILED: {report['success_rate']:.2f}% success rate (target: {target_success}%)")

    def run(self):
        """Execute the soak test."""
        print(f"\n{'═' * 70}")
        print(f"SOAK TEST — Bedrock Traffic Shaper")
        print(f"{'═' * 70}")
        print(f"  Model:          {self.model_id}")
        print(f"  Target RPM:     {self.target_rpm}")
        print(f"  Duration:       {self.duration_seconds / 3600:.1f} hours")
        print(f"  Interval:       {self.interval:.2f}s between requests")
        print(f"  Adversarial:    {self.adversarial_pct * 100:.1f}%")
        print(f"  Checkpoints:    every {self.checkpoint_interval / 60:.0f} min")
        print(f"  Output:         {self.output_file}")
        print(f"{'═' * 70}\n")

        # Validate model config exists
        try:
            config = self.dynamo_service.get_model_config(self.model_id)
            print(f"✓ Model config found: burst_capacity={config.get('burst_capacity')}, "
                  f"rpm_limit={config.get('rpm_limit')}")
        except Exception as e:
            print(f"❌ Model config not found: {e}")
            print(f"   Run: make create-config MODEL={args.model}")
            sys.exit(1)

        # Start background threads
        poller = threading.Thread(target=self.poll_executions, daemon=True)
        monitor = threading.Thread(target=self.monitor_health, daemon=True)
        poller.start()
        monitor.start()

        # Handle Ctrl+C gracefully
        def signal_handler(sig, frame):
            print("\n\nShutdown requested — finishing current requests...")
            self.shutdown.set()
        signal.signal(signal.SIGINT, signal_handler)

        print(f"Starting sustained traffic at {self.target_rpm} RPM...\n")

        seq_num = 0
        last_checkpoint = time.time()
        start_time = time.time()

        while not self.shutdown.is_set():
            elapsed = time.time() - start_time
            if elapsed >= self.duration_seconds:
                print(f"\nDuration reached ({self.duration_seconds / 3600:.1f} hours). Stopping...")
                break

            # Send one request
            self.send_request(seq_num)
            seq_num += 1

            # Progress indicator
            if seq_num % 10 == 0:
                with self.pending_lock:
                    pending_count = len(self.pending)
                m = self.metrics
                print(f"  [{elapsed/60:.1f}m] Sent: {m.total_sent} | OK: {m.total_succeeded + m.total_queued} | "
                      f"Fail: {m.total_failed} | Reject: {m.total_rejected} | Pending: {pending_count}",
                      end='\r')

            # Checkpoint report
            if time.time() - last_checkpoint >= self.checkpoint_interval:
                snap = self.metrics.snapshot()
                self.print_checkpoint(snap)
                last_checkpoint = time.time()

            # Rate limiting: sleep until next request
            # If we fall behind (e.g. checkpoint calculation), don't burst to catch up
            next_send = start_time + (seq_num * self.interval)
            sleep_time = next_send - time.time()
            if sleep_time > 0:
                self.shutdown.wait(sleep_time)
            elif sleep_time < -self.interval * 2:
                # More than 2 intervals behind — reset schedule instead of bursting
                start_time = time.time() - (seq_num * self.interval)

        # Wait for pending executions to complete (up to 5 minutes)
        print(f"\nWaiting for {len(self.pending)} pending executions (max 5 min)...")
        drain_start = time.time()
        while self.pending and (time.time() - drain_start) < 300:
            with self.pending_lock:
                remaining = len(self.pending)
            if remaining == 0:
                break
            print(f"  Pending: {remaining}", end='\r')
            time.sleep(2)  # nosemgrep: arbitrary-sleep -- intentional drain-polling interval in load test

        self.shutdown.set()

        # Final checkpoint and report
        self.metrics.snapshot()
        report = self.metrics.final_report()
        self.print_final_report(report)

        # Save results to JSON
        output_path = os.path.join(os.path.dirname(__file__), '..', self.output_file)
        with open(output_path, 'w', encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nResults saved to: {self.output_file}")

        return report


if __name__ == '__main__':
    args = parse_args()
    config = config_loader.get_config_with_aws_check()
    test = SoakTest(args, config)
    report = test.run()

    # Exit with appropriate code
    if report['success_rate'] >= 99.9 and report['max_dlq_depth'] == 0:
        sys.exit(0)
    else:
        sys.exit(1)
