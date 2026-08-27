.PHONY: help setup deploy test check-queue analyze-conflicts tail-budget tail-queue tail-bedrock clean \
	inspect-queue inspect-queue-single inspect-consumption inspect-queue-capacity inspect-config \
	inspect-tpm-consumption \
	test-budget-manager test-direct-bedrock test-direct-retry test-direct-leaky test-all test-stale-lock \
	test-queue-processor-sim test-budget-manager-sim \
	test-multi-model \
	soak-test analyze-soak \
	logs-recent logs-budget-recent logs-queue-recent logs-bedrock-recent logs-errors \
	set-capacity get-capacity create-config \
	inspect-dlq drain-dlq \
	dashboard

# Default target
help:
	@echo "Semaphore Rate Limiter - Available Commands"
	@echo "==========================================="
	@echo ""
	@echo "Setup & Deployment:"
	@echo "  make setup              - Initial setup (create venv, install deps, deploy CDK)"
	@echo "  make deploy             - Redeploy CDK stack and update config.env"
	@echo ""
	@echo "Testing:"
	@echo "  make test               - Run reserve/release test (5 requests)"
	@echo "  make test-queue-processor-sim - Offline queue drain simulation (no AWS)"
	@echo "  make test-budget-manager-sim  - Offline admission-gate simulation (no AWS)"
	@echo "  make test-budget-manager - Load test via semaphore (uses config.env defaults)"
	@echo "  make test-budget-manager ARGS='--model nova-2-lite --num-requests 50' - With custom parameters"
	@echo "  make test-direct-bedrock - Baseline throttling test, no retry (uses config.env defaults)"
	@echo "  make test-direct-bedrock ARGS='--model opus-5 --num-requests 100' - With custom parameters"
	@echo "  make test-direct-retry   - Baseline: retry + full jitter (customer status quo)"
	@echo "  make test-direct-leaky ARGS='--model nova-lite --tpm-limit 4000000 --prompt-size 60000' - Baseline: client-side leaky bucket"
	@echo "  make test-direct-retry  - Baseline with retry+jitter (customer status quo)"
	@echo "  make test-direct-retry ARGS='--model nova-lite --num-requests 200' - High-volume retry test"
	@echo "  make test-stale-lock    - Test stale lock recovery"
	@echo "  make soak-test          - Soak test (sustained RPM + adversarial injection)"
	@echo "  make soak-test ARGS='--model nova-lite --target-rpm 70 --duration-hours 1' - Quick soak"
	@echo "  make test-multi-model   - Multi-model contention test (Opus + Jamba + Nova Lite)"
	@echo "  make analyze-soak ARGS='soak_results.json' - Analyze soak results"
	@echo "  make analyze-soak ARGS='--cloudwatch --hours 72' - Pull CloudWatch metrics"
	@echo "  make test-all           - Run all tests"
	@echo "  make check-queue        - Check queue depth (legacy table)"
	@echo ""
	@echo "Inspection (e.g. MODEL=nova-2-lite, MODEL=sonnet-5):"
	@echo "  make inspect-config         - View model configuration"
	@echo "  make inspect-queue-single   - View single table queue items"
	@echo "  make inspect-consumption    - View burst capacity consumption"
	@echo "  make inspect-queue-capacity - View queue capacity consumption"
	@echo "  make inspect-tpm-consumption - View TPM token consumption (burst + queue)"
	@echo "  make inspect-queue          - View legacy queue table items"
	@echo ""
	@echo "Monitoring:"
	@echo "  make tail-budget         - Tail Budget Manager logs (follows)"
	@echo "  make tail-queue          - Tail Queue Processor logs (follows)"
	@echo "  make tail-bedrock        - Tail Bedrock Processor logs (follows)"
	@echo "  make logs-recent         - View recent logs (last 10 min, all lambdas)"
	@echo "  make logs-budget-recent  - View Budget Manager logs (last 10 min)"
	@echo "  make logs-queue-recent   - View Queue Processor logs (last 10 min)"
	@echo "  make logs-bedrock-recent - View Bedrock Processor logs (last 10 min)"
	@echo "  make logs-errors         - Filter error messages (last 30 min)"
	@echo ""
	@echo "Configuration:"
	@echo "  make create-config MODEL=nova-2-lite              - Create model config with defaults"
	@echo "  make create-config MODEL=nova-2-lite RPM=10 BURST_CAPACITY=2 - Custom low burst (watch queueing)"
	@echo "  make create-config MODEL=nova-2-lite RPM=2000 COUNTER_SHARDS=5 - Set RPM and shards"
	@echo "  make create-config MODEL=nova-lite BURST_CAPACITY=5 ADAPTIVE_SHIFT_MAX=0.2 ADAPTIVE_QUEUE_THRESHOLD=10"
	@echo "  make set-capacity CAPACITY=2            - Set token bucket capacity (without redeployment)"
	@echo "  make get-capacity                       - Get current token bucket capacity"
	@echo ""
	@echo "Dead Letter Queue:"
	@echo "  make inspect-dlq        - View messages in the Dead Letter Queue"
	@echo "  make drain-dlq          - Purge all messages from the Dead Letter Queue"
	@echo ""
	@echo "Dashboard:"
	@echo "  make dashboard          - Launch test initiation dashboard (http://localhost:8080)"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean              - Clean up DynamoDB tables"
	@echo ""

# Initial setup - create venv, install dependencies, deploy
setup:
	@echo "Running initial setup..."
	@./deploy.sh --init

# Deploy CDK and update config.env
deploy:
	@echo "Redeploying CDK stack..."
	@./deploy.sh

# Run the reserve/release test
test:
	@echo "Running reserve/release test..."
	@source .venv/bin/activate && ./scripts/test_reserve_release.sh

# Check queue depth
check-queue:
	@source .venv/bin/activate && python scripts/check_queue_depth.py

# Attribute admission-gate TransactionConflicts to the specific transact item
# (rate2s / tok2s / tpm_window / ...) and classify sheds as contention vs cap_breach.
analyze-conflicts:
	@source .venv/bin/activate && python scripts/analyze_conflict_attribution.py $(ARGS)

# Tail Budget Manager logs
tail-budget:
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	aws logs tail $$BUDGET_MANAGER_LOG_GROUP --since 5m --format short --follow

# Tail Queue Processor logs
tail-queue:
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	aws logs tail $$QUEUE_PROCESSOR_LOG_GROUP --since 5m --format short --follow

# Tail Bedrock Processor logs
tail-bedrock:
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	aws logs tail $$BEDROCK_PROCESSOR_LOG_GROUP --since 5m --format short --follow

# Inspection & Debugging
inspect-config:
	@echo "⚙️  Inspecting model configuration..."
	@if [ -z "$(MODEL)" ]; then \
		source .venv/bin/activate && python scripts/inspect_single_table.py --config --model opus; \
	else \
		source .venv/bin/activate && python scripts/inspect_single_table.py --config --model "$(MODEL)"; \
	fi

inspect-queue:
	@echo "� Inspecting queue (legacy table)..."
	@source .venv/bin/activate && python scripts/check_queue_depth.py

inspect-queue-single:
	@echo "📋 Inspecting queue (single table)..."
	@if [ -z "$(MODEL)" ]; then \
		source .venv/bin/activate && python scripts/inspect_single_table.py --queue --model opus; \
	else \
		source .venv/bin/activate && python scripts/inspect_single_table.py --queue --model "$(MODEL)"; \
	fi

inspect-consumption:
	@echo "� Inspecting burst consumption records..."
	@if [ -z "$(MODEL)" ]; then \
		source .venv/bin/activate && python scripts/inspect_single_table.py --consumption --model opus --capacity-mode BURST; \
	else \
		source .venv/bin/activate && python scripts/inspect_single_table.py --consumption --model "$(MODEL)" --capacity-mode BURST; \
	fi

inspect-queue-capacity:
	@echo "📊 Inspecting queue capacity consumption..."
	@if [ -z "$(MODEL)" ]; then \
		source .venv/bin/activate && python scripts/inspect_single_table.py --consumption --model opus --capacity-mode QUEUE; \
	else \
		source .venv/bin/activate && python scripts/inspect_single_table.py --consumption --model "$(MODEL)" --capacity-mode QUEUE; \
	fi

inspect-tpm-consumption:
	@echo "📊 Inspecting TPM consumption (estimated_tokens in records)..."
	@if [ -z "$(MODEL)" ]; then \
		source .venv/bin/activate && python scripts/inspect_single_table.py --consumption --model opus --capacity-mode BURST --show-tpm; \
	else \
		source .venv/bin/activate && python scripts/inspect_single_table.py --consumption --model "$(MODEL)" --capacity-mode BURST --show-tpm; \
	fi

# Offline Simulations (no AWS required)
test-queue-processor-sim:
	@echo "🧪 Running queue processor dispatch simulation (offline, no AWS)..."
	@source .venv/bin/activate && python scripts/test_queue_processor_sim.py $(ARGS)

test-budget-manager-sim:
	@echo "🧪 Running budget manager admission-gate simulation (offline, no AWS)..."
	@source .venv/bin/activate && python scripts/test_budget_manager_sim.py $(ARGS)

# Load Testing
test-budget-manager:
	@echo "Running Budget Manager load test..."
	@source .venv/bin/activate && python scripts/test_budget_manager.py $(ARGS)

test-stale-lock: ## Test stale lock recovery
	@echo "Testing stale lock recovery..."
	@source .venv/bin/activate && ./scripts/test_stale_lock_recovery.sh

soak-test:
	@echo "Running soak test (sustained traffic + adversarial injection)..."
	@source .venv/bin/activate && python scripts/soak_test.py $(ARGS)

test-multi-model:
	@echo "Running multi-model contention test..."
	@source .venv/bin/activate && python scripts/test_multi_model.py $(ARGS)

analyze-soak:
	@echo "Analyzing soak test results..."
	@source .venv/bin/activate && python scripts/analyze_soak_results.py $(ARGS)

test-direct-bedrock:
	@echo "🧪 Running direct Bedrock test (baseline, no retry)..."
	@source .venv/bin/activate && python scripts/test_direct_bedrock.py $(ARGS)

test-direct-retry:
	@echo "🧪 Running direct Bedrock test (retry + jitter baseline)..."
	@source .venv/bin/activate && python scripts/test_direct_bedrock_retry.py $(ARGS)

test-direct-leaky:
	@echo "🧪 Running direct Bedrock test (client-side leaky bucket baseline)..."
	@source .venv/bin/activate && python scripts/test_direct_bedrock_leaky.py $(ARGS)

test-all:
	@echo "🧪 Running all tests..."
	@make test
	@make test-budget-manager
	@make test-direct-bedrock
	@make test-direct-retry

# Monitoring
logs-recent:
	@echo "📜 Viewing recent logs (last 10 minutes)..."
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	echo "\n=== Budget Manager ===" && \
	aws logs tail $$BUDGET_MANAGER_LOG_GROUP --since 10m --format short | tail -20 && \
	echo "\n=== Queue Processor ===" && \
	aws logs tail $$QUEUE_PROCESSOR_LOG_GROUP --since 10m --format short | tail -20

logs-budget-recent:
	@echo "📜 Budget Manager logs (last 10 minutes)..."
	@source .venv/bin/activate && source config.env && \
	aws logs tail $$BUDGET_MANAGER_LOG_GROUP --since 10m --format short

logs-queue-recent:
	@echo "📜 Queue Processor logs (last 10 minutes)..."
	@source .venv/bin/activate && source config.env && \
	aws logs tail $$QUEUE_PROCESSOR_LOG_GROUP --since 10m --format short

logs-bedrock-recent:
	@echo "📜 Bedrock Processor logs (last 10 minutes)..."
	@source .venv/bin/activate && source config.env && \
	aws logs tail $$BEDROCK_PROCESSOR_LOG_GROUP --since 10m --format short

logs-errors:
	@echo "⚠️  Filtering error logs..."
	@source .venv/bin/activate && source config.env && \
	echo "\n=== Budget Manager Errors ===" && \
	aws logs tail $$BUDGET_MANAGER_LOG_GROUP --since 30m --format short | grep -i "error\|exception\|failed" || echo "No errors found" && \
	echo "\n=== Queue Processor Errors ===" && \
	aws logs tail $$QUEUE_PROCESSOR_LOG_GROUP --since 30m --format short | grep -i "error\|exception\|failed" || echo "No errors found"

# Configuration Management
set-capacity:
	@if [ -z "$(CAPACITY)" ]; then \
		echo "❌ Error: CAPACITY not specified"; \
		echo "Usage: make set-capacity CAPACITY=2"; \
		echo ""; \
		echo "Examples:"; \
		echo "  make set-capacity CAPACITY=2   # 2 tokens (test queueing with 'make test')"; \
		echo "  make set-capacity CAPACITY=50  # 50 tokens (production capacity)"; \
		exit 1; \
	fi
	@echo "Setting BURST_CAPACITY to $(CAPACITY) tokens..."
	@source .venv/bin/activate && python scripts/update_burst_capacity.py $(CAPACITY)

get-capacity:
	@echo "Getting current BURST_CAPACITY..."
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	aws lambda get-function-configuration --function-name $$(basename $$BUDGET_MANAGER_ARN) \
		--query 'Environment.Variables.BURST_CAPACITY' --output text

create-config:
	@if [ -z "$(MODEL)" ]; then \
		echo "❌ Error: MODEL not specified"; \
		echo "Usage: make create-config MODEL=nova-2-lite"; \
		echo "       make create-config MODEL=nova-2-lite RPM=10 BURST_CAPACITY=2"; \
		echo ""; \
		echo "Examples:"; \
		echo "  make create-config MODEL=nova-2-lite         # Model defaults"; \
		echo "  make create-config MODEL=nova-2-lite RPM=10 BURST_CAPACITY=2  # Test queueing"; \
		echo "  make create-config MODEL=sonnet-5            # Token-only model"; \
		echo "  make create-config MODEL=nova-lite BURST_CAPACITY=5 ADAPTIVE_SHIFT_MAX=0.2  # Adaptive"; \
		echo "  make create-config MODEL=nova-2-lite RPM=2000 COUNTER_SHARDS=5  # Set RPM + shards"; \
		exit 1; \
	fi
	@CMD="source .venv/bin/activate && python scripts/create_model_config.py $(MODEL)"; \
	if [ -n "$(BURST_CAPACITY)" ]; then CMD="$$CMD --burst-capacity $(BURST_CAPACITY)"; fi; \
	if [ -n "$(RPM)" ]; then CMD="$$CMD --rpm $(RPM)"; fi; \
	if [ -n "$(TPM)" ]; then CMD="$$CMD --tpm $(TPM)"; fi; \
	if [ -n "$(COUNTER_SHARDS)" ]; then CMD="$$CMD --counter-shards $(COUNTER_SHARDS)"; fi; \
	if [ -n "$(BURST_FRACTION)" ]; then CMD="$$CMD --burst-fraction $(BURST_FRACTION)"; fi; \
	if [ -n "$(QUEUE_FRACTION)" ]; then CMD="$$CMD --queue-fraction $(QUEUE_FRACTION)"; fi; \
	if [ -n "$(QUEUE_TARGET_TPM)" ]; then CMD="$$CMD --queue-target-tpm $(QUEUE_TARGET_TPM)"; fi; \
	if [ -n "$(ADAPTIVE_SHIFT_MAX)" ]; then CMD="$$CMD --adaptive-shift-max $(ADAPTIVE_SHIFT_MAX)"; fi; \
	if [ -n "$(ADAPTIVE_QUEUE_THRESHOLD)" ]; then CMD="$$CMD --adaptive-queue-threshold $(ADAPTIVE_QUEUE_THRESHOLD)"; fi; \
	if [ -n "$(MAX_BURST_MULTIPLIER)" ]; then CMD="$$CMD --max-burst-multiplier $(MAX_BURST_MULTIPLIER)"; fi; \
	echo "Creating config for $(MODEL)..."; \
	eval $$CMD

# Dead Letter Queue
inspect-dlq:
	@echo "📋 Inspecting Dead Letter Queue..."
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	aws sqs receive-message --queue-url "$$DLQ_URL" --max-number-of-messages 10 \
		--attribute-names All --message-attribute-names All \
		--visibility-timeout 0 2>/dev/null | python3 -m json.tool || echo "No messages in DLQ"

drain-dlq:
	@echo "🗑️  Purging Dead Letter Queue..."
	@if [ ! -f config.env ]; then \
		echo "❌ config.env not found. Run 'make setup' first."; \
		exit 1; \
	fi; \
	source .venv/bin/activate && \
	source config.env && \
	aws sqs purge-queue --queue-url "$$DLQ_URL" && \
	echo "✅ DLQ purged"

# Test Dashboard
dashboard:
	@echo "Launching Test Dashboard..."
	@source .venv/bin/activate && python scripts/test_dashboard.py

# Clean up DynamoDB tables
clean:
	@echo "Cleaning up DynamoDB tables..."
	@source .venv/bin/activate && python scripts/cleanup_tables.py
