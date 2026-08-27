"""CDK stack definition for Semaphore Rate Limiter infrastructure."""

import os
from pathlib import Path
from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_kms as kms,
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs as logs,
    aws_apigateway as apigw,
    aws_wafv2 as wafv2,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_lambda_destinations as lambda_destinations,
    aws_lambda_event_sources as lambda_event_sources,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    RemovalPolicy,
    Duration,
    CfnOutput,
)
from constructs import Construct
from cdk_nag import NagSuppressions
# Shared INTERNAL helper: enforces a real (>=15 char, non-placeholder)
# justification on every waiver. We reuse its validator to gate the
# stack-wide entries below (see the suppression block for the rationale
# on why these specific rules stay stack-scoped rather than per-resource).
from nag_suppressions import _assert_justified


def _checkov_skip(resource, *skips: "tuple[str, str]") -> None:
    """Emit checkov skip Metadata onto a construct's synthesized L1 resource.

    ASH/checkov scans the synthesized CloudFormation template
    (cdk.out/*.template.json), NOT the Python source — so a bare
    ``# checkov:skip=<ID>:<reason>`` code comment never registers. The skip must
    live in the resource's CFN ``Metadata.checkov.skip`` block to take effect,
    which is what this helper writes. Each skip is a (checkov_id, reason) pair;
    the reason is validated (>=15 real chars, non-placeholder) via the shared
    nag-suppression justification gate so a lazy waiver fails synth.
    """
    for _id, _reason in skips:
        _assert_justified({"id": "AwsSolutions-checkov-shim", "reason": _reason})
    resource.node.default_child.add_metadata(
        "checkov",
        {"skip": [{"id": _id, "comment": _reason} for _id, _reason in skips]},
    )


class SemaphoreRateLimiterStack(Stack):
    """CDK stack for the Semaphore Rate Limiter system - Phase 1 MVP."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Lambda handler directory
        lambda_dir = Path(__file__).parent / "lambda_handlers"

        # Lambda Layer - Shared service code for all handlers
        layer_dir = Path(__file__).parent / "lambda_layer"

        shared_service_layer = lambda_.LayerVersion(
            self, "SharedServiceLayer",
            code=lambda_.Code.from_asset(str(layer_dir)),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Shared service layer for DynamoDB operations (Phase 1: Hello World)",
            layer_version_name="semaphore-shared-service",
        )

        # ============================================================
        # KMS — customer-managed CMK for data-at-rest (Palisade Sev-3 fix)
        # ============================================================
        # Amazon Palisade flags AWS-managed / S3-managed encryption as an advisory
        # (Missing Encryption at Rest with a customer-managed KMS key). Resolve — not
        # suppress — by provisioning one CMK and encrypting both the single table and
        # the inference-output bucket with it. Rotation is enabled. The CDK grant_*
        # helpers (grant_read_write_data / bucket.grant_read) automatically add the
        # kms:Decrypt / kms:GenerateDataKey permissions to each consuming Lambda role.
        data_key = kms.Key(
            self, "DataAtRestKey",
            description="Semaphore shaper: CMK for table, bucket, log groups, DLQ + Lambda env",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Allow the CloudWatch Logs service to use the CMK so CMK-encrypted log
        # groups (CKV_AWS_158 fix below) can be written. Scoped by the standard
        # kms:EncryptionContext condition to log-group ARNs in THIS account/region;
        # the ArnLike wildcard (not a per-group ref) keeps the key policy free of a
        # circular dependency on the log groups it protects.
        data_key.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal(f"logs.{self.region}.amazonaws.com")],
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:Describe*",
                ],
                resources=["*"],
                conditions={
                    "ArnLike": {
                        "kms:EncryptionContext:aws:logs:arn":
                            f"arn:aws:logs:{self.region}:{self.account}:log-group:*"
                    }
                },
            )
        )

        # ============================================================
        # DynamoDB Single Table - Leaky bucket design
        # ============================================================

        # Single Table
        single_table = dynamodb.Table(
            self, "SingleTable",
            table_name="semaphore-single-table",
            partition_key=dynamodb.Attribute(
                name="pk",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="sk",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
            # Honest-outcomes (OBJ3): NEW_AND_OLD_IMAGES stream feeds OutcomeStreamFn,
            # the SOLE emitter of the RequestOutcome EMF metric (Cato C-2: an inline
            # writer-emit is only at-most-once; projecting the committed
            # PENDING|QUEUED->terminal transition off the stream makes the metric
            # liveness-independent). Enabling a stream on an existing table is an
            # in-place UpdateTable (no replacement) — verified against the live table
            # (Phase-0 Gate 1: Stream was null pre-change).
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            # Encryption at rest: customer-managed KMS CMK (data_key above). Switching
            # SSE from AWS_MANAGED to CUSTOMER_MANAGED is an in-place UpdateTable, not a
            # replacement (verified via changeset). Resolves the Palisade Sev-3.
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=data_key,
            # Use the non-deprecated PITR specification (point_in_time_recovery bool is
            # deprecated in aws-cdk-lib and slated for removal in the next major).
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
        )

        # ============================================================
        # S3 — inference-output bucket (honest-outcomes OBJ3)
        # ============================================================
        # bedrock_processor writes the SUCCESS body here (key outputs/{request_id}.json)
        # and stores only the s3:// URI as output_ref on the terminal-status item —
        # keeps the DDB item small and dodges the 400KB item cliff (Cato C-3). ResultFn
        # presigns a GET on /result. Bodies are transient (2-day lifecycle expiry).
        outcome_output_bucket = s3.Bucket(
            self, "OutcomeOutputBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=data_key,
            bucket_key_enabled=True,
            enforce_ssl=True,
            # CKV_AWS_21: object versioning enabled. Paired with a noncurrent-version
            # expiry so versioning does not accumulate transient bodies indefinitely.
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[s3.LifecycleRule(
                id="expire-transient-outputs",
                expiration=Duration.days(2),
                noncurrent_version_expiration=Duration.days(2),
                enabled=True,
            )],
        )
        # CKV_AWS_18 (S3 access logging): skipped, consistent with the cdk-nag
        # AwsSolutions-S1 waiver below. This bucket holds only transient (2-day)
        # inference-response bodies for an internal load-test prototype; enabling
        # server access logging would add a second self-logging log bucket + cost
        # for no operational value here. Enable before any production promotion.
        _checkov_skip(
            outcome_output_bucket,
            ("CKV_AWS_18", "Transient 2-day inference bodies on an internal prototype; "
                           "server access logging deliberately off to avoid a second "
                           "log bucket + cost, consistent with the cdk-nag S1 waiver."),
        )

        # ============================================================
        # SQS Dead Letter Queue — Failed request recovery
        # ============================================================
        # Declared before the Lambda functions so the async-invoked handlers can
        # attach it as their DeadLetterConfig (CKV_AWS_116) at construction time.
        dlq = sqs.Queue(
            self, "BedrockProcessorDLQ",
            queue_name="bedrock-processor-dlq",
            retention_period=Duration.days(14),
            visibility_timeout=Duration.seconds(30),
            removal_policy=RemovalPolicy.DESTROY,
            # AwsSolutions-SQS4: deny non-TLS transport. CDK appends an
            # aws:SecureTransport=false Deny statement to the queue policy.
            enforce_ssl=True,
            # CKV_AWS_27: CMK-encrypt the queue at rest with the stack CMK.
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=data_key,
        )

        # ============================================================
        # Lambda Functions - Simple hardcoded responses
        # ============================================================

        # Bedrock Processor Lambda - Calls Bedrock and sends Step Functions callback
        bedrock_processor_log_group = logs.LogGroup(
            self, "BedrockProcessorLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )

        # Lambda concurrency (CDK context overridable). A `reserved_concurrent_executions`
        # value is both a FLOOR and a CEILING: reserving N guarantees N but also caps the
        # function at N. Iterating on the 2026-07-09 5x load test showed that capping the
        # synchronous admission gate is counterproductive under an extreme instantaneous
        # burst — at 200 it throttled hard (49.8% failures), at 1000 it improved to 71%
        # success but still hit its own ceiling. Since the account has 5000 concurrency
        # (4390 unreserved), the admission gate performs best drawing from the FULL
        # unreserved pool rather than a fixed reservation. So the default is now 0 =
        # "no reservation" (unreserved, full-pool access); set a positive value via
        # context only if you need a guaranteed floor in a shared/constrained account.
        #
        # KNOWN LIMITATION (documented, not a bug): a synchronous per-request admission
        # Lambda has a finite burst-absorption ceiling. Under a deliberately extreme
        # instantaneous slam (100 workers firing ~7k StartExecutions in 60s = 5x quota),
        # some requests will still see Lambda.TooManyRequestsException even unreserved.
        # This shaper is designed for QUOTA overflow (queue what exceeds the model's TPM),
        # not for absorbing an unbounded Lambda invocation burst. For true burst
        # absorption, front the admission path with SQS or API Gateway throttling
        # (see docs/design/) — deferred as out-of-scope for the quota-shaping mission.
        bedrock_processor_reserved = int(
            self.node.try_get_context("bedrock_processor_reserved_concurrency") or 0
        )
        budget_manager_reserved = int(
            self.node.try_get_context("budget_manager_reserved_concurrency") or 0
        )
        # 0 → None so CDK OMITS reserved_concurrent_executions (unreserved, full-pool).
        # A literal 0 would instead DISABLE the function — not what we want.
        bedrock_processor_reserved = bedrock_processor_reserved or None
        budget_manager_reserved = budget_manager_reserved or None

        # Lambda memory (also scales CPU). The 2026-07-06 5x extreme-spike test
        # deadlocked partly because the Budget Manager admission gate ran at the 128MB
        # default and got CPU-starved doing TransactWriteItems + conflict retries under
        # contention, timing out at 30s (Sandbox.Timedout). 128MB gives a fraction of a
        # vCPU; 1024MB gives ~0.6 vCPU. Bump the admission gate to 1024MB and the
        # processors to 512MB so the compute tier is not the bottleneck the hot-partition
        # fix is trying to remove. Context-overridable.
        budget_manager_memory = int(
            self.node.try_get_context("budget_manager_memory_mb") or 1024
        )
        processor_memory = int(
            self.node.try_get_context("processor_memory_mb") or 512
        )

        bedrock_processor_lambda = lambda_.Function(
            self, "BedrockProcessorFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="bedrock_processor.handler",
            code=lambda_.Code.from_asset(str(lambda_dir)),
            timeout=Duration.minutes(5),
            memory_size=processor_memory,
            reserved_concurrent_executions=bedrock_processor_reserved,
            # CKV_AWS_173: encrypt environment variables with the stack CMK.
            environment_encryption=data_key,
            # CKV_AWS_116: this function is invoked asynchronously (InvocationType
            # 'Event') by the budget/queue processors — give it a DeadLetterConfig
            # in addition to the on_failure EventInvokeConfig destination wired below.
            dead_letter_queue=dlq,
            environment={
                'SINGLE_TABLE_NAME': single_table.table_name,
                # Pre-invoke arrival jitter (ms). Re-spreads async-delivery-bunched
                # invocations across the second so they don't clump against Bedrock's
                # sub-minute token bucket. CDK-context overridable
                # (-c bedrock_invoke_jitter_ms=N) so the window can be swept without
                # a code change. See bedrock_processor.BEDROCK_INVOKE_JITTER_MS.
                'BEDROCK_INVOKE_JITTER_MS': str(
                    self.node.try_get_context("bedrock_invoke_jitter_ms") or 250
                ),
            },
            log_group=bedrock_processor_log_group,
            layers=[shared_service_layer],
        )
        # checkov:skip=CKV_AWS_117 checkov:skip=CKV_AWS_115 (see CFN Metadata below)
        _checkov_skip(
            bedrock_processor_lambda,
            ("CKV_AWS_117", "Reference impl has no VPC resources to reach; running "
                            "outside a VPC is the intended design for this sample."),
            ("CKV_AWS_115", "Admission-path function intentionally unbounded: it must "
                            "draw from the full unreserved concurrency pool under burst; "
                            "a reserved ceiling throttled throughput in 5x load testing."),
        )

        # Grant Bedrock permissions to Bedrock Processor
        # Converse API needs both foundation-model/* and inference-profile/*
        # (us.* prefixed model IDs route through inference profiles)
        bedrock_processor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    "arn:aws:bedrock:*:*:foundation-model/*",
                    "arn:aws:bedrock:*:*:inference-profile/*",
                    f"arn:aws:bedrock:*:{self.account}:inference-profile/*",
                ]
            )
        )

        # The Bedrock Processor reads the model CONFIG row (to pick runtime vs mantle
        # backend), writes consumption actuals reconciliation, and re-enqueues burst
        # requests on throttle, so it needs table read/write. Previously config was
        # passed via the event, so it had no table access at all.
        single_table.grant_read_write_data(bedrock_processor_lambda)

        # Honest-outcomes: the processor persists the SUCCESS body to S3 and writes
        # the terminal-status item (via the shared DynamoService helper on the table
        # it already read/writes). Grant S3 write + expose the bucket name.
        outcome_output_bucket.grant_write(bedrock_processor_lambda)
        bedrock_processor_lambda.add_environment("OUTPUT_BUCKET", outcome_output_bucket.bucket_name)

        # Grant Step Functions callback permissions to Bedrock Processor
        # Note: State machine is defined later in this stack, so we use an
        # account-scoped ARN pattern to avoid a circular reference.
        bedrock_processor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:SendTaskSuccess", "states:SendTaskFailure"],
                resources=[f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"]
            )
        )
        bedrock_processor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:DescribeExecution"],
                resources=[f"arn:aws:states:{self.region}:{self.account}:execution:*"]
            )
        )

        # Budget Manager Lambda - Handles reserve/release
        budget_manager_log_group = logs.LogGroup(
            self, "BudgetManagerLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )

        budget_manager_lambda = lambda_.Function(
            self, "BudgetManagerFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="budget_manager.handler",
            code=lambda_.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
            memory_size=budget_manager_memory,
            reserved_concurrent_executions=budget_manager_reserved,
            # CKV_AWS_173: encrypt environment variables with the stack CMK.
            environment_encryption=data_key,
            environment={
                'SINGLE_TABLE_NAME': single_table.table_name,
                'BEDROCK_PROCESSOR_ARN': bedrock_processor_lambda.function_arn,
            },
            log_group=budget_manager_log_group,
            layers=[shared_service_layer],
        )
        # checkov:skip=CKV_AWS_117 checkov:skip=CKV_AWS_115 checkov:skip=CKV_AWS_116
        _checkov_skip(
            budget_manager_lambda,
            ("CKV_AWS_117", "Reference impl has no VPC resources to reach; running "
                            "outside a VPC is the intended design for this sample."),
            ("CKV_AWS_115", "Synchronous admission gate intentionally unbounded so it "
                            "can draw from the full unreserved concurrency pool under "
                            "burst; a reserved ceiling deadlocked it in 5x load testing."),
            ("CKV_AWS_116", "Invoked synchronously by Step Functions (waitForTaskToken); "
                            "an async DeadLetterConfig has no meaning for this call path."),
        )

        single_table.grant_read_write_data(budget_manager_lambda)

        # Grant EventBridge permissions to Budget Manager (for triggering Queue Processor).
        # Scoped to the default event bus ARN — EventBridge DOES support resource-level
        # permissions on PutEvents via the event-bus ARN (the earlier "no resource-level
        # support" comment was outdated).
        default_event_bus_arn = f"arn:aws:events:{self.region}:{self.account}:event-bus/default"
        budget_manager_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["events:PutEvents"],
                resources=[default_event_bus_arn]
            )
        )

        # Grant permission to invoke Bedrock Processor
        bedrock_processor_lambda.grant_invoke(budget_manager_lambda)

        # Grant Step Functions callback permissions to Budget Manager (for queued path)
        # Note: State machine is defined later in this stack, so we use an
        # account-scoped ARN pattern to avoid a circular reference.
        budget_manager_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:SendTaskSuccess", "states:SendTaskFailure"],
                resources=[f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"]
            )
        )

        # Queue Processor Lambda - Processes queued requests
        queue_processor_log_group = logs.LogGroup(
            self, "QueueProcessorLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )
        
        queue_processor_lambda = lambda_.Function(
            self, "QueueProcessorFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="queue_processor.handler",
            code=lambda_.Code.from_asset(str(lambda_dir)),
            timeout=Duration.minutes(15),
            memory_size=processor_memory,
            # CKV_AWS_173: encrypt environment variables with the stack CMK.
            environment_encryption=data_key,
            # CKV_AWS_116: invoked asynchronously by the EventBridge rule below;
            # capture pre/post-handler async failures to the shared DLQ.
            dead_letter_queue=dlq,
            environment={
                'SINGLE_TABLE_NAME': single_table.table_name,
                'BEDROCK_PROCESSOR_ARN': bedrock_processor_lambda.function_arn,
            },
            log_group=queue_processor_log_group,
            layers=[shared_service_layer],
        )
        # checkov:skip=CKV_AWS_117 checkov:skip=CKV_AWS_115 (see CFN Metadata below)
        _checkov_skip(
            queue_processor_lambda,
            ("CKV_AWS_117", "Reference impl has no VPC resources to reach; running "
                            "outside a VPC is the intended design for this sample."),
            ("CKV_AWS_115", "Queue-drain path intentionally unbounded so it can draw "
                            "from the full unreserved concurrency pool; a reserved "
                            "ceiling caps drain throughput under backlog."),
        )

        single_table.grant_read_write_data(queue_processor_lambda)
        
        # Grant EventBridge permissions to Queue Processor (for self-triggering).
        # Scoped to the default event bus ARN (EventBridge supports resource-level
        # PutEvents permissions via the event-bus ARN).
        queue_processor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["events:PutEvents"],
                resources=[default_event_bus_arn]
            )
        )
        
        # Grant Bedrock permissions to Queue Processor (for invoking models)
        # Converse API needs both foundation-model/* and inference-profile/*
        queue_processor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    "arn:aws:bedrock:*:*:foundation-model/*",
                    "arn:aws:bedrock:*:*:inference-profile/*",
                    f"arn:aws:bedrock:*:{self.account}:inference-profile/*",
                ]
            )
        )

        # Tier 2: bedrock-mantle access, behind the `enable_mantle` CDK context flag
        # (default off). Mantle uses a DISTINCT IAM action (bedrock-mantle:CreateInference),
        # not bedrock:InvokeModel. Granted to both lambdas that call Bedrock. Runtime path
        # is unaffected when the flag is absent.
        if self.node.try_get_context("enable_mantle"):
            for _lam in (bedrock_processor_lambda, queue_processor_lambda):
                _lam.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=["bedrock-mantle:CreateInference"],
                        resources=[f"arn:aws:bedrock-mantle:*:{self.account}:project/*"],
                    )
                )

        # Grant permission for Queue Processor to invoke Bedrock Processor
        bedrock_processor_lambda.grant_invoke(queue_processor_lambda)

        # ============================================================
        # EventBridge Rule - Event-driven triggering
        # ============================================================
        
        # EventBridge rule for queue processing events
        queue_processor_rule = events.Rule(
            self, "QueueProcessorRule",
            enabled=True,
            event_pattern=events.EventPattern(
                source=["budget-manager", "queue-processor"],
                detail_type=["QueueProcessingRequired"]
            ),
            description="Trigger queue processor when Budget Manager enqueues requests"
        )
        
        queue_processor_rule.add_target(
            targets.LambdaFunction(queue_processor_lambda)
        )

        # ============================================================
        # Step Functions State Machine - With Callback Pattern
        # ============================================================

        # Reserve Budget task with waitForTaskToken
        # Budget Manager either:
        #   - Immediate path: Invokes Bedrock Processor async (sends callback with Bedrock response)
        #   - Queued path: Enqueues with task_token/execution_arn, Queue Processor invokes
        #     Bedrock Processor which resolves payload via describe_execution
        # Note: Passing entire input ($) so Lambda can extract request_payload if present,
        # or construct it from loose params (prompt, max_tokens, etc.)
        reserve_budget_task = tasks.LambdaInvoke(
            self, "ReserveBudget",
            lambda_function=budget_manager_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object({
                "action": "reserve",
                "request_id": sfn.JsonPath.string_at("$.request_id"),
                "model_id": sfn.JsonPath.string_at("$.model_id"),
                "input.$": "$",  # Pass entire input for flexible payload extraction
                "task_token": sfn.JsonPath.task_token,
                "execution_arn": sfn.JsonPath.string_at("$$.Execution.Id"),
            }),
            # Callback output becomes result directly - no result_selector needed
            result_path="$.budget_result",
        )

        # Success state - Bedrock response in budget_result (both immediate and queued paths)
        success_state = sfn.Succeed(
            self, "Success",
            comment="Workflow completed successfully - Bedrock response in budget_result"
        )

        # Failure state
        failure_state = sfn.Fail(
            self, "ExecutionFailed",
            comment="Workflow failed during processing"
        )

        # Retry on Lambda concurrency throttles before failing.
        # Covers the case where account-level Lambda concurrency is exhausted under
        # thundering herd conditions — SFN backs off and retries transparently rather
        # than hard-failing the execution.
        reserve_budget_task.add_retry(
            errors=["Lambda.TooManyRequestsException", "Lambda.SdkClientException"],
            interval=Duration.seconds(1),
            max_attempts=3,
            backoff_rate=2,
        )

        # Add error handling for reserve task
        reserve_budget_task.add_catch(
            failure_state,
            errors=["States.ALL"],
            result_path="$.error"
        )

        # Define workflow - simplified with callback pattern
        # Both immediate and queued paths complete via Bedrock Processor callback
        workflow_definition = reserve_budget_task.next(success_state)

        # Create state machine
        # SFN execution timeout MUST exceed the queue-item TTL (enqueue_request
        # expiry_hours=1 → 60 min) so a queued request's waitForTaskToken callback
        # token never dies before the item expires. Previously 30 min < 60 min TTL:
        # a backlogged request could be dequeued and its Bedrock call spent AFTER the
        # token expired, wasting spend and silently dropping the result. 65 > 60 closes
        # the race (the queue item TTL-expires and is swept before the token dies).
        state_machine = sfn.StateMachine(
            self, "SemaphoreWorkflow",
            definition_body=sfn.DefinitionBody.from_chainable(workflow_definition),
            timeout=Duration.minutes(65),
            # AwsSolutions-SF2: enable X-Ray active tracing for end-to-end
            # request tracing across the SFN -> Lambda -> Bedrock callback path.
            tracing_enabled=True,
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "StateMachineLogs",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY,
                    encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
                ),
                level=sfn.LogLevel.ALL,
            ),
        )

        # ============================================================
        # Honest-outcomes Lambdas (OBJ3): ResultFn, FinalizerFn, OutcomeStreamFn
        # ============================================================

        # -- OutcomeStreamFn: SOLE RequestOutcome EMF emitter, off the DDB stream --
        outcome_stream_log_group = logs.LogGroup(
            self, "OutcomeStreamLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )
        outcome_stream_lambda = lambda_.Function(
            self, "OutcomeStreamFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="outcome_stream_fn.handler",
            code=lambda_.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
            memory_size=256,
            # CKV_AWS_173: encrypt environment variables with the stack CMK.
            environment_encryption=data_key,
            # CKV_AWS_115: bounded reserved concurrency is safe here — this is a
            # DDB-stream poll consumer, not a burst-facing admission path.
            reserved_concurrent_executions=10,
            environment={'SINGLE_TABLE_NAME': single_table.table_name},
            log_group=outcome_stream_log_group,
            layers=[shared_service_layer],
        )
        # checkov:skip=CKV_AWS_117 checkov:skip=CKV_AWS_116 (see CFN Metadata below)
        _checkov_skip(
            outcome_stream_lambda,
            ("CKV_AWS_117", "Reference impl has no VPC resources to reach; running "
                            "outside a VPC is the intended design for this sample."),
            ("CKV_AWS_116", "Poll-based DDB stream consumer, not async-invoked: uses "
                            "bisect-batch-on-error + retry_attempts for failure handling, "
                            "so an async DeadLetterConfig does not apply."),
        )
        # Read the stream + emit EMF only (no table writes). DynamoEventSource wires
        # the stream-read IAM perms automatically.
        outcome_stream_lambda.add_event_source(
            lambda_event_sources.DynamoEventSource(
                single_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                bisect_batch_on_error=True,
                retry_attempts=3,
                report_batch_item_failures=False,
            )
        )

        # -- FinalizerFn: EventBridge SFN-status handler (FAILED/TIMED_OUT/ABORTED) --
        finalizer_log_group = logs.LogGroup(
            self, "FinalizerLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )
        finalizer_lambda = lambda_.Function(
            self, "FinalizerFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="finalizer_fn.handler",
            code=lambda_.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(30),
            memory_size=256,
            # CKV_AWS_173: encrypt environment variables with the stack CMK.
            environment_encryption=data_key,
            # CKV_AWS_115: bounded reserved concurrency is safe — low-volume
            # EventBridge terminal-status handler, not a burst-facing path.
            reserved_concurrent_executions=5,
            # CKV_AWS_116: invoked asynchronously by the EventBridge SFN-status
            # rule below; capture async failures to the shared DLQ.
            dead_letter_queue=dlq,
            environment={'SINGLE_TABLE_NAME': single_table.table_name},
            log_group=finalizer_log_group,
            layers=[shared_service_layer],
        )
        # checkov:skip=CKV_AWS_117 (see CFN Metadata below)
        _checkov_skip(
            finalizer_lambda,
            ("CKV_AWS_117", "Reference impl has no VPC resources to reach; running "
                            "outside a VPC is the intended design for this sample."),
        )
        # Reads current state (queue_expired vs timed_out) + writes terminal status.
        single_table.grant_read_write_data(finalizer_lambda)

        # EventBridge rule: only THIS state machine's terminal transitions, and only
        # the non-success statuses (SUCCEEDED is written by bedrock_processor before
        # send_task_success — there is deliberately NO success finalizer, design OBJ3).
        sfn_status_rule = events.Rule(
            self, "SfnTerminalStatusRule",
            enabled=True,
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": ["FAILED", "TIMED_OUT", "ABORTED"],
                    "stateMachineArn": [state_machine.state_machine_arn],
                },
            ),
            description="Route failed/timed-out/aborted executions to the honest-outcomes finalizer",
        )
        sfn_status_rule.add_target(targets.LambdaFunction(finalizer_lambda))

        # -- ResultFn: GET /result/{request_id} — GetItem + S3 presign only --
        result_log_group = logs.LogGroup(
            self, "ResultFnLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )
        result_lambda = lambda_.Function(
            self, "ResultFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="result_fn.handler",
            code=lambda_.Code.from_asset(str(lambda_dir)),
            timeout=Duration.seconds(10),
            memory_size=256,
            # CKV_AWS_173: encrypt environment variables with the stack CMK.
            environment_encryption=data_key,
            # CKV_AWS_115: generous bounded reservation — high enough not to throttle
            # concurrent /result polls, while still declaring an explicit ceiling.
            reserved_concurrent_executions=20,
            environment={
                'SINGLE_TABLE_NAME': single_table.table_name,
                'OUTPUT_BUCKET': outcome_output_bucket.bucket_name,
            },
            log_group=result_log_group,
            layers=[shared_service_layer],
        )
        # checkov:skip=CKV_AWS_117 checkov:skip=CKV_AWS_116 (see CFN Metadata below)
        _checkov_skip(
            result_lambda,
            ("CKV_AWS_117", "Reference impl has no VPC resources to reach; running "
                            "outside a VPC is the intended design for this sample."),
            ("CKV_AWS_116", "Synchronous request/response API handler (GET /result); "
                            "an async DeadLetterConfig has no meaning for this path."),
        )
        # Pure read path: GetItem on the status item + GetObject/presign on the body.
        # No states:* — the item stores no executionArn, so ARN leakage is impossible.
        single_table.grant_read_data(result_lambda)
        outcome_output_bucket.grant_read(result_lambda)

        # ============================================================
        # API Gateway - HTTP entry point for edge layer
        # ============================================================

        # IAM role for API Gateway to invoke Step Functions
        apigw_sfn_role = iam.Role(
            self, "ApiGatewayStepFunctionsRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
        )
        apigw_sfn_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        # Access-log destination for the REST API stage (AwsSolutions-APIG1).
        api_access_log_group = logs.LogGroup(
            self, "TrafficShaperApiAccessLogs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
            encryption_key=data_key,  # CKV_AWS_158: CMK-encrypt the log group
        )

        # REST API
        api = apigw.RestApi(
            self, "TrafficShaperApi",
            rest_api_name="Bedrock Traffic Shaper",
            description="HTTP entry point for the Bedrock Traffic Shaper",
            # Provision the account-level CloudWatch Logs role so stage
            # execution logging (APIG6) can write. cdk.json sets the
            # disableCloudWatchRole feature flag, so we opt in explicitly.
            cloud_watch_role=True,
            deploy_options=apigw.StageOptions(
                stage_name="prod",
                # AwsSolutions-APIG1: structured access logging.
                access_log_destination=apigw.LogGroupLogDestination(api_access_log_group),
                access_log_format=apigw.AccessLogFormat.json_with_standard_fields(
                    caller=True, http_method=True, ip=True, protocol=True,
                    request_time=True, resource_path=True, response_length=True,
                    status=True, user=True,
                ),
                # AwsSolutions-APIG6: per-method CloudWatch execution logging.
                logging_level=apigw.MethodLoggingLevel.INFO,
                metrics_enabled=True,
            ),
        )

        # POST /invoke — maps request body to StartExecution
        invoke_resource = api.root.add_resource("invoke")
        invoke_resource.add_method(
            "POST",
            apigw.AwsIntegration(
                service="states",
                action="StartExecution",
                integration_http_method="POST",
                options=apigw.IntegrationOptions(
                    credentials_role=apigw_sfn_role,
                    request_templates={
                        "application/json": (
                            '{\n'
                            '  "stateMachineArn": "' + state_machine.state_machine_arn + '",\n'
                            '  "input": "$util.escapeJavaScript($input.json(\'$\'))"\n'
                            '}'
                        ),
                    },
                    integration_responses=[
                        apigw.IntegrationResponse(
                            status_code="200",
                            response_templates={
                                "application/json": "$input.json('$')",
                            },
                        ),
                        # Cato C-1: StartExecution has its own ~1200/s account throttle.
                        # A ThrottlingException must surface as 429 (ingress_throttled),
                        # NOT be swallowed into the generic 4xx->400 below (which read as
                        # "validation" and hid the front-door overload). Order matters —
                        # APIGW uses the FIRST matching selection_pattern, so the specific
                        # throttle pattern precedes the generic 4xx.
                        apigw.IntegrationResponse(
                            status_code="429",
                            selection_pattern=".*(ThrottlingException|Throttling|TooManyRequests).*",
                            response_templates={
                                "application/json": '{"error":"ingress_throttled","message":"request rate exceeded at ingress; retry with backoff"}',
                            },
                        ),
                        apigw.IntegrationResponse(
                            status_code="400",
                            selection_pattern="4\\d{2}",
                        ),
                        apigw.IntegrationResponse(
                            status_code="500",
                            selection_pattern="5\\d{2}",
                        ),
                    ],
                ),
            ),
            method_responses=[
                apigw.MethodResponse(status_code="200"),
                apigw.MethodResponse(status_code="429"),
                apigw.MethodResponse(status_code="400"),
                apigw.MethodResponse(status_code="500"),
            ],
            authorization_type=apigw.AuthorizationType.IAM,
        )

        # GET /result/{request_id} — honest-outcomes poll endpoint (ResultFn, LAMBDA_PROXY).
        # Pure read: returns 202 (pending/queued), 200 + presigned output_url (succeeded),
        # or the honest failure code (429/503/504/400). IAM auth to match /invoke.
        result_resource = api.root.add_resource("result").add_resource("{request_id}")
        result_resource.add_method(
            "GET",
            apigw.LambdaIntegration(result_lambda, proxy=True),
            authorization_type=apigw.AuthorizationType.IAM,
        )

        # ============================================================
        # WAF WebACL - Per-tenant rate limiting (REGIONAL, on the API GW stage)
        # ============================================================

        waf_acl = wafv2.CfnWebACL(
            self, "TrafficShaperWaf",
            # REGIONAL scope so the web ACL can associate with the regional API
            # Gateway stage (was CLOUDFRONT when the ACL fronted a CloudFront dist).
            scope="REGIONAL",
            default_action=wafv2.CfnWebACL.DefaultActionProperty(allow={}),
            visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                cloud_watch_metrics_enabled=True,
                metric_name="TrafficShaperWaf",
                sampled_requests_enabled=True,
            ),
            name="TrafficShaperWaf",
            rules=[
                # Rule 1: Per-tenant rate limit (X-Tenant-ID header)
                wafv2.CfnWebACL.RuleProperty(
                    name="PerTenantRateLimit",
                    priority=1,
                    action=wafv2.CfnWebACL.RuleActionProperty(block={}),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        cloud_watch_metrics_enabled=True,
                        metric_name="PerTenantRateLimit",
                        sampled_requests_enabled=True,
                    ),
                    statement=wafv2.CfnWebACL.StatementProperty(
                        rate_based_statement=wafv2.CfnWebACL.RateBasedStatementProperty(
                            limit=100,
                            evaluation_window_sec=300,
                            aggregate_key_type="CUSTOM_KEYS",
                            custom_keys=[
                                wafv2.CfnWebACL.RateBasedStatementCustomKeyProperty(
                                    header=wafv2.CfnWebACL.RateLimitHeaderProperty(
                                        name="X-Tenant-ID",
                                        text_transformations=[
                                            wafv2.CfnWebACL.TextTransformationProperty(
                                                priority=0,
                                                type="NONE",
                                            )
                                        ],
                                    )
                                ),
                            ],
                            scope_down_statement=wafv2.CfnWebACL.StatementProperty(
                                size_constraint_statement=wafv2.CfnWebACL.SizeConstraintStatementProperty(
                                    field_to_match=wafv2.CfnWebACL.FieldToMatchProperty(
                                        single_header={"Name": "x-tenant-id"},
                                    ),
                                    comparison_operator="GT",
                                    size=0,
                                    text_transformations=[
                                        wafv2.CfnWebACL.TextTransformationProperty(
                                            priority=0,
                                            type="NONE",
                                        )
                                    ],
                                ),
                            ),
                        ),
                    ),
                ),
                # Rule 2: Per-IP fallback rate limit
                wafv2.CfnWebACL.RuleProperty(
                    name="PerIpRateLimit",
                    priority=2,
                    action=wafv2.CfnWebACL.RuleActionProperty(block={}),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        cloud_watch_metrics_enabled=True,
                        metric_name="PerIpRateLimit",
                        sampled_requests_enabled=True,
                    ),
                    statement=wafv2.CfnWebACL.StatementProperty(
                        rate_based_statement=wafv2.CfnWebACL.RateBasedStatementProperty(
                            limit=200,
                            evaluation_window_sec=300,
                            aggregate_key_type="IP",
                        ),
                    ),
                ),
            ],
        )

        # Associate the REGIONAL web ACL with the API Gateway prod stage. The stage
        # ARN uses the apigateway resource form (region + restapis/{id}/stages/{name},
        # note the empty account segment). Referencing api.deployment_stage both
        # yields the stage name and forces the association to depend on the stage.
        prod_stage = api.deployment_stage
        waf_association = wafv2.CfnWebACLAssociation(
            self, "TrafficShaperWafAssociation",
            resource_arn=(
                f"arn:aws:apigateway:{self.region}::/restapis/"
                f"{api.rest_api_id}/stages/{prod_stage.stage_name}"
            ),
            web_acl_arn=waf_acl.attr_arn,
        )
        # Belt-and-suspenders ordering: the stage must exist before the association
        # (the resource_arn string does not create the CFN dependency on its own).
        waf_association.node.add_dependency(prod_stage)

        # ============================================================
        # SQS Dead Letter Queue wiring — (queue declared earlier, above Lambdas)
        # ============================================================

        # Grant Bedrock Processor permission to send to DLQ
        dlq.grant_send_messages(bedrock_processor_lambda)

        # Add DLQ_URL to Bedrock Processor environment
        bedrock_processor_lambda.add_environment("DLQ_URL", dlq.queue_url)

        # Wire the SAME queue as the Lambda async on_failure destination. The handler
        # sends to the DLQ for failures it catches at runtime, but bedrock_processor is
        # invoked asynchronously (InvocationType='Event') and now has a reserved
        # concurrency ceiling — an event that is THROTTLED before the handler runs, or
        # that exhausts Lambda's async retries, would otherwise be dropped silently.
        # The on_failure destination captures those pre/post-handler async failures to
        # the same DLQ so nothing is lost without a trace. max_event_age + retry_attempts
        # bound how long Lambda retries before routing to the destination.
        bedrock_processor_lambda.configure_async_invoke(
            on_failure=lambda_destinations.SqsDestination(dlq),
            retry_attempts=2,
            max_event_age=Duration.hours(1),  # aligns with the 60-min queue TTL
        )

        # CloudWatch alarm: DLQ has messages (any failed request is critical)
        dlq_alarm = cw.Alarm(
            self, "DlqDepthAlarm",
            metric=dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=1,
            alarm_name="BedrockShaper-DLQ-NotEmpty",
            alarm_description="Dead letter queue has failed requests requiring attention",
        )

        # Leading-indicator alarms (Sprint 0.5)
        # NOTE: There is deliberately no absolute-queue-depth alarm. The old one read a
        # QueueDepth metric backed by a get_queue_depth() Select='COUNT' scan of the hot
        # QUEUE#ITEMS partition (RCU spike → ThrottlingException) — that emission and its
        # metric were retired. The derived replacement uses SEARCH() metric math, which
        # CloudWatch does NOT support on Metric Alarms ("SEARCH is not supported on
        # Metric Alarms"). Derived backlog is therefore shown on the "Derived Queue
        # Depth" dashboard widget (Row 1b) for observation; the drain control loop no
        # longer needs depth for any decision, so no alarm is required here.

        lambda_error_alarm = cw.Alarm(
            self, "LambdaErrorRateAlarm",
            metric=cw.MathExpression(
                expression="errors / invocations * 100",
                using_metrics={
                    "errors": budget_manager_lambda.metric_errors(period=Duration.minutes(1)),
                    "invocations": budget_manager_lambda.metric_invocations(period=Duration.minutes(1)),
                },
                period=Duration.minutes(1),
            ),
            threshold=5,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=3,
            alarm_name="BedrockShaper-LambdaErrorRate",
            alarm_description="Budget Manager error rate > 5% for 3 minutes",
        )

        sfn_failures_alarm = cw.Alarm(
            self, "SfnFailuresAlarm",
            metric=state_machine.metric_failed(period=Duration.minutes(1)),
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=2,
            alarm_name="BedrockShaper-SfnFailures",
            alarm_description="Step Functions executions failing for 2+ minutes",
        )

        circuit_breaker_alarm = cw.Alarm(
            self, "CircuitBreakerTrippedAlarm",
            metric=cw.Metric(
                namespace="BedrockShaper",
                metric_name="CircuitBreakerTripped",
                dimensions_map={"ServiceName": "TrafficShaper"},
                period=Duration.minutes(1),
                statistic="Sum",
            ),
            threshold=0,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=1,
            alarm_name="BedrockShaper-CircuitBreakerTripped",
            alarm_description="Queue Processor circuit breaker tripped — Bedrock may be down",
        )

        # NOTE: The Reconciliation Lambda + its 60s EventBridge schedule were removed
        # with the counter-based admission gate. The gate is now a sliding-window READ
        # over the consumption records (see dynamo.py::put_allocation); there are no
        # counter items to reconcile, and the 15s window horizon self-heals any
        # over-admission drift. Consumption records are garbage-collected by their
        # 60s DynamoDB TTL, so no orphan sweep is needed either.

        # ============================================================
        # CloudWatch Dashboard — Operational visibility
        # ============================================================

        dashboard = cw.Dashboard(
            self, "TrafficShaperDashboard",
            dashboard_name="BedrockTrafficShaper",
        )

        period_1m = Duration.minutes(1)
        EMF_NS = "BedrockShaper"
        EMF_DIMS = {"ServiceName": "TrafficShaper"}

        # Shaper EMF metrics (QueueDepth, BurstUtilization, ProcessingRate,
        # QueueUtilization, BedrockLatency) are published with BOTH
        # ServiceName AND model_id dimensions. A single-dimension Metric query
        # returns NO data (a 2-dim series is distinct from a 1-dim one in CW),
        # which left these widgets blank. Use a SEARCH() metric-math expression
        # so the dashboard auto-renders one line per model — new models appear
        # automatically, no hardcoded model list. period_s for SEARCH below.
        period_s = int(period_1m.to_seconds())

        def emf_metric(name: str, statistic: str) -> cw.IMetric:
            return cw.MathExpression(
                expression=(
                    f"SEARCH('{{{EMF_NS},ServiceName,model_id}} "
                    f"MetricName=\"{name}\" ServiceName=\"TrafficShaper\"', "
                    f"'{statistic}', {period_s})"
                ),
                label="",  # CW labels each line by its model_id dimension
                period=period_1m,
            )

        # Source-dimensioned variant for the processor's per-request counters
        # (RequestsProcessed, BedrockThrottles) which carry an extra `source`
        # dimension ('immediate' = burst gate, 'queued' = queue drain). The SEARCH
        # schema must list all THREE dimensions to match the emitted series; CW
        # then renders one line per (model_id, source) pair — so the
        # immediate-vs-queued split and the throttle-source split appear
        # automatically, no hand-reconstruction from logs. See bedrock_processor's
        # emit_request_metrics().
        def emf_metric_by_source(name: str, statistic: str) -> cw.IMetric:
            return cw.MathExpression(
                expression=(
                    f"SEARCH('{{{EMF_NS},ServiceName,model_id,source}} "
                    f"MetricName=\"{name}\" ServiceName=\"TrafficShaper\"', "
                    f"'{statistic}', {period_s})"
                ),
                label="",  # CW labels each line by its model_id + source dimensions
                period=period_1m,
            )

        # --- Annotation: Admission Gate baselines ---
        dashboard.add_widgets(
            cw.TextWidget(
                markdown=(
                    "## Admission Gate\n"
                    "| Metric | Normal | Investigate |\n"
                    "|--------|--------|-------------|\n"
                    "| Queue Depth | 0-10 | > 50 sustained |\n"
                    "| Burst Utilization | 0-80% | 100% for > 2 min |"
                ),
                width=24, height=3,
            ),
        )

        # --- Row 1: Admission Gate (EMF from Budget Manager) ---
        # (The old absolute "Queue Depth" widget was removed with the get_queue_depth
        # hot-partition scan; backlog is now shown by "Derived Queue Depth" in Row 1b.)
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Burst Utilization",
                left=[emf_metric("BurstUtilization", "Average")],
                width=12, height=6,
            ),
        )

        # --- Row 1b: DERIVED Queue Depth (Option A — metric math, no state) ---
        # We never query DynamoDB for backlog depth (a get_queue_depth Select='COUNT'
        # scan of the hot QUEUE#ITEMS partition previously spiked RCU and threw
        # ThrottlingException). Instead we DERIVE depth from two counters the system
        # ALREADY emits (no new state, no writes, no Lambda):
        #   enqueued = RequestQueued     (budget_manager, dims [ServiceName, model_id];
        #                                 value 1 per reject->enqueue)
        #   dequeued = RequestsProcessed (bedrock_processor, dims
        #              (source=queued)     [ServiceName, model_id, source]; source="queued"
        #                                 = a request dispatched to Bedrock off the queue)
        #
        # SUM(SEARCH(...)) collapses the per-model (and per-source) series into ONE
        # series each, so the math is fleet-wide — new models appear automatically.
        #
        # HONEST CAVEAT: `depth` is a NET-DELTA / trend, NOT a guaranteed absolute
        # queue depth. CloudWatch metrics are immutable, append-only time series —
        # there is no server-side mutable gauge. RUNNING_SUM re-zeros at the start of
        # the viewed window, so the line means "backlog accumulated since window start"
        # and equals true depth only when the window begins at a known-empty point.
        # Acceptable because the control loop no longer uses depth for ANY decision.
        derived_enqueued = (
            f"SUM(SEARCH('{{{EMF_NS},ServiceName,model_id}} "
            f"MetricName=\"RequestQueued\" ServiceName=\"TrafficShaper\"', 'Sum', {period_s}))"
        )
        derived_dequeued = (
            f"SUM(SEARCH('{{{EMF_NS},ServiceName,model_id,source}} "
            f"MetricName=\"RequestsProcessed\" ServiceName=\"TrafficShaper\" source=\"queued\"', "
            f"'Sum', {period_s}))"
        )
        derived_depth = cw.MathExpression(
            expression=f"RUNNING_SUM(({derived_enqueued}) - ({derived_dequeued}))",
            label="Derived Depth (net since window start)",
            period=period_1m,
        )
        derived_net_rate = cw.MathExpression(
            expression=f"({derived_enqueued}) - ({derived_dequeued})",
            label="Net Backlog Change / min (+grow / -drain)",
            period=period_1m,
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Derived Queue Depth (metric math — net delta, NOT absolute)",
                left=[derived_depth],
                right=[derived_net_rate],
                width=24, height=6,
            ),
        )

        # --- Row 2: Queue Processing (EMF from Queue Processor) ---
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Processing Rate (items/min)",
                left=[emf_metric("ProcessingRate", "Sum")],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="Queue Utilization",
                left=[emf_metric("QueueUtilization", "Average")],
                width=12, height=6,
            ),
        )

        # --- Row 2b: Admission Source Split (EMF from Bedrock Processor) ---
        # One line per (model, source). "Requests by Source" answers "how many
        # processed immediately (burst) vs queued (drain)"; "Throttles by Source"
        # answers "which admission path is producing the throttles" — both without
        # hand-walking log streams.
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Requests by Source (immediate vs queued)",
                left=[emf_metric_by_source("RequestsProcessed", "Sum")],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="Throttles by Source (immediate vs queued)",
                left=[emf_metric_by_source("BedrockThrottles", "Sum")],
                width=12, height=6,
            ),
        )

        # --- Row 3: Bedrock Performance ---
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Bedrock Latency (P50/P95/P99)",
                left=[emf_metric("BedrockLatency", s) for s in ("p50", "p95", "p99")],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="Bedrock Processor Invocations",
                left=[bedrock_processor_lambda.metric_invocations(period=period_1m)],
                width=12, height=6,
            ),
        )

        # --- Row 4: Lambda Operations ---
        all_lambdas = [
            ("BudgetMgr", budget_manager_lambda),
            ("QueueProc", queue_processor_lambda),
            ("BedrockProc", bedrock_processor_lambda),
        ]
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Lambda Duration (P50/P95)",
                left=[fn.metric_duration(period=period_1m, statistic="p50", label=f"{name} p50")
                      for name, fn in all_lambdas],
                right=[fn.metric_duration(period=period_1m, statistic="p95", label=f"{name} p95")
                       for name, fn in all_lambdas],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="Lambda Errors",
                left=[fn.metric_errors(period=period_1m, label=name)
                      for name, fn in all_lambdas],
                width=12, height=6,
            ),
        )

        # --- Annotation: Step Functions baselines ---
        dashboard.add_widgets(
            cw.TextWidget(
                markdown=(
                    "## Step Functions\n"
                    "| Metric | Normal | Investigate |\n"
                    "|--------|--------|-------------|\n"
                    "| Failed | 0 | Any non-zero |\n"
                    "| TimedOut | 0 | > 0 (queue backlog or timeout too short) |\n"
                    "| Succeeded | Matches Started | Divergence > 5% |"
                ),
                width=24, height=3,
            ),
        )

        # --- Row 5: Step Functions ---
        dashboard.add_widgets(
            cw.GraphWidget(
                title="SFN Executions (Started / Succeeded)",
                left=[
                    state_machine.metric_started(period=period_1m),
                    state_machine.metric_succeeded(period=period_1m),
                ],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="SFN Executions (Failed / TimedOut)",
                left=[
                    state_machine.metric_failed(period=period_1m),
                    state_machine.metric_timed_out(period=period_1m),
                ],
                width=12, height=6,
            ),
        )

        # --- Row 6: System Health ---
        dashboard.add_widgets(
            cw.GraphWidget(
                title="DLQ Depth + Orphaned Records",
                left=[dlq.metric_approximate_number_of_messages_visible(period=period_1m)],
                # OrphanedRecordsSwept is emitted with ServiceName ONLY (no model_id),
                # so it uses a plain single-dimension Metric, not the per-model SEARCH.
                right=[cw.Metric(namespace=EMF_NS, metric_name="OrphanedRecordsSwept",
                                 dimensions_map=EMF_DIMS, period=period_1m, statistic="Sum")],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="DynamoDB Consumed Capacity",
                left=[single_table.metric_consumed_read_capacity_units(period=period_1m)],
                right=[single_table.metric_consumed_write_capacity_units(period=period_1m)],
                width=12, height=6,
            ),
        )

        # --- Row 7: Alarm Status ---
        dashboard.add_widgets(
            cw.AlarmStatusWidget(
                title="Alarm Status",
                alarms=[dlq_alarm, lambda_error_alarm, sfn_failures_alarm, circuit_breaker_alarm],
                width=24, height=3,
            ),
        )

        # ============================================================
        # Outputs
        # ============================================================
        
        CfnOutput(
            self, "BudgetManagerFunctionArn",
            value=budget_manager_lambda.function_arn,
            description="Budget Manager Lambda ARN"
        )

        CfnOutput(
            self, "BedrockProcessorFunctionArn",
            value=bedrock_processor_lambda.function_arn,
            description="Bedrock Processor Lambda ARN"
        )

        CfnOutput(
            self, "QueueProcessorFunctionArn",
            value=queue_processor_lambda.function_arn,
            description="Queue Processor Lambda ARN"
        )

        CfnOutput(
            self, "StateMachineArn",
            value=state_machine.state_machine_arn,
            description="Step Functions State Machine ARN"
        )

        CfnOutput(
            self, "StateMachineConsoleUrl",
            value=f"https://console.aws.amazon.com/states/home?region={self.region}#/statemachines/view/{state_machine.state_machine_arn}",
            description="Step Functions Console URL"
        )

        CfnOutput(
            self, "SingleTableName",
            value=single_table.table_name,
            description="Single table for leaky bucket consumption tracking"
        )

        CfnOutput(
            self, "SharedServiceLayerArn",
            value=shared_service_layer.layer_version_arn,
            description="Shared Service Layer ARN"
        )

        CfnOutput(
            self, "ApiGatewayUrl",
            value=api.url,
            description="API Gateway REST API URL"
        )

        CfnOutput(
            self, "WafWebAclArn",
            value=waf_acl.attr_arn,
            description="WAF WebACL ARN (REGIONAL, associated to the API GW prod stage)"
        )

        CfnOutput(
            self, "DlqUrl",
            value=dlq.queue_url,
            description="Dead Letter Queue URL"
        )

        CfnOutput(
            self, "DlqArn",
            value=dlq.queue_arn,
            description="Dead Letter Queue ARN"
        )

        CfnOutput(
            self, "DashboardUrl",
            value=f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name=BedrockTrafficShaper",
            description="CloudWatch Dashboard URL"
        )

        # Honest-outcomes (OBJ3) outputs
        CfnOutput(
            self, "OutputBucketName",
            value=outcome_output_bucket.bucket_name,
            description="S3 bucket holding inference-output bodies (output_ref target)"
        )

        CfnOutput(
            self, "ResultEndpoint",
            value=f"{api.url}result/",
            description="GET /result/{request_id} honest-outcomes poll endpoint"
        )

        CfnOutput(
            self, "FinalizerFunctionArn",
            value=finalizer_lambda.function_arn,
            description="Finalizer Lambda ARN (EventBridge SFN-status handler)"
        )

        CfnOutput(
            self, "OutcomeStreamFunctionArn",
            value=outcome_stream_lambda.function_arn,
            description="Outcome Stream Lambda ARN (sole RequestOutcome EMF emitter)"
        )

        # ------------------------------------------------------------------
        # cdk-nag suppressions
        #
        # This is an internal load-test / demo-grade stack for the Bedrock
        # traffic-shaper prototype. Every entry below is a genuine, currently-
        # firing AwsSolutions finding that is an INTENTIONAL deviation for an
        # internal prototype (not a production account). Findings that could be
        # fixed safely without changing the stack's purpose were FIXED instead
        # of suppressed:
        #   - AwsSolutions-SQS4  -> fixed: DLQ now enforce_ssl=True
        #   - AwsSolutions-SF2   -> fixed: state machine tracing_enabled=True
        #   - AwsSolutions-APIG1 -> fixed: stage access logging to a log group
        #   - AwsSolutions-APIG6 -> fixed: stage execution logging (INFO)
        #   - AwsSolutions-APIG3 -> fixed: REGIONAL WAFv2 WebACL now associated
        #                           directly to the API GW prod stage (was suppressed
        #                           when CloudFront fronted the API).
        # Findings that never fire (dead waivers) were REMOVED:
        #   - AwsSolutions-APIG4 -> POST /invoke already uses IAM auth (satisfied)
        #   - AwsSolutions-CFR1/CFR2/CFR3/CFR4 -> CloudFront removed entirely by the
        #                           ingress re-architecture (distribution + viewer
        #                           function deleted), so these rules no longer fire.
        #
        # SCOPING NOTE: these are applied stack-wide via add_stack_suppressions
        # rather than per-resource on purpose. IAM4/IAM5/L1 are cross-cutting —
        # they fire identically on EVERY Lambda role + function CDK generates, so
        # a stack-wide waiver is the honest scope (a per-resource loop would just
        # re-list the same reason N times and drift as functions are added). The
        # remaining rules each map to a single resource, but keeping them in one
        # reviewed block with the cross-cutting ones keeps the whole waiver set in
        # one auditable place. Each reason is run through the shared INTERNAL
        # helper's validator (_assert_justified) so an empty/placeholder reason
        # fails synth rather than silently shipping.
        # ------------------------------------------------------------------
        _stack_suppressions = [
            {
                "id": "AwsSolutions-IAM4",
                "reason": (
                    "Lambda functions use the AWS-managed "
                    "AWSLambdaBasicExecutionRole for CloudWatch Logs access. "
                    "Acceptable for this internal prototype; scope to a "
                    "customer-managed policy before production promotion."
                ),
            },
            {
                "id": "AwsSolutions-IAM5",
                "reason": (
                    "Remaining wildcard resources are constrained to the "
                    "deploying account/region for Step Functions (stateMachine:* "
                    "/ execution:*) and to Bedrock foundation-model / "
                    "cross-region inference-profile ARNs. bedrock:InvokeModel "
                    "spans a dynamic, per-request model set so the model segment "
                    "cannot be pinned at deploy time. events:PutEvents is scoped "
                    "to the default event-bus ARN. Grants from grant_read_write_data / "
                    "grant_invoke expand to table-index and function-version wildcards "
                    "that CDK manages. Revisit model-ARN scoping for production."
                ),
            },
            {
                "id": "AwsSolutions-APIG2",
                "reason": (
                    "Request validation is handled downstream by the "
                    "budget_manager Lambda (explicit model_id/token/payload "
                    "validation) rather than API Gateway request validators."
                ),
            },
            {
                "id": "AwsSolutions-COG4",
                "reason": (
                    "No Cognito user-pool authorizer by design — both methods use "
                    "IAM (SigV4) authorization instead, which is the intended "
                    "control for AWS-signed callers, now behind a REGIONAL WAFv2 "
                    "WebACL associated directly to the API Gateway stage."
                ),
            },
            {
                "id": "AwsSolutions-SQS3",
                "reason": (
                    "This queue IS the dead-letter queue for failed Bedrock "
                    "callbacks; a DLQ does not itself require a further DLQ."
                ),
            },
            {
                "id": "AwsSolutions-L1",
                "reason": (
                    "Lambda runtime is pinned to Python 3.13 to match the "
                    "shared layer's compatible_runtimes; upgrade the functions "
                    "and the layer in lockstep to avoid a runtime/layer mismatch."
                ),
            },
            {
                "id": "AwsSolutions-S1",
                "reason": (
                    "The honest-outcomes OutcomeOutputBucket holds transient "
                    "inference-response bodies (2-day lifecycle expiry) for this "
                    "internal load-test/prototype. Server access logging is not "
                    "enabled to avoid a second self-logging log bucket + cost "
                    "(mirrored by the checkov CKV_AWS_18 skip on the bucket). Enable "
                    "before production promotion."
                ),
            },
        ]

        # Fail synth NOW (not at review time) if any waiver reason is empty or a
        # placeholder — same guarantee the shared resource-scoped helper gives.
        for _entry in _stack_suppressions:
            _assert_justified(_entry)

        NagSuppressions.add_stack_suppressions(self, _stack_suppressions)
