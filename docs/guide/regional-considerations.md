# Regional considerations

The shaper is region-agnostic AWS-native infrastructure — but the **Bedrock models it fronts vary
by region**, and the admission gate is configured *per model ID*. Before you deploy anywhere other
than `us-east-1`, confirm your target model is available in that region and note its real quota. A
model ID valid in `us-east-1` may not exist — or may carry a different quota — elsewhere, and
cross-region inference-profile prefixes (`us.`, `eu.`, `global.`) are themselves per-geography.

## Confirm model availability, then configure

```bash
# 1. list foundation models actually available in your target region
aws bedrock list-foundation-models --region <region> \
  --query "modelSummaries[].modelId" --output table

# 2. (cross-region inference) list the inference profiles that resolve there
aws bedrock list-inference-profiles --region <region> \
  --query "inferenceProfileSummaries[].inferenceProfileId" --output table

# 3. create the shaper config with the exact in-region model ID and its real quota
make create-config MODEL=<in-region-model-id> TPM=<tpm> [RPM=<rpm>]
```

## Official AWS guidance (source of truth)

Model-by-region support changes frequently — rely on the AWS docs rather than any static table:

- [Supported foundation models in Amazon Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html) — availability by Region.
- [Cross-region inference](https://docs.aws.amazon.com/bedrock/latest/userguide/cross-region-inference.html) — which inference-profile prefixes resolve where.
- [Amazon Bedrock endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/bedrock.html) — regional endpoints and default service quotas.

> Deploying into GovCloud, China, or other non-standard partitions also changes ARN/endpoint shapes
> (SigV4 signing, IAM resource ARNs). Confirm the partition prefix (`aws` / `aws-us-gov` / `aws-cn`)
> matches everywhere, and keep the stack, its table, and the Bedrock models it calls in the same
> partition — cross-partition invocation is not supported.
