# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities
2. Email the maintainers with details of the vulnerability
3. Include steps to reproduce, impact assessment, and suggested fix if possible

We will acknowledge receipt within 48 hours and provide a timeline for remediation.

## Scope

This policy covers:
- The CDK infrastructure code and Lambda handlers
- The DynamoDB access patterns and admission logic
- The API Gateway and CloudFront configuration
- Test scripts that interact with AWS services

## Security Design Decisions

- API Gateway uses IAM authorization (no unauthenticated access)
- CloudFront enforces HTTPS-only with WAF rate limiting
- DynamoDB uses AWS-managed encryption (acceptable for rate-limiting metadata; no PII stored)
- Lambda functions use least-privilege IAM with documented exceptions (see cdk-nag suppressions)
- No secrets or credentials are stored in code; all authentication uses IAM roles
