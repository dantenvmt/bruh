# Security Guide

This project is designed to run without ever committing secrets to git.

## Security Updates (January 2026)

### Breaking Changes
1. **API authentication now required by default** - Set `JOB_SCRAPER_REQUIRE_API_KEY=false` to disable
2. **Raw payloads no longer stored by default** - Set `JOB_SCRAPER_STORE_RAW_PAYLOAD=true` to enable
3. **POSTGRES_PASSWORD now required** - No default value in docker-compose
4. **HTML stripped from descriptions** - Set `JOB_SCRAPER_SANITIZE_HTML=false` to disable

### CVE Fixes
- **Starlette CVEs** (CVE-2024-47874, CVE-2025-54121): Fixed by upgrading FastAPI to 0.115.6+
- Verify with: `pip show starlette` → version should be ≥0.47.2

### New Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| `JOB_SCRAPER_STORE_RAW_PAYLOAD` | `false` | Enable raw payload storage |
| `JOB_SCRAPER_SKIP_DSN_TLS_CHECK` | `false` | Bypass TLS validation (testing only) |
| `JOB_SCRAPER_ENABLE_HSTS` | `false` | Enable HSTS header |
| `JOB_SCRAPER_SANITIZE_HTML` | `true` | Enable HTML sanitization |

## Secrets Management (Recommended)

Use environment variables or a secrets manager. Do not store secrets in `config.yaml`.

## API Access Controls

- API key auth: set `JOB_SCRAPER_API_KEY` to require `X-API-Key` on all API endpoints.
- CORS: set `JOB_SCRAPER_CORS_ORIGINS` to a comma-separated allowlist. Defaults to localhost for dev; empty disables CORS.
- Rate limiting: set `JOB_SCRAPER_RATE_LIMIT` (example: `60/minute`).
- Query guardrail: set `JOB_SCRAPER_QUERY_MAX_LEN` to cap `q`/`location` size.

### Env-Only Mode
Set this to ignore `config.yaml` entirely:

```
JOB_SCRAPER_ENV_ONLY=1
```

### Load Secrets from JSON (Manager-Friendly)
Supply a JSON payload from your secrets manager:

```
JOB_SCRAPER_SECRETS_JSON='{"adzuna":{"app_id":"...","app_key":"..."},"db":{"dsn":"postgresql+psycopg://..."}}'
```

Or point to a local secrets file:

```
JOB_SCRAPER_SECRETS_FILE=secrets.json
```

### AWS Secrets Manager (Optional)
If you use AWS, set:

```
JOB_SCRAPER_AWS_SECRET_ID=your-secret-id-or-arn
```

Install the dependency if needed:
```
pip install boto3
```

## Logging Redaction

Logs automatically redact known env secrets and Postgres DSNs. You can add more:

```
JOB_SCRAPER_REDACT_VALUES=extra_secret_1,extra_secret_2
```

**Config-sourced secrets are also redacted**: Secrets loaded from `secrets.json` or AWS Secrets Manager are automatically added to the redaction list.

HTTPX request logs are muted to avoid leaking query-string keys.

### Safe Exception Logging
API client errors are logged without full exception messages to prevent URL leakage:
```python
# ✗ Unsafe (may contain API keys in URLs)
logger.error(f"API error: {e}")

# ✓ Safe (only logs error type and status)
logger.error(f"API error: type={type(e).__name__}, status={status}")
```

## Source Notes

### Adzuna Query-Key Risk

Adzuna requires credentials (`app_id` and `app_key`) to be passed in query parameters. This presents security risks:

**Why Query Parameters Are Risky:**
- **Web Server Logs**: URLs with query params are logged in web server access logs (nginx, Apache, etc.), exposing credentials in plaintext
- **Browser History**: URLs appear in browser history and bookmarks, persisting credentials locally
- **Proxy/CDN Caching**: Proxies and CDNs may cache or log full URLs including credentials
- **Referer Headers**: When users click links from your site, the Referer header can leak full URLs (including query params) to third-party sites
- **Analytics Tools**: URL tracking in Google Analytics, Sentry, or other tools may capture credentials

**Risk Mitigation:**
- Keep server logs protected with strict file permissions (600/640)
- Implement log scrubbing to redact `app_id` and `app_key` from access logs
- Minimize log retention periods (7-30 days recommended)
- Use `Referrer-Policy: no-referrer` or `strict-origin-when-cross-origin` headers
- Never share raw logs externally without sanitization
- Consider using a backend proxy that adds credentials server-side (prevents client exposure)

Note: HTTPX request logs are already muted in this project to prevent query-string credential leakage.

## Secret Scanning (Pre-Commit + CI)

Install pre-commit and gitleaks:

```
pip install pre-commit
pre-commit install
```

The CI workflow runs gitleaks on every push/PR.

## Database Hygiene

### TLS/SSL Requirements

**MANDATORY for Production:** Use `sslmode=require` in your database connection string.

```
postgresql+psycopg://user:pass@host:5432/db?sslmode=require
```

**Why TLS is Critical:**
- **Credential Interception**: Without TLS, database credentials are transmitted in plaintext and can be intercepted via network sniffing (MITM attacks)
- **Data Exposure**: All query data (including sensitive PII, job postings, user data) is visible to anyone monitoring network traffic
- **Session Hijacking**: Attackers can steal session tokens and impersonate legitimate database connections
- **Compliance**: Most compliance frameworks (SOC 2, HIPAA, PCI-DSS) require encryption in transit

**SSL Mode Options:**
- `disable`: No SSL (NEVER use in production)
- `allow`: Try SSL, fallback to plaintext (NOT recommended)
- `prefer`: Prefer SSL but allow plaintext (NOT recommended)
- `require`: **Require SSL** (minimum for production)
- `verify-ca`: Require SSL and verify certificate authority
- `verify-full`: Require SSL and verify hostname (most secure)

**Cloud Provider Notes:**
- Supabase and most cloud database providers enforce TLS by default
- AWS RDS, Google Cloud SQL, Azure Database all support/require TLS
- Verify your provider's TLS configuration in their console

### Additional Database Security

- Use a least-privilege DB user (read/write only to required tables)
- Enable IP allowlisting in Supabase/AWS/GCP to restrict access
- Rotate database credentials regularly (90 days recommended)
- Use connection pooling with idle timeout to prevent connection exhaustion
- Monitor for suspicious queries or access patterns

## Raw Payload Security

**Two separate controls exist for raw payloads:**

1. **Storage Toggle** (`JOB_SCRAPER_STORE_RAW_PAYLOAD`): Controls whether raw payloads are saved to database
2. **API Access Toggle** (`JOB_SCRAPER_RAW_PAYLOAD_ENABLED`): Controls whether `/jobs/raw` endpoint is accessible

**Defaults: Both disabled** (recommended for production)

### Scrubbing Existing Data
To remove existing raw payloads from your database, run the migration:
```bash
alembic upgrade head  # Includes 003_scrub_raw_payloads migration
```
⚠️ **Back up your database first** - this is irreversible.

### Why Raw Payloads Are Risky

Raw API payloads from job boards may contain:
- **PII (Personally Identifiable Information)**: Recruiter names, email addresses, phone numbers
- **Sensitive Job Data**: Internal company information, salary details, confidential project names
- **Tracking Identifiers**: Session tokens, user IDs, tracking pixels
- **Debugging Information**: API keys in error responses, internal URLs, stack traces
- **Legal/Compliance Issues**: Data you're not authorized to store or redistribute

### When to Enable

Only enable raw payload storage when:
- Debugging integration issues that require full API responses
- Performing data analysis that requires original unprocessed data
- You have legal/compliance approval to store raw third-party data
- You have implemented data retention policies and scrubbing procedures

### How to Enable Safely

```bash
# Enable raw payload storage
JOB_SCRAPER_RAW_PAYLOAD_ENABLED=1
```

**If enabled, implement these safeguards:**
1. Set up automated PII scrubbing/redaction before storage
2. Implement strict data retention policies (e.g., delete after 7 days)
3. Restrict database access to raw payload columns
4. Audit who accesses raw payload data
5. Disable after debugging is complete

## Production Hardening Checklist

Before deploying to production, verify all security controls are enabled:

### Authentication & Access Control
```bash
# Require API key authentication on all endpoints
JOB_SCRAPER_REQUIRE_API_KEY=1

# Set a strong random API key (32+ characters, alphanumeric + symbols)
JOB_SCRAPER_API_KEY="$(openssl rand -base64 32)"

# Restrict CORS to specific domains (comma-separated, no wildcards)
JOB_SCRAPER_CORS_ORIGINS="https://yourdomain.com,https://app.yourdomain.com"
```

### Database Security
```bash
# Use TLS/SSL for database connections (REQUIRED)
JOB_SCRAPER_DB_DSN="postgresql+psycopg://user:pass@host:5432/db?sslmode=require"

# Verify SSL mode is set to 'require' or higher
echo $JOB_SCRAPER_DB_DSN | grep -q "sslmode=require" && echo "OK" || echo "FAIL"
```

### Data Protection
```bash
# Keep raw payload storage disabled (default)
JOB_SCRAPER_RAW_PAYLOAD_ENABLED=0  # or unset

# Add any additional secrets to redaction list
JOB_SCRAPER_REDACT_VALUES="internal_api_key,oauth_token"
```

### Rate Limiting & Input Validation
```bash
# Set appropriate rate limits for your use case
JOB_SCRAPER_RATE_LIMIT="60/minute"  # Adjust based on expected traffic

# Limit query parameter length to prevent abuse
JOB_SCRAPER_QUERY_MAX_LEN=200
```

### Environment Configuration
```bash
# Use environment-only mode (ignore config.yaml)
JOB_SCRAPER_ENV_ONLY=1

# Or load secrets from a secure secrets manager
JOB_SCRAPER_AWS_SECRET_ID="your-secret-id"  # AWS Secrets Manager
# OR
JOB_SCRAPER_SECRETS_FILE="/secure/path/secrets.json"  # Local file with strict permissions (400)
```

### Security Headers (Application Layer)
Security headers are now set automatically by the API:
```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: strict-origin-when-cross-origin
Content-Security-Policy: default-src 'self'
```

For HTTPS deployments, enable HSTS:
```bash
JOB_SCRAPER_ENABLE_HSTS=true
# Adds: Strict-Transport-Security: max-age=31536000; includeSubDomains
```

### Monitoring & Logging
- Enable audit logging for API key usage
- Set up alerts for rate limit violations
- Monitor for unusual query patterns or access attempts
- Regularly review access logs (with credentials redacted)
- Implement log rotation and retention policies

### Dependency Security
Keep dependencies updated and scan for vulnerabilities:
```bash
# Scan for CVEs
pip-audit
safety scan

# Check unpinned deps (Safety warning)
# Consider using a lockfile for reproducible builds
```

### Pre-Deployment Verification
```bash
# Run security scans
gitleaks detect --source .
pip-audit
trivy fs .

# Verify no secrets in git history
git log -p | grep -i "password\|api_key\|secret" && echo "WARNING: Found secrets" || echo "OK"

# Test API key requirement
curl -I http://localhost:8000/api/jobs  # Should return 401/403 if API key is required

# Verify CORS restrictions
curl -H "Origin: https://evil.com" http://localhost:8000/api/jobs  # Should be blocked
```

### Post-Deployment
- [ ] Rotate all secrets after initial deployment
- [ ] Enable database backups with encryption at rest
- [ ] Set up monitoring/alerting for service health
- [ ] Document incident response procedures
- [ ] Schedule regular security audits (quarterly recommended)
