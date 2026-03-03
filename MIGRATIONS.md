# Database Migrations Guide

This project uses **Alembic** for database schema migrations with PostgreSQL.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database Connection

Set your database URL using one of these methods (in order of priority):

**Option A: Environment Variable (Recommended)**
```bash
export DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/multi_api_aggregator"
# OR
export JOB_SCRAPER_DB_DSN="postgresql+psycopg://user:pass@localhost:5432/multi_api_aggregator"
```

**Option B: config.yaml**
```yaml
db:
  dsn: "postgresql+psycopg://user:pass@localhost:5432/multi_api_aggregator"
```

**Option C: Edit alembic.ini**
```ini
sqlalchemy.url = postgresql+psycopg://user:pass@localhost:5432/multi_api_aggregator
```

### 3. Run Initial Migration

```bash
# Apply all pending migrations
alembic upgrade head
```

## Common Migration Commands

### Check Migration Status

```bash
# Show current database revision
alembic current

# Show migration history
alembic history --verbose

# Show pending migrations
alembic history --verbose --indicate-current
```

### Apply Migrations

```bash
# Upgrade to latest version
alembic upgrade head

# Upgrade by one version
alembic upgrade +1

# Upgrade to specific revision
alembic upgrade <revision_id>
```

### Rollback Migrations

```bash
# Rollback one migration
alembic downgrade -1

# Rollback to specific revision
alembic downgrade <revision_id>

# Rollback all migrations
alembic downgrade base
```

### Create New Migrations

```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "add column to jobs table"

# Create blank migration (manual)
alembic revision -m "custom data migration"
```

## Migration Workflow

### For New Deployments

1. **Fresh Database**: Run `alembic upgrade head` to create all tables
2. **Verify**: Check tables were created: `alembic current`

### For Existing Databases (Created with storage.init_db())

If you already have tables created by `storage.init_db()`:

```bash
# Mark database as being at the initial migration (without running it)
alembic stamp 001_initial_schema

# Then apply any new migrations
alembic upgrade head
```

### For Schema Changes

1. **Modify Models**: Update SQLAlchemy models in `job_scraper/storage.py`

2. **Generate Migration**:
   ```bash
   alembic revision --autogenerate -m "describe your changes"
   ```

3. **Review Migration**: Check the generated file in `alembic/versions/`
   - Verify upgrade() and downgrade() functions
   - Test on development database first

4. **Apply Migration**:
   ```bash
   alembic upgrade head
   ```

## Current Schema (001_initial_schema)

The initial migration creates these tables:

### `runs` - Scraper Run Tracking
- `id` (UUID, PK)
- `started_at` (timestamp)
- `ended_at` (timestamp, nullable)
- `status` (varchar: running/success/failed)
- `sources` (JSONB array)
- `total_jobs` (integer)

### `jobs` - Job Postings
- `id` (UUID, PK)
- `dedupe_key` (text, unique, indexed)
- `source` (varchar, indexed)
- `source_job_id` (varchar)
- `title` (text)
- `company` (text)
- `location` (text)
- `url` (text)
- `description` (text)
- `salary` (text)
- `employment_type` (text)
- `posted_date` (text)
- `remote` (boolean, partial index where true)
- `category` (text)
- `tags` (JSONB)
- `skills` (JSONB)
- `raw_payload` (JSONB)
- `created_at` (timestamp, default now())
- `updated_at` (timestamp, default now(), auto-update)
- `last_seen_at` (timestamp, default now())

**Indexes:**
- `idx_jobs_dedupe_key` (unique)
- `idx_jobs_source`
- `idx_jobs_created_at`
- `idx_jobs_last_seen_at`
- `idx_jobs_updated_at_desc` (descending)
- `idx_jobs_remote_partial` (partial, where remote = true)

### `job_seen` - Run-Job Junction Table
- `id` (UUID, PK)
- `run_id` (UUID, FK to runs, cascade delete, indexed)
- `job_id` (UUID, FK to jobs, cascade delete, indexed)
- `seen_at` (timestamp)
- **Unique constraint**: (run_id, job_id)

### `source_errors` - API Error Tracking
- `id` (UUID, PK)
- `run_id` (UUID, FK to runs, cascade delete, indexed)
- `source` (varchar)
- `message` (text)
- `payload` (JSONB)
- `created_at` (timestamp)

## Best Practices

### DO:
- ✅ Always review auto-generated migrations before applying
- ✅ Test migrations on a development database first
- ✅ Keep migrations small and focused on single changes
- ✅ Write reversible migrations (implement both upgrade and downgrade)
- ✅ Commit migration files to version control
- ✅ Use descriptive migration messages

### DON'T:
- ❌ Edit migration files after they've been applied in production
- ❌ Delete migration files
- ❌ Skip migrations or run them out of order
- ❌ Commit database credentials to version control
- ❌ Run migrations directly on production without testing

## Troubleshooting

### "No module named 'psycopg'"
Install PostgreSQL driver:
```bash
pip install "psycopg[binary]"
```

### "Database connection failed"
Check your database URL configuration and ensure PostgreSQL is running.

### "Target database is not up to date"
Run `alembic upgrade head` to apply pending migrations.

### "Can't locate revision identified by '...'"
Your database may have been modified manually. Check `alembic current` and migration files.

### Reset Alembic State (Development Only)
```bash
# Drop all tables
alembic downgrade base

# Reapply all migrations
alembic upgrade head
```

## Integration with Existing Code

The project has both migration-based and code-based schema creation:

### Using Migrations (Recommended)
```bash
alembic upgrade head
```

### Using storage.init_db() (Legacy)
```python
from job_scraper.storage import init_db
init_db(dsn)
```

**Note**: If you've used `init_db()`, run `alembic stamp 001_initial_schema` to sync Alembic's version tracking.

## Migration File Structure

```
alembic/
├── versions/
│   └── 001_initial_schema.py    # Initial schema
├── env.py                        # Alembic environment config
├── script.py.mako                # Migration template
└── README                        # This file

alembic.ini                       # Alembic configuration
```

## Further Reading

- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
