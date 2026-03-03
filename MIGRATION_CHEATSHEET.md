# Alembic Migration Quick Reference

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set database URL (choose one)
export DATABASE_URL="postgresql+psycopg://user:pass@host:port/dbname"
export JOB_SCRAPER_DB_DSN="postgresql+psycopg://user:pass@host:port/dbname"
# OR edit config.yaml or alembic.ini
```

## Most Common Commands

```bash
# Apply all migrations (fresh database)
alembic upgrade head

# Mark existing database as migrated (if tables already exist)
alembic stamp 001_initial_schema

# Check current migration status
alembic current

# View migration history
alembic history

# Rollback one migration
alembic downgrade -1
```

## Creating New Migrations

```bash
# Auto-generate from model changes
alembic revision --autogenerate -m "add email column to users"

# Create blank migration (for data migrations)
alembic revision -m "backfill user data"
```

## Example Workflows

### Fresh Database
```bash
alembic upgrade head
```

### Existing Database (created with storage.init_db())
```bash
alembic stamp 001_initial_schema
# Now ready for future migrations
```

### Adding a New Column
1. Edit `job_scraper/storage.py`:
   ```python
   class JobRecord(Base):
       # ... existing columns ...
       email = Column(String(255), nullable=True)  # NEW
   ```

2. Generate migration:
   ```bash
   alembic revision --autogenerate -m "add email to jobs"
   ```

3. Review the generated file in `alembic/versions/`

4. Apply migration:
   ```bash
   alembic upgrade head
   ```

### Rolling Back a Migration
```bash
# Rollback last migration
alembic downgrade -1

# Rollback to specific version
alembic downgrade abc123

# Rollback all migrations
alembic downgrade base
```

## Troubleshooting

| Error | Solution |
|-------|----------|
| "No module named 'psycopg'" | `pip install "psycopg[binary]"` |
| "Can't connect to database" | Check DATABASE_URL and PostgreSQL is running |
| "Target database is not up to date" | Run `alembic upgrade head` |
| Tables already exist | Run `alembic stamp 001_initial_schema` |

## Environment Variables

```bash
# Database connection (choose one)
DATABASE_URL="postgresql+psycopg://..."
JOB_SCRAPER_DB_DSN="postgresql+psycopg://..."

# Optional: specify config file
JOB_SCRAPER_CONFIG="path/to/config.yaml"
```

## File Structure

```
C:\Users\annsa\Desktop\Multi-api-aggregator
├── alembic/
│   ├── versions/
│   │   └── 001_initial_schema.py    # Initial migration
│   ├── env.py                        # Environment config
│   └── script.py.mako                # Template
├── alembic.ini                       # Alembic config
├── MIGRATIONS.md                     # Full documentation
└── MIGRATION_CHEATSHEET.md          # This file
```

## Safety Checklist

Before running migrations in production:

- [ ] Tested on development database
- [ ] Reviewed generated migration code
- [ ] Backed up production database
- [ ] Verified rollback (downgrade) works
- [ ] Checked for breaking changes
- [ ] Coordinated with application deployment

## Need Help?

- Full documentation: See `MIGRATIONS.md`
- Alembic docs: https://alembic.sqlalchemy.org/
- Project docs: See `README.md`
