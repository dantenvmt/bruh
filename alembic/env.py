"""Alembic environment configuration for Multi-API Job Aggregator."""
from logging.config import fileConfig
import os
from pathlib import Path

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# Import Base and all models from storage.py
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from job_scraper.storage import Base
from job_scraper.config import Config

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url():
    """
    Get database URL from environment variables or config.yaml.
    Priority order:
    1. DATABASE_URL environment variable
    2. JOB_SCRAPER_DB_DSN environment variable
    3. config.yaml db.dsn setting
    4. alembic.ini sqlalchemy.url setting
    """
    # Try environment variables first
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    url = os.getenv("JOB_SCRAPER_DB_DSN")
    if url:
        return url

    # Try loading from config.yaml
    try:
        app_config = Config()
        if app_config.db_dsn:
            return app_config.db_dsn
    except Exception:
        pass

    # Fall back to alembic.ini setting
    return config.get_main_option("sqlalchemy.url")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Get the database URL
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
