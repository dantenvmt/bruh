"""
Company list builders for discovery.

Loads company lists from various sources for the discovery pipeline.
Phase 1 scope: seed_csv and hardcoded only.
"""
import csv
import logging
from pathlib import Path
from typing import Iterator, Optional

import yaml

from .types import DiscoveredCompany, DiscoverySource


logger = logging.getLogger(__name__)


def load_seed_csv(
    path: Path,
    max_priority: Optional[int] = None,
) -> Iterator[DiscoveredCompany]:
    """Load companies from the targets seed CSV file.

    Expected CSV format:
        company_name,priority,category
        Amazon,1,big_tech
        Google,1,big_tech

    Args:
        path: Path to the CSV file
        max_priority: If set, only include companies with priority <= this value

    Yields:
        DiscoveredCompany for each row in the CSV
    """
    if not path.exists():
        logger.warning(f"Seed CSV not found: {path}")
        return

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            name = row.get("company_name", "").strip()
            if not name:
                continue

            try:
                priority = int(row.get("priority", 3))
            except (ValueError, TypeError):
                priority = 3

            # Filter by priority if requested
            if max_priority and priority > max_priority:
                continue

            category = row.get("category", "").strip() or None

            yield DiscoveredCompany(
                name=name,
                source=DiscoverySource.SEED_CSV,
                priority=priority,
                category=category,
            )

    logger.info(f"Loaded companies from {path}")


def load_hardcoded_yaml(path: Path) -> Iterator[DiscoveredCompany]:
    """Load companies from a hardcoded YAML file.

    Expected YAML format:
        companies:
          - name: Stripe
            careers_url: https://stripe.com/jobs
            priority: 1
            category: fintech
          - name: Plaid
            careers_url: https://plaid.com/careers

    Args:
        path: Path to the YAML file

    Yields:
        DiscoveredCompany for each entry in the YAML
    """
    if not path.exists():
        logger.warning(f"Hardcoded YAML not found: {path}")
        return

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data or "companies" not in data:
        logger.warning(f"No companies found in {path}")
        return

    for entry in data["companies"]:
        if isinstance(entry, str):
            # Simple string format: just company name
            yield DiscoveredCompany(
                name=entry,
                source=DiscoverySource.HARDCODED,
            )
        elif isinstance(entry, dict):
            name = entry.get("name", "").strip()
            if not name:
                continue

            yield DiscoveredCompany(
                name=name,
                source=DiscoverySource.HARDCODED,
                priority=entry.get("priority"),
                careers_url=entry.get("careers_url"),
                category=entry.get("category"),
            )

    logger.info(f"Loaded companies from {path}")


class CompanySource:
    """Unified interface for loading companies from various sources."""

    def __init__(
        self,
        data_dir: Optional[Path] = None,
    ):
        """Initialize company source.

        Args:
            data_dir: Directory containing data files. Defaults to project data/.
        """
        if data_dir is None:
            # Default to project root/data
            data_dir = Path(__file__).parent.parent.parent / "data"
        self.data_dir = data_dir

    @property
    def seed_csv_path(self) -> Path:
        """Path to the seed CSV file."""
        return self.data_dir / "targets_seed_150.csv"

    @property
    def hardcoded_yaml_path(self) -> Path:
        """Path to the hardcoded companies YAML."""
        return self.data_dir / "hardcoded_companies.yaml"

    def load(
        self,
        source: DiscoverySource,
        max_priority: Optional[int] = None,
    ) -> Iterator[DiscoveredCompany]:
        """Load companies from a specific source.

        Args:
            source: Source to load from
            max_priority: For seed_csv, filter by priority

        Yields:
            DiscoveredCompany for each entry

        Raises:
            ValueError: If source is not supported
        """
        if source == DiscoverySource.SEED_CSV:
            yield from load_seed_csv(self.seed_csv_path, max_priority)
        elif source == DiscoverySource.HARDCODED:
            yield from load_hardcoded_yaml(self.hardcoded_yaml_path)
        elif source == DiscoverySource.FORTUNE500:
            raise ValueError("fortune500 source is deferred (not implemented in Phase 1)")
        elif source == DiscoverySource.YC:
            raise ValueError("yc source is deferred (not implemented in Phase 1)")
        else:
            raise ValueError(f"Unknown source: {source}")

    def load_all_phase1(
        self,
        max_priority: Optional[int] = None,
    ) -> Iterator[DiscoveredCompany]:
        """Load companies from all Phase 1 sources.

        Args:
            max_priority: Filter seed_csv by priority

        Yields:
            DiscoveredCompany from seed_csv and hardcoded sources
        """
        yield from self.load(DiscoverySource.SEED_CSV, max_priority)
        yield from self.load(DiscoverySource.HARDCODED)

    def count(self, source: DiscoverySource) -> int:
        """Count companies in a source without loading all details.

        Args:
            source: Source to count

        Returns:
            Number of companies in the source
        """
        return sum(1 for _ in self.load(source))
