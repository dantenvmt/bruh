"""
build_seed.py — Fetch company seed data from public sources and write to data/

Sources:
  --yc          5,600+ YC companies (yc-oss GitHub API)
  --greenhouse  5,000  Greenhouse board tokens (Feashliaa/job-board-aggregator)
  --lever       1,000  Lever company slugs
  --ashby       1,000  Ashby company slugs
  --workday     3,000+ Workday tenant strings (company|instance|site)

Output files:
  data/yc_companies.csv            name, website, industry, batch, team_size, is_hiring
  data/greenhouse_tokens.csv       token, careers_url
  data/lever_tokens.csv            token, careers_url
  data/ashby_tokens.csv            token, careers_url
  data/workday_tenants.csv         company, instance, site, careers_url
  data/seed_config.yaml            drop-in config include for all ATS adapters

Usage:
  python scripts/build_seed.py --all
  python scripts/build_seed.py --yc --greenhouse --lever
  python scripts/build_seed.py --greenhouse --out data/

To activate the generated config:
  export JOB_SCRAPER_CONFIG_INCLUDES=data/seed_config.yaml
  # or add to your .env / config.yaml includes list
"""

import argparse
import csv
import json
import logging
import sys
import urllib.request
from pathlib import Path

import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------

YC_URL = "https://yc-oss.github.io/api/companies/all.json"
GREENHOUSE_URL = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/greenhouse_companies.json"
LEVER_URL = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/lever_companies.json"
ASHBY_URL = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/ashby_companies.json"
WORKDAY_URL = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data/workday_companies.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_json(url: str) -> object:
    log.info("Fetching %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "job-scraper-seed-builder/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _write_csv(path: Path, fieldnames: list, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %d rows → %s", len(rows), path)


# ---------------------------------------------------------------------------
# Source fetchers
# ---------------------------------------------------------------------------

def fetch_yc(out_dir: Path) -> int:
    """Fetch YC companies, write to yc_companies.csv.

    Fields kept: name, website, industry, batch, team_size, is_hiring, status, one_liner
    Only active/public companies with a website are included.
    """
    data = _fetch_json(YC_URL)
    if not isinstance(data, list):
        log.error("Unexpected YC response shape")
        return 0

    rows = []
    skipped = 0
    for company in data:
        website = (company.get("website") or "").strip()
        status = (company.get("status") or "").strip()
        name = (company.get("name") or "").strip()

        if not website or not name:
            skipped += 1
            continue

        # Only keep active or public companies — skip Inactive/Acquired
        if status.lower() in ("inactive",):
            skipped += 1
            continue

        rows.append({
            "name": name,
            "website": website,
            "industry": company.get("industry") or "",
            "batch": company.get("batch") or "",
            "team_size": company.get("team_size") or "",
            "is_hiring": "true" if company.get("isHiring") else "false",
            "status": status,
            "one_liner": (company.get("one_liner") or "").replace("\n", " ").strip(),
        })

    path = out_dir / "yc_companies.csv"
    _write_csv(path, ["name", "website", "industry", "batch", "team_size", "is_hiring", "status", "one_liner"], rows)
    log.info("YC: %d included, %d skipped (no website or inactive)", len(rows), skipped)
    return len(rows)


def fetch_greenhouse(out_dir: Path) -> int:
    """Fetch Greenhouse board tokens, write to greenhouse_tokens.csv."""
    tokens = _fetch_json(GREENHOUSE_URL)
    if not isinstance(tokens, list):
        log.error("Unexpected Greenhouse response shape")
        return 0

    rows = []
    for token in tokens:
        token = (token or "").strip()
        if not token:
            continue
        rows.append({
            "token": token,
            "careers_url": f"https://boards.greenhouse.io/{token}",
            "api_url": f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true",
        })

    path = out_dir / "greenhouse_tokens.csv"
    _write_csv(path, ["token", "careers_url", "api_url"], rows)
    return len(rows)


def fetch_lever(out_dir: Path) -> int:
    """Fetch Lever company slugs, write to lever_tokens.csv."""
    slugs = _fetch_json(LEVER_URL)
    if not isinstance(slugs, list):
        log.error("Unexpected Lever response shape")
        return 0

    rows = []
    for slug in slugs:
        slug = (slug or "").strip()
        if not slug:
            continue
        rows.append({
            "token": slug,
            "careers_url": f"https://jobs.lever.co/{slug}",
            "api_url": f"https://api.lever.co/v0/postings/{slug}?mode=json",
        })

    path = out_dir / "lever_tokens.csv"
    _write_csv(path, ["token", "careers_url", "api_url"], rows)
    return len(rows)


def fetch_ashby(out_dir: Path) -> int:
    """Fetch Ashby company slugs, write to ashby_tokens.csv."""
    slugs = _fetch_json(ASHBY_URL)
    if not isinstance(slugs, list):
        log.error("Unexpected Ashby response shape")
        return 0

    rows = []
    for slug in slugs:
        slug = (slug or "").strip()
        if not slug:
            continue
        rows.append({
            "token": slug,
            "careers_url": f"https://jobs.ashbyhq.com/{slug}",
        })

    path = out_dir / "ashby_tokens.csv"
    _write_csv(path, ["token", "careers_url"], rows)
    return len(rows)


def fetch_workday(out_dir: Path) -> int:
    """Fetch Workday tenants, write to workday_tenants.csv.

    Source format: "company|instance|career_site"  e.g. "3m|wd1|search"
    Output URL:    https://{company}.{instance}.myworkdayjobs.com/en-US/{career_site}
    """
    entries = _fetch_json(WORKDAY_URL)
    if not isinstance(entries, list):
        log.error("Unexpected Workday response shape")
        return 0

    rows = []
    malformed = 0
    for entry in entries:
        entry = (entry or "").strip()
        parts = entry.split("|")
        if len(parts) != 3:
            malformed += 1
            continue
        company, instance, site = [p.strip() for p in parts]
        if not company or not instance or not site:
            malformed += 1
            continue
        rows.append({
            "company": company,
            "instance": instance,
            "site": site,
            "careers_url": f"https://{company}.{instance}.myworkdayjobs.com/en-US/{site}",
        })

    if malformed:
        log.warning("Workday: %d malformed entries skipped", malformed)

    # Deduplicate on careers_url (some companies have multiple career sites)
    seen = set()
    deduped = []
    for row in rows:
        key = row["careers_url"]
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    path = out_dir / "workday_tenants.csv"
    _write_csv(path, ["company", "instance", "site", "careers_url"], deduped)
    log.info("Workday: %d unique tenants (%d dupes removed)", len(deduped), len(rows) - len(deduped))
    return len(deduped)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def generate_config(out_dir: Path) -> None:
    """Read the fetched CSVs and write data/seed_config.yaml.

    This is a config include file that feeds all ATS adapters at once.
    Load it by setting:  JOB_SCRAPER_CONFIG_INCLUDES=data/seed_config.yaml
    """
    config: dict = {}

    # Greenhouse
    gh_path = out_dir / "greenhouse_tokens.csv"
    if gh_path.exists():
        with open(gh_path, newline="", encoding="utf-8") as f:
            tokens = [row["token"] for row in csv.DictReader(f) if row.get("token")]
        config["greenhouse"] = {"boards": tokens}
        log.info("Config: %d greenhouse boards", len(tokens))

    # Lever
    lever_path = out_dir / "lever_tokens.csv"
    if lever_path.exists():
        with open(lever_path, newline="", encoding="utf-8") as f:
            tokens = [row["token"] for row in csv.DictReader(f) if row.get("token")]
        config["lever"] = {"sites": tokens}
        log.info("Config: %d lever sites", len(tokens))

    # Ashby
    ashby_path = out_dir / "ashby_tokens.csv"
    if ashby_path.exists():
        with open(ashby_path, newline="", encoding="utf-8") as f:
            tokens = [row["token"] for row in csv.DictReader(f) if row.get("token")]
        config["ashby"] = {"companies": tokens}
        log.info("Config: %d ashby companies", len(tokens))

    # Workday — adapter expects list of {host, tenant, site} dicts
    wd_path = out_dir / "workday_tenants.csv"
    if wd_path.exists():
        with open(wd_path, newline="", encoding="utf-8") as f:
            sites = []
            for row in csv.DictReader(f):
                company = row.get("company", "").strip()
                instance = row.get("instance", "").strip()
                site = row.get("site", "").strip()
                if company and instance and site:
                    sites.append({
                        "host": f"{company}.{instance}.myworkdayjobs.com",
                        "tenant": company,
                        "site": site,
                    })
        config["workday"] = {"sites": sites}
        log.info("Config: %d workday sites", len(sites))

    out_path = out_dir / "seed_config.yaml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated by scripts/build_seed.py — do not edit manually\n")
        f.write("# Re-generate with: python scripts/build_seed.py --all\n")
        f.write("# Activate with:    export JOB_SCRAPER_CONFIG_INCLUDES=data/seed_config.yaml\n\n")
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=True)

    log.info("Wrote seed_config.yaml → %s", out_path)


def main():
    parser = argparse.ArgumentParser(
        description="Build company seed data from public sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--all", action="store_true", help="Fetch all sources")
    parser.add_argument("--yc", action="store_true", help="Fetch YC companies")
    parser.add_argument("--greenhouse", action="store_true", help="Fetch Greenhouse tokens")
    parser.add_argument("--lever", action="store_true", help="Fetch Lever slugs")
    parser.add_argument("--ashby", action="store_true", help="Fetch Ashby slugs")
    parser.add_argument("--workday", action="store_true", help="Fetch Workday tenants")
    parser.add_argument(
        "--out",
        default="data",
        help="Output directory (default: data/)",
    )
    args = parser.parse_args()

    if not any([args.all, args.yc, args.greenhouse, args.lever, args.ashby, args.workday]):
        parser.print_help()
        sys.exit(1)

    out_dir = Path(args.out)
    totals = {}

    if args.all or args.yc:
        totals["yc"] = fetch_yc(out_dir)

    if args.all or args.greenhouse:
        totals["greenhouse"] = fetch_greenhouse(out_dir)

    if args.all or args.lever:
        totals["lever"] = fetch_lever(out_dir)

    if args.all or args.ashby:
        totals["ashby"] = fetch_ashby(out_dir)

    if args.all or args.workday:
        totals["workday"] = fetch_workday(out_dir)

    # Always regenerate the config include after any fetch
    generate_config(out_dir)

    print("\n--- Summary ---")
    total = 0
    for source, count in totals.items():
        print(f"  {source:<12} {count:>6,} records")
        total += count
    print(f"  {'TOTAL':<12} {total:>6,} records")
    print(f"\nFiles written to: {out_dir.resolve()}/")
    print(f"\nTo activate:")
    print(f"  export JOB_SCRAPER_CONFIG_INCLUDES=data/seed_config.yaml")


if __name__ == "__main__":
    main()
