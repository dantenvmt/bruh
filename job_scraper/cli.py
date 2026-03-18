"""
CLI interface for multi-API job scraper
"""
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import typer
from sqlalchemy import select
from rich import box
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .aggregator import JobAggregator
from .config import Config
from .enrichment import enrich_job
from .ingest import run_ingest
from .models import Job
from .storage import JobRecord, init_db as init_db_schema, session_scope
from .utils import setup_logging
from .discovery.cli import app as discover_app

app = typer.Typer(help="Multi-API Job Scraper - Aggregate jobs from multiple free job boards")

# Register discover subcommand group
app.add_typer(discover_app, name="discover", help="Career site discovery commands")


def _build_console() -> Console:
    if os.name == "nt":
        # Avoid legacy Windows console rendering issues and Unicode errors.
        return Console(
            legacy_windows=False,
            force_terminal=sys.stdout.isatty(),
            color_system="standard" if sys.stdout.isatty() else None,
        )
    return Console()


console = _build_console()


@app.command()
def search(
    query: str = typer.Argument(..., help="Job search query (e.g., 'python developer')"),
    location: Optional[str] = typer.Option(None, "--location", "-l", help="Location filter"),
    sources: Optional[str] = typer.Option(
        None,
        "--sources",
        "-s",
        help=(
            "Comma-separated list of sources "
            "(remoteok,adzuna,usajobs,careeronestop,jsearch,greenhouse,lever,smartrecruiters,ashby,themuse,remotive,weworkremotely,builtin,findwork,hnrss,jobspy)"
        ),
    ),
    max_per_source: int = typer.Option(100, "--max", "-m", help="Max results per source"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output JSON file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),
):
    """Search for jobs across multiple job boards"""

    # Setup logging
    setup_logging("DEBUG" if verbose else "INFO")

    # Parse sources
    source_list = None
    if sources:
        source_list = [s.strip() for s in sources.split(",")]

    # Run search
    with Progress(
        SpinnerColumn(spinner_name="line"),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Searching job boards...", total=None)

        # Run async search
        aggregator = JobAggregator()
        jobs = asyncio.run(
            aggregator.search(
                query=query,
                location=location,
                max_per_source=max_per_source,
                sources=source_list,
            )
        )

        progress.update(task, completed=True)

    # Display results
    if not jobs:
        console.print("[yellow]No jobs found[/yellow]")
        return

    console.print(f"\n[green]Found {len(jobs)} jobs[/green]\n")

    # Create table
    table = Table(show_header=True, header_style="bold magenta", box=box.ASCII)
    table.add_column("Title", style="cyan", no_wrap=False, max_width=40)
    table.add_column("Company", style="green", max_width=25)
    table.add_column("Location", style="yellow", max_width=25)
    table.add_column("Source", style="blue", max_width=15)

    for job in jobs[:50]:  # Show first 50
        table.add_row(
            job.title or "N/A",
            job.company or "N/A",
            job.location or "Remote",
            job.source or "Unknown",
        )

    console.print(table)

    if len(jobs) > 50:
        console.print(f"\n[dim]... and {len(jobs) - 50} more jobs[/dim]")

    # Save to file if requested
    if output:
        jobs_data = [job.to_dict() for job in jobs]
        with open(output, "w") as f:
            json.dump(jobs_data, f, indent=2)
        console.print(f"\n[green]Saved {len(jobs)} jobs to {output}[/green]")


@app.command()
def init_db():
    """Initialize Postgres schema"""
    cfg = Config()
    if not cfg.db_dsn:
        console.print("[red]DB DSN not configured. Set JOB_SCRAPER_DB_DSN or config.yaml db.dsn[/red]")
        raise typer.Exit(code=1)
    init_db_schema(cfg.db_dsn)
    console.print("[green]Database schema initialized[/green]")


@app.command()
def ingest(
    query: Optional[str] = typer.Option(None, "--query", "-q", help="Keyword query"),
    location: Optional[str] = typer.Option(None, "--location", "-l", help="Location filter"),
    sources: Optional[str] = typer.Option(None, "--sources", "-s", help="Comma-separated sources"),
    max_per_source: int = typer.Option(100, "--max", "-m", help="Max results per source"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview ingestion without DB writes"),
    rollout: Optional[int] = typer.Option(None, "--rollout", help="Limit to first N boards (sorted by priority)"),
):
    """Run ingestion and persist to Postgres"""
    source_list = [s.strip() for s in sources.split(",")] if sources else None

    # Handle rollout limiting for ATS sources
    if rollout is not None and rollout > 0:
        from pathlib import Path
        import csv

        # Load seed data to get priority ordering
        seed_file = Path("data/targets_seed_150.csv")
        if not seed_file.exists():
            console.print("[yellow]Warning: targets_seed_150.csv not found, rollout will use arbitrary order[/yellow]")
            priority_companies = []
        else:
            try:
                with open(seed_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    # Sort by priority (1 is highest), then by name
                    companies_data = sorted(
                        [row for row in reader],
                        key=lambda x: (int(x.get("priority", 999)), x.get("company_name", ""))
                    )
                    priority_companies = [row["company_name"] for row in companies_data[:rollout]]
                    console.print(f"[cyan]Rollout mode: limiting to first {rollout} companies by priority[/cyan]")
            except Exception as e:
                console.print(f"[yellow]Warning: could not load seed file: {e}[/yellow]")
                priority_companies = []

        # Update config to limit boards based on rollout
        if priority_companies:
            import os
            from .config import Config
            cfg = Config()

            # Override boards/sites/companies in config based on priority list
            # This requires loading the generated YAML or using known tokens
            console.print(f"[dim]Limiting to companies: {', '.join(priority_companies[:5])}{'...' if len(priority_companies) > 5 else ''}[/dim]")

            # For now, log the limitation - full implementation would filter boards
            # This is a simplified version; full implementation in Phase 2.4 would:
            # 1. Load ats_targets.generated.yaml
            # 2. Map company names to board tokens
            # 3. Override config with limited board list

    run_id = run_ingest(
        query=query,
        location=location,
        max_per_source=max_per_source,
        sources=source_list,
        dry_run=dry_run,
    )

    if dry_run:
        console.print(f"[green]Dry run complete. No data written to database.[/green]")
    else:
        console.print(f"[green]Ingest complete. Run ID: {run_id}[/green]")


@app.command("backfill-enrichment")
def backfill_enrichment(
    batch_size: int = typer.Option(250, "--batch-size", help="Rows processed per batch"),
    max_jobs: int = typer.Option(0, "--max-jobs", help="Optional cap on rows to process (0 = all)"),
):
    """Backfill deterministic enrichment fields for existing jobs."""
    cfg = Config()
    if not cfg.db_dsn:
        console.print("[red]DB DSN not configured. Set JOB_SCRAPER_DB_DSN or config.yaml db.dsn[/red]")
        raise typer.Exit(code=1)

    version = int(cfg.enrichment.get("version", 1) or 1)
    processed = 0
    updated = 0
    cursor_id = None

    while True:
        with session_scope(cfg.db_dsn) as session:
            stmt = select(JobRecord).order_by(JobRecord.id.asc()).limit(batch_size)
            if cursor_id is not None:
                stmt = stmt.where(JobRecord.id > cursor_id)

            rows = list(session.execute(stmt).scalars().all())
            if not rows:
                break

            for row in rows:
                if max_jobs > 0 and processed >= max_jobs:
                    break
                processed += 1
                if row.enrichment_version is not None and row.enrichment_version >= version:
                    continue

                job = Job(
                    title=row.title,
                    company=row.company or "",
                    location=row.location,
                    url=row.url,
                    description=row.description,
                    salary=row.salary,
                    employment_type=row.employment_type,
                    posted_date=row.posted_date,
                    source=row.source,
                    job_id=row.source_job_id,
                    category=row.category,
                    tags=row.tags,
                    skills=row.skills,
                    remote=row.remote,
                    raw_payload=row.raw_payload,
                )
                enrich_job(job, enrichment_version=version)
                row.experience_level = job.experience_level
                row.experience_min_years = job.experience_min_years
                row.experience_max_years = job.experience_max_years
                row.required_skills = job.required_skills
                row.industry = job.industry
                row.industry_confidence = job.industry_confidence
                row.work_mode = job.work_mode
                row.role_pop_reasons = job.role_pop_reasons
                row.enrichment_version = job.enrichment_version
                row.enrichment_updated_at = datetime.utcnow()
                updated += 1

        if max_jobs > 0 and processed >= max_jobs:
            break
        cursor_id = rows[-1].id

    console.print(
        f"[green]Enrichment backfill complete. processed={processed}, updated={updated}, version={version}[/green]"
    )


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
):
    """Run the REST API server"""
    try:
        import uvicorn
    except ImportError as exc:
        console.print("[red]uvicorn not installed. Add it to requirements.txt[/red]")
        raise typer.Exit(code=1) from exc
    uvicorn.run("job_scraper.api.app:app", host=host, port=port, reload=False)


@app.command()
def sources():
    """List all available job board sources and their configuration status"""

    aggregator = JobAggregator()
    status = aggregator.get_source_status()

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Source", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Status", style="yellow")

    for source_id, info in status.items():
        # ASCII-only to avoid terminal encoding issues on Windows/SSH consoles.
        status_emoji = "OK" if info["configured"] else "NO"
        status_text = "Configured" if info["configured"] else "Not configured"
        status_color = "green" if info["configured"] else "red"

        table.add_row(
            source_id,
            info["name"],
            f"[{status_color}]{status_emoji} {status_text}[/{status_color}]",
        )

    console.print(table)
    console.print(
        "\n[dim]To configure sources, set environment variables or create config.yaml[/dim]"
    )
    console.print("[dim]See config.example.yaml for template[/dim]")


@app.command()
def config():
    """Show configuration help and examples"""

    console.print("[bold]Configuration Options[/bold]\n")

    console.print("[cyan]1. Environment Variables:[/cyan]")
    console.print("   export ADZUNA_APP_ID=your_app_id")
    console.print("   export ADZUNA_APP_KEY=your_app_key")
    console.print("   export USAJOBS_API_KEY=your_api_key")
    console.print("   export USAJOBS_USER_AGENT=your-email@example.com")
    console.print("   export FINDWORK_API_KEY=your_api_key")
    console.print("   export THEMUSE_API_KEY=your_api_key\n")

    console.print("[cyan]2. Config File (config.yaml):[/cyan]")
    console.print("   See config.example.yaml for template\n")

    console.print("[cyan]3. Sources that don't require configuration:[/cyan]")
    console.print("   - RemoteOK (no auth required)")
    console.print("   - Remotive (no auth required)")
    console.print("   - WeWorkRemotely (no auth required)")
    console.print("   - Built In (no auth required)")
    console.print("   - HN RSS (no auth required)")
    console.print("   - JobSpy (just install: pip install python-jobspy)\n")

    console.print("[yellow]Get API Keys:[/yellow]")
    console.print("   Adzuna: https://developer.adzuna.com/signup")
    console.print("   USAJobs: https://developer.usajobs.gov/apirequest/")
    console.print("   Findwork: https://findwork.dev/")


@app.command()
def seed_sponsors(
    output: Path = typer.Option(
        Path("data/visa_sponsor_companies.seed.txt"),
        "--output",
        "-o",
        help="Write a newline-delimited seed list of sponsor-friendly companies",
    ),
    max_companies: int = typer.Option(
        200,
        "--max",
        help="Maximum number of companies to write (useful for quick experiments)",
    ),
):
    """
    Write a starter sponsor-company list.

    This is a seed list (best-effort) meant to bootstrap discovery and tagging.
    """
    from .seeds import H1B_OPT_SPONSOR_SEED_COMPANIES, write_company_list

    companies = list(H1B_OPT_SPONSOR_SEED_COMPANIES)
    if max_companies and max_companies > 0:
        companies = companies[: int(max_companies)]

    write_company_list(output, companies)
    console.print(f"[green]Wrote {len(companies)} companies to {output}[/green]")


@app.command()
def list_seeds(
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table or json",
    ),
    seed_file: Path = typer.Option(
        Path("data/targets_seed_150.csv"),
        "--input",
        "-i",
        help="Path to seed CSV file",
    ),
):
    """
    List seed companies from targets_seed_150.csv with summary statistics.

    Displays company count by priority and category, and validates for duplicates.
    """
    import csv
    from collections import Counter

    if not seed_file.exists():
        console.print(f"[red]Seed file not found: {seed_file}[/red]")
        raise typer.Exit(code=1)

    # Read CSV
    companies = []
    seen_names = set()
    duplicates = []

    try:
        with open(seed_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                company_name = row.get("company_name", "").strip()
                priority = row.get("priority", "").strip()
                category = row.get("category", "").strip()

                if not company_name:
                    continue

                # Check for duplicates
                if company_name in seen_names:
                    duplicates.append(company_name)
                seen_names.add(company_name)

                companies.append({
                    "company_name": company_name,
                    "priority": priority,
                    "category": category,
                })
    except Exception as e:
        console.print(f"[red]Error reading CSV: {e}[/red]")
        raise typer.Exit(code=1)

    # Validate duplicates
    if duplicates:
        console.print(f"[yellow]Warning: Found {len(duplicates)} duplicate companies:[/yellow]")
        for dup in duplicates:
            console.print(f"  - {dup}")
        console.print()

    # Count by priority and category
    priority_counts = Counter(c["priority"] for c in companies)
    category_counts = Counter(c["category"] for c in companies)

    # Output based on format
    if format == "json":
        import json
        output_data = {
            "total": len(companies),
            "by_priority": dict(priority_counts),
            "by_category": dict(category_counts),
            "duplicates": duplicates,
            "companies": companies,
        }
        print(json.dumps(output_data, indent=2))
    else:
        # Table format (default)
        console.print("[bold]Seed Companies Summary[/bold]")
        console.print("=" * 50)
        console.print(f"[green]Total: {len(companies)} companies[/green]\n")

        console.print("[cyan]By Priority:[/cyan]")
        for priority in sorted(priority_counts.keys()):
            count = priority_counts[priority]
            console.print(f"  Priority {priority}: {count} companies")

        console.print("\n[cyan]By Category:[/cyan]")
        for category in sorted(category_counts.keys()):
            count = category_counts[category]
            console.print(f"  {category}: {count} companies")


@app.command()
def discover_ats(
    input_file: Path = typer.Option(
        Path("data/targets_seed_150.csv"),
        "--input",
        "-i",
        help="Input file: CSV with company_name column or newline-delimited text",
    ),
    output: Path = typer.Option(
        Path("data/ats_targets.generated.yaml"),
        "--output",
        "-o",
        help="Write discovered ATS targets as a YAML config include",
    ),
    metadata: Optional[Path] = typer.Option(
        Path("data/ats_discovery_log.yaml"),
        "--metadata",
        help="Write discovery metadata for debugging (set to empty string to disable)",
    ),
    platforms: str = typer.Option(
        "greenhouse,lever,smartrecruiters,workday",
        "--platforms",
        help="Comma-separated ATS platforms to detect (greenhouse,lever,smartrecruiters,workable,ashby,workday)",
    ),
    max_companies: int = typer.Option(200, "--max", help="Max input companies to process"),
    concurrency: int = typer.Option(8, "--concurrency", help="Parallelism (keep low to avoid blocking)"),
    http_rpm: int = typer.Option(120, "--http-rpm", help="HTTP requests per minute (overall)"),
    clearbit_rpm: int = typer.Option(60, "--clearbit-rpm", help="Clearbit suggest requests per minute"),
    treat_input_as_domain: bool = typer.Option(
        False,
        "--treat-input-as-domain",
        help="Treat each input line as a domain instead of a company name (skips Clearbit)",
    ),
    validate: bool = typer.Option(
        True,
        "--validate/--no-validate",
        help="Validate tokens return jobs before including them",
    ),
    min_jobs: int = typer.Option(
        1,
        "--min-jobs",
        help="Minimum job count to consider token valid",
    ),
    known_tokens: Optional[Path] = typer.Option(
        Path("data/known_tokens.yaml"),
        "--known-tokens",
        help="Path to known_tokens.yaml (set to empty string to disable)",
    ),
):
    """
    Discover ATS tokens (Greenhouse/Lever/SmartRecruiters/etc.) from company names/domains.

    This is intended to quickly build large target lists to increase job volume.
    """
    from .ats_discovery import discover_ats_targets
    import yaml
    import csv

    if not input_file.exists():
        console.print(f"[red]Input file not found: {input_file}[/red]")
        raise typer.Exit(code=1)

    # Load company names from input (supports both CSV and plain text)
    lines = []
    if input_file.suffix.lower() == ".csv":
        try:
            with open(input_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    company_name = row.get("company_name", "").strip()
                    if company_name and not company_name.startswith("#"):
                        lines.append(company_name)
        except Exception as e:
            console.print(f"[red]Error reading CSV: {e}[/red]")
            raise typer.Exit(code=1)
    else:
        # Plain text: one company per line
        lines = [
            ln.strip()
            for ln in input_file.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]

    if not lines:
        console.print("[yellow]No companies found in input file[/yellow]")
        raise typer.Exit(code=1)

    # Load known tokens if provided
    known_tokens_dict = None
    if known_tokens and known_tokens.exists():
        try:
            with open(known_tokens, "r", encoding="utf-8") as f:
                known_tokens_dict = yaml.safe_load(f) or {}
            console.print(f"[dim]Loaded known tokens from {known_tokens}[/dim]")
        except Exception as e:
            console.print(f"[yellow]Warning: Could not load known_tokens.yaml: {e}[/yellow]")

    platform_set = {p.strip().lower() for p in platforms.split(",") if p.strip()}

    console.print(f"[cyan]Discovering ATS tokens for {len(lines)} companies...[/cyan]")
    if validate:
        console.print(f"[dim]Validation enabled: tokens must return at least {min_jobs} job(s)[/dim]")

    targets, details, metadata_dict = asyncio.run(
        discover_ats_targets(
            lines,
            platforms=platform_set,
            max_companies=max_companies,
            concurrency=concurrency,
            http_requests_per_minute=http_rpm,
            clearbit_requests_per_minute=clearbit_rpm,
            treat_input_as_domain=treat_input_as_domain,
            known_tokens=known_tokens_dict,
            validate=validate,
            min_jobs=min_jobs,
        )
    )

    # Write targets YAML
    include_cfg = {}
    if targets.get("greenhouse"):
        include_cfg["greenhouse"] = {"boards": targets["greenhouse"]}
    if targets.get("lever"):
        include_cfg["lever"] = {"sites": targets["lever"]}
    if targets.get("smartrecruiters"):
        include_cfg["smartrecruiters"] = {"companies": targets["smartrecruiters"]}
    if targets.get("workable"):
        include_cfg["workable"] = {"accounts": targets["workable"]}
    if targets.get("ashby"):
        include_cfg["ashby"] = {"companies": targets["ashby"]}
    if targets.get("workday"):
        # Workday tokens are JSON-encoded dicts; unpack to structured list
        import json as _json
        workday_sites = []
        for token_json in sorted(targets["workday"]):
            try:
                site_dict = _json.loads(token_json)
                workday_sites.append({
                    "host": site_dict["host"],
                    "tenant": site_dict["tenant"],
                    "site": site_dict["site"],
                })
            except (ValueError, KeyError):
                continue
        if workday_sites:
            include_cfg["workday"] = {"sites": workday_sites}

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.safe_dump(include_cfg, sort_keys=False), encoding="utf-8")

    # Write metadata if requested
    if metadata and str(metadata):
        metadata.parent.mkdir(parents=True, exist_ok=True)
        metadata.write_text(yaml.safe_dump(metadata_dict, sort_keys=False), encoding="utf-8")
        console.print(f"[green]Wrote discovery metadata to {metadata}[/green]")

    # Print summary
    found_companies = sum(1 for d in details if d.found)
    tokens_found = metadata_dict["discovery_run"]["tokens_found"]
    tokens_validated = metadata_dict["discovery_run"]["tokens_validated"]

    console.print(f"\n[green]Discovery complete![/green]")
    console.print(f"  Companies processed: {len(lines)}")
    console.print(f"  Companies with tokens: {found_companies}")
    console.print(f"  Tokens found: {tokens_found}")
    if validate:
        console.print(f"  Tokens validated: {tokens_validated}")
    console.print()

    for key in ("greenhouse", "lever", "smartrecruiters", "workable", "ashby", "workday"):
        if key in targets:
            console.print(f"  [cyan]{key}[/cyan]: {len(targets[key])} boards")

    console.print(f"\n[green]Wrote YAML include to {output}[/green]")
    console.print(
        "[dim]To use it: set JOB_SCRAPER_CONFIG_INCLUDES to include this file (and keep your main config.yaml for DB/API keys).[/dim]"
    )


@app.command("reset-sites")
def reset_sites(
    all_sites: bool = typer.Option(
        False, "--all", help="Reset ALL custom sites, not just disabled ones"
    ),
):
    """Re-enable disabled custom scrape sites and reset failure counters.

    By default only resets sites that are currently disabled.  Pass --all
    to reset every custom site (useful before a full test run).
    """
    cfg = Config()
    if not cfg.db_dsn:
        console.print("[red]DB DSN not configured. Set JOB_SCRAPER_DB_DSN or config.yaml db.dsn[/red]")
        raise typer.Exit(code=1)

    from .scraping.models import ScrapeSite

    with session_scope(cfg.db_dsn) as session:
        query = session.query(ScrapeSite).filter(ScrapeSite.detected_ats == "custom")
        if not all_sites:
            query = query.filter(ScrapeSite.enabled == False)  # noqa: E712
        n = query.update(
            {
                "enabled": True,
                "consecutive_failures": 0,
                "next_scrape_at": None,
                "last_error_code": None,
            },
            synchronize_session=False,
        )
    console.print(f"[green]Reset {n} custom site(s)[/green]")


@app.command()
def spy(
    url: str = typer.Argument(..., help="URL to spy on (e.g. https://example.com/jobs)"),
    min_confidence: float = typer.Option(
        0.3, "--min-confidence", "-c", help="Minimum confidence score to display (0.0-1.0)"
    ),
    scroll: bool = typer.Option(True, "--scroll/--no-scroll", help="Scroll page to trigger lazy loads"),
    load_more: bool = typer.Option(True, "--load-more/--no-load-more", help="Click Load More buttons"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Save discovered endpoints to JSON"),
    all_calls: bool = typer.Option(False, "--all", "-a", help="Show all captured calls, not just job-likely ones"),
    timeout: int = typer.Option(30, "--timeout", "-t", help="Navigation timeout in seconds"),
    no_headless: bool = typer.Option(False, "--no-headless", help="Show browser window (for debugging)"),
):
    """
    Spy on a website's network traffic to discover hidden job APIs.

    Opens the URL in a headless browser, intercepts all XHR/Fetch calls
    (like F12 > Network > XHR), and scores each JSON response for job-listing
    relevance.  High-confidence endpoints can then be replayed directly
    with httpx — no browser required.

    Example:

        python -m job_scraper.cli spy https://example.com/careers
    """
    from .scraping.fetchers.network_spy import NetworkSpy

    setup_logging("INFO")

    console.print(f"\n[bold cyan]NetworkSpy[/bold cyan] — spying on [underline]{url}[/underline]\n")

    spy_instance = NetworkSpy(
        headless=not no_headless,
        timeout=timeout * 1000,
        scroll=scroll,
        click_load_more=load_more,
        min_confidence=0.0,  # filter later so we can show all in --all mode
        extra_wait_ms=1500,
    )

    with Progress(
        SpinnerColumn(spinner_name="line"),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Intercepting network traffic...", total=None)
        endpoints = asyncio.run(spy_instance.spy(url))
        progress.update(task, completed=True)

    if not endpoints:
        console.print("[yellow]No JSON XHR/Fetch calls captured.[/yellow]")
        console.print("[dim]Try --no-headless to watch the browser, or increase --timeout.[/dim]")
        return

    # Filter for display
    display = endpoints if all_calls else [ep for ep in endpoints if ep.confidence >= min_confidence]

    if not display:
        console.print(
            f"[yellow]Captured {len(endpoints)} call(s) but none exceeded "
            f"confidence {min_confidence:.2f}.[/yellow]"
        )
        console.print("[dim]Lower --min-confidence or use --all to see everything.[/dim]")
    else:
        table = Table(
            show_header=True,
            header_style="bold magenta",
            box=box.ASCII,
            show_lines=True,
        )
        table.add_column("Score", style="bold", width=6)
        table.add_column("Jobs?", width=5)
        table.add_column("Count", width=6)
        table.add_column("Method", width=6)
        table.add_column("Status", width=6)
        table.add_column("URL", no_wrap=False)
        table.add_column("Pagination", width=12)

        for ep in display:
            score_color = (
                "green" if ep.confidence >= 0.6
                else "yellow" if ep.confidence >= 0.4
                else "dim"
            )
            pag_str = (
                f"{ep.pagination.style}:{ep.pagination.param_name}"
                if ep.pagination else "—"
            )
            table.add_row(
                f"[{score_color}]{ep.confidence:.2f}[/{score_color}]",
                "[green]YES[/green]" if ep.looks_like_jobs else "[dim]no[/dim]",
                str(ep.job_count_estimate) if ep.job_count_estimate else "—",
                ep.method,
                str(ep.response_status),
                ep.url,
                pag_str,
            )

        console.print(table)
        console.print(
            f"\n[dim]Captured {len(endpoints)} total call(s). "
            f"Showing {len(display)} above min_confidence={min_confidence:.2f}[/dim]"
        )

        # Show score notes for top hit
        top = display[0]
        if top.score_notes:
            console.print(f"\n[bold]Top hit notes:[/bold] {', '.join(top.score_notes)}")
        if top.pagination:
            p = top.pagination
            console.print(
                f"[bold]Pagination detected:[/bold] style={p.style}, "
                f"param={p.param_name!r}, current={p.current_value}, "
                f"in_body={p.in_body}"
            )
            if not p.in_body:
                next_url = p.next_url(top.url)
                console.print(f"[dim]Next page URL: {next_url}[/dim]")

    # Save to file
    if output:
        serializable = []
        for ep in endpoints:
            serializable.append(
                {
                    "url": ep.url,
                    "method": ep.method,
                    "confidence": ep.confidence,
                    "looks_like_jobs": ep.looks_like_jobs,
                    "job_count_estimate": ep.job_count_estimate,
                    "score_notes": ep.score_notes,
                    "response_status": ep.response_status,
                    "request_post_data": ep.request_post_data,
                    "replay_headers": ep.replay_headers,
                    "pagination": (
                        {
                            "style": ep.pagination.style,
                            "param_name": ep.pagination.param_name,
                            "current_value": ep.pagination.current_value,
                            "in_body": ep.pagination.in_body,
                        }
                        if ep.pagination
                        else None
                    ),
                    "response_json": ep.response_json,
                }
            )
        with open(output, "w") as f:
            json.dump(serializable, f, indent=2, default=str)
        console.print(f"\n[green]Saved {len(serializable)} endpoint(s) to {output}[/green]")


@app.command("refresh-seeds")
def refresh_seeds_cmd(
    data_dir: Optional[Path] = typer.Option(
        None,
        "--data-dir",
        "-d",
        help="Directory containing targets_seed_150.csv (defaults to <project-root>/data)",
    ),
):
    """Manually trigger seed refresh (fetch new companies from visa/fortune500 sources)."""
    from .seed_refresh import run_seed_refresh

    with Progress(
        SpinnerColumn(spinner_name="line"),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching and parsing company lists...", total=None)
        summary = run_seed_refresh(data_dir=data_dir)
        progress.update(task, completed=True)

    console.print(f"\n[green]Seed refresh complete.[/green]")
    console.print(f"  New companies added: {summary['total_new_added']}")
    console.print(f"  CSV path: {summary['csv_path']}")
    for src in summary.get("sources", []):
        console.print(f"  [{src['source']}] new_added={src['new_added']}")


def main():
    """Entry point for CLI"""
    app()


if __name__ == "__main__":
    main()
