"""
Configuration management for API keys and settings
"""
import json
import logging
import os
from pathlib import Path
from typing import Optional
import yaml


logger = logging.getLogger(__name__)


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on", "y"}


def _to_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    text = str(value).strip()
    if not text:
        return []
    return [v.strip() for v in text.split(",") if v.strip()]


def _to_int(value, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_int(value, *, default: int, minimum: int, name: str = "") -> int:
    """Parse an int and clamp to *minimum*; warn + use *default* on bad input."""
    v = _to_int(value, default)
    if v < minimum:
        logger.warning(
            "%s=%s is below minimum %d — using default %d",
            name or "config", value, minimum, default,
        )
        return default
    return v


def _clamp_float(value, *, default: float, minimum: float, name: str = "") -> float:
    """Parse a float and clamp to *minimum*; warn + use *default* on bad input."""
    v = _to_float(value, default)
    if v < minimum:
        logger.warning(
            "%s=%s is below minimum %.1f — using default %.1f",
            name or "config", value, minimum, default,
        )
        return default
    return v


def _parse_workday_sites(value) -> list:
    """Parse WORKDAY_SITES from JSON string or list of dicts.

    Each entry must have host, tenant, site.  Host must match
    *.myworkdayjobs.com.  Dedupes by (host, tenant, site).
    """
    if value is None:
        return []
    raw = value
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return []
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("WORKDAY_SITES is not valid JSON")
            return []
    if not isinstance(raw, list):
        return []

    seen = set()
    sites = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        host = (item.get("host") or "").strip().lower().rstrip("/")
        tenant = (item.get("tenant") or "").strip()
        site = (item.get("site") or "").strip()
        if not host or not tenant or not site:
            continue
        if not host.endswith(".myworkdayjobs.com"):
            logger.warning("Workday host %s rejected — must be *.myworkdayjobs.com", host)
            continue
        key = (host, tenant.lower(), site.lower())
        if key in seen:
            continue
        seen.add(key)
        sites.append({"host": host, "tenant": tenant, "site": site})
    return sites


def _deep_update(target: dict, updates: dict) -> dict:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = value
    return target


def _load_json_secrets() -> dict:
    raw = os.getenv("JOB_SCRAPER_SECRETS_JSON")
    if raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            logger.warning("JOB_SCRAPER_SECRETS_JSON is not valid JSON")
            return {}

    path = os.getenv("JOB_SCRAPER_SECRETS_FILE")
    if path:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                parsed = json.load(handle)
            return parsed if isinstance(parsed, dict) else {}
        except FileNotFoundError:
            logger.warning("JOB_SCRAPER_SECRETS_FILE not found")
        except json.JSONDecodeError:
            logger.warning("JOB_SCRAPER_SECRETS_FILE is not valid JSON")
        except OSError:
            logger.warning("JOB_SCRAPER_SECRETS_FILE could not be read")
    return {}


def _load_aws_secrets() -> dict:
    secret_id = os.getenv("JOB_SCRAPER_AWS_SECRET_ID") or os.getenv("JOB_SCRAPER_AWS_SECRET_ARN")
    if not secret_id:
        return {}
    try:
        import boto3
    except ImportError:
        logger.warning("boto3 not installed; skipping AWS Secrets Manager load")
        return {}

    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
        if "SecretString" in response and response["SecretString"]:
            payload = json.loads(response["SecretString"])
            return payload if isinstance(payload, dict) else {}
        if "SecretBinary" in response and response["SecretBinary"]:
            decoded = response["SecretBinary"].decode("utf-8")
            payload = json.loads(decoded)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        logger.warning("AWS Secrets Manager load failed")
    return {}


def _load_secret_overrides() -> dict:
    merged = {}
    for loader in (_load_json_secrets, _load_aws_secrets):
        payload = loader()
        if payload:
            _deep_update(merged, payload)
    return merged


class Config:
    """Configuration manager for API credentials"""

    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or os.getenv(
            "JOB_SCRAPER_CONFIG", "config.yaml"
        )
        self._config = self._load_config()

    def _load_config(self) -> dict:
        """Load config from file or environment variables"""
        config = {}
        env_only = _to_bool(os.getenv("JOB_SCRAPER_ENV_ONLY", False))

        # Try to load from YAML file
        if not env_only and Path(self.config_file).exists():
            try:
                with open(self.config_file, "r", encoding="utf-8-sig") as f:
                    config = yaml.safe_load(f) or {}
            except UnicodeDecodeError:
                with open(self.config_file, "r") as f:
                    config = yaml.safe_load(f) or {}

        # Secrets load before includes so that include files (e.g. seed_config.yaml)
        # can override secrets. Priority: env vars > includes > secrets > config.yaml
        secrets = _load_secret_overrides()
        if secrets:
            _deep_update(config, secrets)

        includes_raw = os.getenv("JOB_SCRAPER_CONFIG_INCLUDES") or os.getenv("JOB_SCRAPER_CONFIG_INCLUDE")
        for include_path in _to_list(includes_raw):
            try:
                include_file = Path(include_path)
                if not include_file.exists():
                    continue
                with open(include_file, "r", encoding="utf-8-sig") as f:
                    include_config = yaml.safe_load(f) or {}
                if isinstance(include_config, dict) and include_config:
                    _deep_update(config, include_config)
            except Exception:
                logger.warning(f"Failed to load JOB_SCRAPER_CONFIG_INCLUDE file: {include_path}")

        # Override with environment variables
        config.setdefault("adzuna", {})
        config["adzuna"]["app_id"] = os.getenv(
            "ADZUNA_APP_ID", config.get("adzuna", {}).get("app_id")
        )
        config["adzuna"]["app_key"] = os.getenv(
            "ADZUNA_APP_KEY", config.get("adzuna", {}).get("app_key")
        )

        config.setdefault("usajobs", {})
        config["usajobs"]["api_key"] = os.getenv(
            "USAJOBS_API_KEY", config.get("usajobs", {}).get("api_key")
        )
        config["usajobs"]["user_agent"] = os.getenv(
            "USAJOBS_USER_AGENT",
            config.get("usajobs", {}).get("user_agent", "your-email@example.com"),
        )

        config.setdefault("themuse", {})
        config["themuse"]["api_key"] = os.getenv(
            "THEMUSE_API_KEY", config.get("themuse", {}).get("api_key")
        )

        config.setdefault("findwork", {})
        config["findwork"]["api_key"] = os.getenv(
            "FINDWORK_API_KEY", config.get("findwork", {}).get("api_key")
        )
        config["findwork"]["base_url"] = os.getenv(
            "FINDWORK_BASE_URL",
            config.get("findwork", {}).get("base_url", "https://findwork.dev/api/jobs/"),
        )
        config["findwork"]["requests_per_minute"] = _to_int(
            os.getenv(
                "FINDWORK_REQUESTS_PER_MINUTE",
                config.get("findwork", {}).get("requests_per_minute", 60),
            ),
            60,
        )

        config.setdefault("careeronestop", {})
        config["careeronestop"]["api_key"] = os.getenv(
            "CAREERONESTOP_API_KEY", config.get("careeronestop", {}).get("api_key")
        )
        config["careeronestop"]["user_id"] = os.getenv(
            "CAREERONESTOP_USER_ID", config.get("careeronestop", {}).get("user_id")
        )

        config.setdefault("jsearch", {})
        config["jsearch"]["api_key"] = os.getenv(
            "RAPIDAPI_KEY", config.get("jsearch", {}).get("api_key")
        )
        config["jsearch"]["host"] = os.getenv(
            "RAPIDAPI_HOST", config.get("jsearch", {}).get("host", "jsearch.p.rapidapi.com")
        )
        config["jsearch"]["safe_mode"] = _to_bool(
            os.getenv("JSEARCH_SAFE_MODE", config.get("jsearch", {}).get("safe_mode", True))
        )
        config["jsearch"]["min_interval_seconds"] = _to_float(
            os.getenv("JSEARCH_MIN_INTERVAL_SECONDS", config.get("jsearch", {}).get("min_interval_seconds", 1.5)),
            1.5,
        )
        config["jsearch"]["jitter_seconds"] = _to_float(
            os.getenv("JSEARCH_JITTER_SECONDS", config.get("jsearch", {}).get("jitter_seconds", 0.7)),
            0.7,
        )
        config["jsearch"]["requests_per_minute"] = _to_int(
            os.getenv("JSEARCH_REQUESTS_PER_MINUTE", config.get("jsearch", {}).get("requests_per_minute", 25)),
            25,
        )
        config["jsearch"]["max_pages"] = _to_int(
            os.getenv("JSEARCH_MAX_PAGES", config.get("jsearch", {}).get("max_pages", 10)),
            10,
        )
        config["jsearch"]["max_retries"] = _to_int(
            os.getenv("JSEARCH_MAX_RETRIES", config.get("jsearch", {}).get("max_retries", 4)),
            4,
        )
        config["jsearch"]["backoff_base_seconds"] = _to_float(
            os.getenv("JSEARCH_BACKOFF_BASE_SECONDS", config.get("jsearch", {}).get("backoff_base_seconds", 2.0)),
            2.0,
        )
        config["jsearch"]["backoff_cap_seconds"] = _to_float(
            os.getenv("JSEARCH_BACKOFF_CAP_SECONDS", config.get("jsearch", {}).get("backoff_cap_seconds", 45.0)),
            45.0,
        )
        config["jsearch"]["cooldown_every_n_requests"] = _to_int(
            os.getenv(
                "JSEARCH_COOLDOWN_EVERY_N_REQUESTS",
                config.get("jsearch", {}).get("cooldown_every_n_requests", 5),
            ),
            5,
        )
        config["jsearch"]["cooldown_seconds"] = _to_float(
            os.getenv("JSEARCH_COOLDOWN_SECONDS", config.get("jsearch", {}).get("cooldown_seconds", 8.0)),
            8.0,
        )
        config["jsearch"]["respect_retry_after"] = _to_bool(
            os.getenv(
                "JSEARCH_RESPECT_RETRY_AFTER",
                config.get("jsearch", {}).get("respect_retry_after", True),
            )
        )
        config["jsearch"]["user_agent"] = os.getenv(
            "JSEARCH_USER_AGENT", config.get("jsearch", {}).get("user_agent", "multi-api-aggregator/1.0")
        )
        config["jsearch"]["timeout_seconds"] = _to_float(
            os.getenv("JSEARCH_TIMEOUT_SECONDS", config.get("jsearch", {}).get("timeout_seconds", 30.0)),
            30.0,
        )
        config["jsearch"]["rate_limit_remaining_floor"] = _to_int(
            os.getenv(
                "JSEARCH_RATE_LIMIT_REMAINING_FLOOR",
                config.get("jsearch", {}).get("rate_limit_remaining_floor", 1),
            ),
            1,
        )

        config.setdefault("greenhouse", {})
        config["greenhouse"]["boards"] = _to_list(
            os.getenv("GREENHOUSE_BOARDS") or config.get("greenhouse", {}).get("boards")
        )
        config["greenhouse"]["include_content"] = _to_bool(
            os.getenv("GREENHOUSE_INCLUDE_CONTENT", config.get("greenhouse", {}).get("include_content", True))
        )

        config.setdefault("lever", {})
        config["lever"]["sites"] = _to_list(
            os.getenv("LEVER_SITES") or config.get("lever", {}).get("sites")
        )

        config.setdefault("smartrecruiters", {})
        config["smartrecruiters"]["companies"] = _to_list(
            os.getenv("SMARTRECRUITERS_COMPANIES") or config.get("smartrecruiters", {}).get("companies")
        )
        config["smartrecruiters"]["include_content"] = _to_bool(
            os.getenv(
                "SMARTRECRUITERS_INCLUDE_CONTENT",
                config.get("smartrecruiters", {}).get("include_content", False),
            )
        )
        config["smartrecruiters"]["requests_per_minute"] = _to_int(
            os.getenv(
                "SMARTRECRUITERS_REQUESTS_PER_MINUTE",
                config.get("smartrecruiters", {}).get("requests_per_minute", 60),
            ),
            60,
        )

        config.setdefault("ashby", {})
        config["ashby"]["companies"] = _to_list(
            os.getenv("ASHBY_COMPANIES") or config.get("ashby", {}).get("companies")
        )
        config["ashby"]["include_content"] = _to_bool(
            os.getenv("ASHBY_INCLUDE_CONTENT", config.get("ashby", {}).get("include_content", False))
        )
        config["ashby"]["requests_per_minute"] = _to_int(
            os.getenv(
                "ASHBY_REQUESTS_PER_MINUTE",
                config.get("ashby", {}).get("requests_per_minute", 60),
            ),
            60,
        )

        # RemoteOK doesn't need auth
        config.setdefault("remoteok", {})

        # Remotive doesn't need auth
        config.setdefault("remotive", {})

        # WeWorkRemotely doesn't need auth
        config.setdefault("weworkremotely", {})
        config["weworkremotely"]["base_url"] = os.getenv(
            "WEWORKREMOTELY_BASE_URL",
            config.get("weworkremotely", {}).get("base_url", "https://weworkremotely.com/remote-jobs.rss"),
        )

        # Built In doesn't need auth
        config.setdefault("builtin", {})
        config["builtin"]["domains"] = _to_list(
            os.getenv("BUILTIN_DOMAINS", config.get("builtin", {}).get("domains"))
        )
        config["builtin"]["max_pages"] = _to_int(
            os.getenv("BUILTIN_MAX_PAGES", config.get("builtin", {}).get("max_pages", 5)),
            5,
        )
        config["builtin"]["requests_per_minute"] = _to_int(
            os.getenv(
                "BUILTIN_REQUESTS_PER_MINUTE",
                config.get("builtin", {}).get("requests_per_minute", 60),
            ),
            60,
        )

        # HN RSS doesn't need auth
        config.setdefault("hnrss", {})
        config["hnrss"]["base_url"] = os.getenv(
            "HNRSS_BASE_URL", config.get("hnrss", {}).get("base_url", "https://hnrss.org/jobs")
        )

        # Apify actor-based scraping (supports multiple actors)
        config.setdefault("apify", {})
        config["apify"]["api_token"] = os.getenv(
            "APIFY_API_TOKEN", config.get("apify", {}).get("api_token")
        )
        # Comma-separated actor IDs via env var; structured list via config.yaml
        config["apify"]["actor_ids"] = os.getenv(
            "APIFY_ACTOR_IDS",
            config.get("apify", {}).get(
                "actor_ids",
                "memo23/apify-indeed-cheerio-keywords-ppr,worldunboxer/rapid-linkedin-scraper,orgupdate/google-jobs-scraper",
            ),
        )
        # Structured actor list from config.yaml (takes priority over actor_ids)
        # Format: [{id: "actor/name", label: "short-name"}, ...]
        config["apify"].setdefault("actors", config.get("apify", {}).get("actors"))
        config["apify"]["max_items"] = _to_int(
            os.getenv(
                "APIFY_MAX_ITEMS",
                config.get("apify", {}).get("max_items", 200),
            ),
            200,
        )
        config["apify"]["country"] = os.getenv(
            "APIFY_COUNTRY", config.get("apify", {}).get("country", "US")
        )

        # JobSpy doesn't need auth (it's a scraper)
        config.setdefault("jobspy", {})

        # Workday
        config.setdefault("workday", {})
        workday_sites_raw = os.getenv("WORKDAY_SITES")
        if isinstance(workday_sites_raw, str) and not workday_sites_raw.strip():
            workday_sites_raw = None
        config["workday"]["sites"] = _parse_workday_sites(
            workday_sites_raw if workday_sites_raw is not None else config.get("workday", {}).get("sites")
        )
        config["workday"]["requests_per_minute"] = _to_int(
            os.getenv(
                "WORKDAY_REQUESTS_PER_MINUTE",
                config.get("workday", {}).get("requests_per_minute", 30),
            ),
            30,
        )
        config["workday"]["include_details"] = _to_bool(
            os.getenv(
                "WORKDAY_INCLUDE_DETAILS",
                config.get("workday", {}).get("include_details", False),
            )
        )
        config["workday"]["max_details_per_site"] = _to_int(
            os.getenv(
                "WORKDAY_MAX_DETAILS_PER_SITE",
                config.get("workday", {}).get("max_details_per_site", 50),
            ),
            50,
        )
        config["workday"]["detail_concurrency"] = _to_int(
            os.getenv(
                "WORKDAY_DETAIL_CONCURRENCY",
                config.get("workday", {}).get("detail_concurrency", 3),
            ),
            3,
        )
        config["workday"]["detail_timeout"] = _to_float(
            os.getenv(
                "WORKDAY_DETAIL_TIMEOUT",
                config.get("workday", {}).get("detail_timeout", 10.0),
            ),
            10.0,
        )

        # DB + scheduler settings
        config.setdefault("db", {})
        config["db"]["dsn"] = os.getenv(
            "JOB_SCRAPER_DB_DSN",
            os.getenv("DATABASE_URL", config.get("db", {}).get("dsn")),
        )
        config["db"]["retention_days"] = int(
            os.getenv("JOB_SCRAPER_RETENTION_DAYS", config.get("db", {}).get("retention_days", 30))
        )

        config.setdefault("scheduler", {})
        config["scheduler"]["hour"] = int(
            os.getenv("JOB_SCRAPER_SCHEDULE_HOUR", config.get("scheduler", {}).get("hour", 23))
        )
        config["scheduler"]["minute"] = int(
            os.getenv("JOB_SCRAPER_SCHEDULE_MINUTE", config.get("scheduler", {}).get("minute", 59))
        )

        config["us_only"] = _to_bool(os.getenv("JOB_SCRAPER_US_ONLY", config.get("us_only", True)))

        # Visa/H1B/OPT tagging
        config.setdefault("visa", {})
        config["visa"]["tagging_enabled"] = _to_bool(
            os.getenv(
                "JOB_SCRAPER_VISA_TAGGING_ENABLED",
                config.get("visa", {}).get("tagging_enabled", True),
            )
        )
        config["visa"]["sponsor_companies"] = _to_list(
            os.getenv(
                "JOB_SCRAPER_VISA_SPONSOR_COMPANIES",
                config.get("visa", {}).get("sponsor_companies"),
            )
        )
        config["visa"]["sponsor_companies_file"] = os.getenv(
            "JOB_SCRAPER_VISA_SPONSOR_COMPANIES_FILE",
            config.get("visa", {}).get("sponsor_companies_file"),
        )

        config.setdefault("ingestion", {})
        config["ingestion"]["max_posting_age_days"] = _to_int(
            os.getenv(
                "JOB_SCRAPER_MAX_POSTING_AGE_DAYS",
                config.get("ingestion", {}).get("max_posting_age_days", 60),
            ),
            60,
        )

        config.setdefault("analytics", {})
        config["analytics"]["rate_limit"] = os.getenv(
            "JOB_SCRAPER_ANALYTICS_RATE_LIMIT",
            config.get("analytics", {}).get("rate_limit", "120/minute"),
        )
        config["analytics"]["max_batch"] = _to_int(
            os.getenv(
                "JOB_SCRAPER_ANALYTICS_MAX_BATCH",
                config.get("analytics", {}).get("max_batch", 50),
            ),
            50,
        )

        config.setdefault("recommendation", {})
        config["recommendation"]["pool_size"] = _to_int(
            os.getenv(
                "JOB_SCRAPER_RECOMMENDED_POOL_SIZE",
                config.get("recommendation", {}).get("pool_size", 1500),
            ),
            1500,
        )

        config.setdefault("enrichment", {})
        config["enrichment"]["version"] = _to_int(
            os.getenv(
                "JOB_SCRAPER_ENRICHMENT_VERSION",
                config.get("enrichment", {}).get("version", 1),
            ),
            1,
        )
        config["enrichment"]["ai_fallback"] = _to_bool(
            os.getenv(
                "JOB_SCRAPER_ENRICHMENT_AI_FALLBACK",
                config.get("enrichment", {}).get("ai_fallback", False),
            )
        )
        config["enrichment"]["ai_min_confidence"] = _to_float(
            os.getenv(
                "JOB_SCRAPER_ENRICHMENT_AI_MIN_CONFIDENCE",
                config.get("enrichment", {}).get("ai_min_confidence", 0.45),
            ),
            0.45,
        )

        config.setdefault("llm_parser", {})
        config["llm_parser"]["enabled"] = _to_bool(
            os.getenv("LLM_PARSER_ENABLED", config.get("llm_parser", {}).get("enabled", True))
        )
        config["llm_parser"]["groq_api_key"] = os.getenv(
            "GROQ_API_KEY", config.get("llm_parser", {}).get("groq_api_key")
        )
        config["llm_parser"]["hf_api_key"] = os.getenv(
            "HF_API_KEY", config.get("llm_parser", {}).get("hf_api_key")
        )
        config["llm_parser"]["groq_model"] = os.getenv(
            "LLM_PARSER_GROQ_MODEL",
            config.get("llm_parser", {}).get("groq_model", "llama-3.1-8b-instant"),
        )
        config["llm_parser"]["hf_model"] = os.getenv(
            "LLM_PARSER_HF_MODEL",
            config.get("llm_parser", {}).get("hf_model", "Qwen/Qwen2.5-7B-Instruct"),
        )
        config["llm_parser"]["css_fallback"] = _to_bool(
            os.getenv("LLM_PARSER_CSS_FALLBACK", config.get("llm_parser", {}).get("css_fallback", True))
        )
        config["llm_parser"]["vision_model"] = os.getenv(
            "LLM_PARSER_VISION_MODEL",
            config.get("llm_parser", {}).get("vision_model", "meta-llama/llama-4-scout-17b-16e-instruct"),
        )
        config["llm_parser"]["vision_timeout"] = _to_float(
            os.getenv(
                "LLM_PARSER_VISION_TIMEOUT",
                config.get("llm_parser", {}).get("vision_timeout", 45),
            ),
            45.0,
        )

        config.setdefault("detail_enrichment", {})
        config["detail_enrichment"]["enabled"] = _to_bool(
            os.getenv("DETAIL_ENRICHMENT_ENABLED", config.get("detail_enrichment", {}).get("enabled", True))
        )
        config["detail_enrichment"]["max_per_site"] = _clamp_int(
            os.getenv("DETAIL_ENRICHMENT_MAX_PER_SITE", config.get("detail_enrichment", {}).get("max_per_site", 50)),
            default=50, minimum=1, name="DETAIL_ENRICHMENT_MAX_PER_SITE",
        )
        config["detail_enrichment"]["concurrency"] = _clamp_int(
            os.getenv("DETAIL_ENRICHMENT_CONCURRENCY", config.get("detail_enrichment", {}).get("concurrency", 5)),
            default=5, minimum=1, name="DETAIL_ENRICHMENT_CONCURRENCY",
        )
        config["detail_enrichment"]["fetch_timeout"] = _clamp_float(
            os.getenv("DETAIL_ENRICHMENT_FETCH_TIMEOUT", config.get("detail_enrichment", {}).get("fetch_timeout", 15.0)),
            default=15.0, minimum=1.0, name="DETAIL_ENRICHMENT_FETCH_TIMEOUT",
        )
        config["detail_enrichment"]["max_seconds"] = _clamp_float(
            os.getenv("DETAIL_ENRICHMENT_MAX_SECONDS", config.get("detail_enrichment", {}).get("max_seconds", 25.0)),
            default=25.0, minimum=5.0, name="DETAIL_ENRICHMENT_MAX_SECONDS",
        )
        config["detail_enrichment"]["max_fetches"] = _clamp_int(
            os.getenv("DETAIL_ENRICHMENT_MAX_FETCHES", config.get("detail_enrichment", {}).get("max_fetches", 30)),
            default=30, minimum=1, name="DETAIL_ENRICHMENT_MAX_FETCHES",
        )
        config["detail_enrichment"]["max_llm_calls"] = _clamp_int(
            os.getenv("DETAIL_ENRICHMENT_MAX_LLM_CALLS", config.get("detail_enrichment", {}).get("max_llm_calls", 10)),
            default=10, minimum=0, name="DETAIL_ENRICHMENT_MAX_LLM_CALLS",
        )

        config.setdefault("discovery", {})
        config["discovery"]["selector_min_confidence"] = _to_float(
            os.getenv(
                "JOB_SCRAPER_SELECTOR_MIN_CONFIDENCE",
                config.get("discovery", {}).get("selector_min_confidence", 0.6),
            ),
            0.6,
        )
        config["discovery"]["selector_min_jobs"] = _to_int(
            os.getenv(
                "JOB_SCRAPER_SELECTOR_MIN_JOBS",
                config.get("discovery", {}).get("selector_min_jobs", 3),
            ),
            3,
        )
        config["discovery"]["require_approved_selectors"] = _to_bool(
            os.getenv(
                "JOB_SCRAPER_REQUIRE_APPROVED_SELECTORS",
                config.get("discovery", {}).get("require_approved_selectors", True),
            )
        )
        config["discovery"]["hybrid_browser_fallback"] = _to_bool(
            os.getenv(
                "JOB_SCRAPER_HYBRID_BROWSER_FALLBACK",
                config.get("discovery", {}).get("hybrid_browser_fallback", True),
            )
        )

        return config

    def get(self, key: str, default=None):
        """Get config value"""
        return self._config.get(key, default)

    @property
    def adzuna(self) -> dict:
        return self._config.get("adzuna", {})

    @property
    def usajobs(self) -> dict:
        return self._config.get("usajobs", {})

    @property
    def themuse(self) -> dict:
        return self._config.get("themuse", {})

    @property
    def findwork(self) -> dict:
        return self._config.get("findwork", {})

    @property
    def careeronestop(self) -> dict:
        return self._config.get("careeronestop", {})

    @property
    def jsearch(self) -> dict:
        return self._config.get("jsearch", {})

    @property
    def greenhouse(self) -> dict:
        return self._config.get("greenhouse", {})

    @property
    def lever(self) -> dict:
        return self._config.get("lever", {})

    @property
    def smartrecruiters(self) -> dict:
        return self._config.get("smartrecruiters", {})

    @property
    def ashby(self) -> dict:
        return self._config.get("ashby", {})

    @property
    def remotive(self) -> dict:
        return self._config.get("remotive", {})

    @property
    def weworkremotely(self) -> dict:
        return self._config.get("weworkremotely", {})

    @property
    def builtin(self) -> dict:
        return self._config.get("builtin", {})

    @property
    def apify(self) -> dict:
        return self._config.get("apify", {})

    @property
    def hnrss(self) -> dict:
        return self._config.get("hnrss", {})

    @property
    def workday(self) -> dict:
        return self._config.get("workday", {})

    @property
    def db_dsn(self) -> Optional[str]:
        return self._config.get("db", {}).get("dsn")

    @property
    def retention_days(self) -> int:
        return int(self._config.get("db", {}).get("retention_days", 30))

    @property
    def schedule_hour(self) -> int:
        return int(self._config.get("scheduler", {}).get("hour", 23))

    @property
    def schedule_minute(self) -> int:
        return int(self._config.get("scheduler", {}).get("minute", 59))

    @property
    def us_only(self) -> bool:
        return _to_bool(self._config.get("us_only", True))

    @property
    def visa(self) -> dict:
        return self._config.get("visa", {})

    @property
    def analytics(self) -> dict:
        return self._config.get("analytics", {})

    @property
    def recommendation(self) -> dict:
        return self._config.get("recommendation", {})

    @property
    def enrichment(self) -> dict:
        return self._config.get("enrichment", {})

    @property
    def llm_parser(self) -> dict:
        return self._config.get("llm_parser", {})

    @property
    def detail_enrichment(self) -> dict:
        return self._config.get("detail_enrichment", {})

    @property
    def discovery(self) -> dict:
        return self._config.get("discovery", {})

    @property
    def known_tokens(self) -> dict:
        """Load known ATS tokens from YAML file with caching."""
        if hasattr(self, "_known_tokens_cache"):
            return self._known_tokens_cache

        # Get file path from env var or use default
        tokens_file = os.getenv(
            "JOB_SCRAPER_KNOWN_TOKENS_FILE",
            "data/known_tokens.yaml"
        )

        # Try to load the file
        try:
            tokens_path = Path(tokens_file)
            if tokens_path.exists():
                with open(tokens_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                # Extract overrides dict from YAML structure
                self._known_tokens_cache = data.get("overrides", {})
            else:
                logger.debug(f"Known tokens file not found: {tokens_file}")
                self._known_tokens_cache = {}
        except Exception as e:
            logger.warning(f"Failed to load known tokens from {tokens_file}: {e}")
            self._known_tokens_cache = {}

        return self._known_tokens_cache

    @property
    def uncapped_sources(self) -> list:
        """
        Load list of sources that should ignore max_per_source limit.
        These sources will fetch all available jobs (effectively unlimited).
        """
        # Try env var first
        env_val = os.getenv("JOB_SCRAPER_UNCAPPED_SOURCES")
        if env_val:
            return _to_list(env_val)

        # Fall back to config file
        ingestion_config = self._config.get("ingestion", {})
        return ingestion_config.get("uncapped_sources", [])

    @property
    def max_posting_age_days(self) -> int:
        """Maximum posting age to keep when posted_date is available (default: 60)."""
        ingestion_config = self._config.get("ingestion", {})
        return _to_int(ingestion_config.get("max_posting_age_days", 60), 60)
