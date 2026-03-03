"""
Seed datasets for bootstrapping crawling and visa tagging.

These are intentionally small and human-maintainable. They are NOT exhaustive and
not guaranteed to reflect current sponsorship policy. Treat them as a starting point
to discover more ATS targets and to enrich/label jobs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


H1B_OPT_SPONSOR_SEED_COMPANIES: List[str] = [
    # Big tech / software
    "Amazon",
    "Google",
    "Microsoft",
    "Meta",
    "Apple",
    "Netflix",
    "Tesla",
    "NVIDIA",
    "Intel",
    "IBM",
    "Oracle",
    "Salesforce",
    "Adobe",
    "Cisco",
    "Qualcomm",
    "Samsung",
    "ByteDance",
    "TikTok",
    "Uber",
    "Lyft",
    "Airbnb",
    "Stripe",
    "Coinbase",
    "Block",
    "Square",
    "PayPal",
    "Databricks",
    "Snowflake",
    "Atlassian",
    "Shopify",
    "Dropbox",
    "Reddit",
    "Spotify",
    "Pinterest",
    "Snap",
    "Palantir",
    "ServiceNow",
    "Workday",
    "Intuit",
    "Zoom",
    "Twilio",
    "Okta",
    "Cloudflare",
    "HubSpot",
    "DocuSign",
    "LinkedIn",
    "OpenAI",
    "Anthropic",
    "Cohere",
    "Datadog",
    "Elastic",
    "Confluent",
    "MongoDB",
    "HashiCorp",
    "Splunk",
    "VMware",
    "Broadcom",
    "AMD",
    "ARM",
    "Micron",
    "Texas Instruments",
    "Applied Materials",
    "ASML",
    "Lam Research",
    "Cadence",
    "Synopsys",
    # Finance / trading
    "JPMorgan Chase",
    "Goldman Sachs",
    "Morgan Stanley",
    "Bank of America",
    "Citigroup",
    "Wells Fargo",
    "Capital One",
    "American Express",
    "Visa",
    "Mastercard",
    "Bloomberg",
    "BlackRock",
    "Fidelity Investments",
    "Charles Schwab",
    "Robinhood",
    "Two Sigma",
    "Citadel",
    "Jane Street",
    "DRW",
    "Hudson River Trading",
    "Susquehanna International Group",
    # Consulting / IT services (historically heavy H1B filers)
    "Accenture",
    "Deloitte",
    "PwC",
    "KPMG",
    "Ernst & Young",
    "Capgemini",
    "Cognizant",
    "Infosys",
    "Tata Consultancy Services",
    "Wipro",
    "HCLTech",
    "Tech Mahindra",
    "LTIMindtree",
    "Larsen & Toubro",
    "NTT DATA",
    "DXC Technology",
    "CGI",
    "Booz Allen Hamilton",
    # Healthcare / biotech
    "UnitedHealth Group",
    "CVS Health",
    "Johnson & Johnson",
    "Pfizer",
    "Merck",
    "AbbVie",
    "Novartis",
    "Roche",
    "Amgen",
    "Gilead Sciences",
    "Moderna",
    "Regeneron",
    "Medtronic",
    "Siemens Healthineers",
    "GE HealthCare",
    # Manufacturing / industrial
    "Boeing",
    "Lockheed Martin",
    "Northrop Grumman",
    "Raytheon",
    "General Motors",
    "Ford",
    "General Electric",
    "Siemens",
    "Honeywell",
    "3M",
    "Caterpillar",
    "Schneider Electric",
    "ABB",
    "Bosch",
    "Panasonic",
    # Retail / consumer
    "Walmart",
    "Target",
    "Home Depot",
    "Costco",
    "Nike",
    # Cloud / telecom
    "Verizon",
    "AT&T",
    "T-Mobile",
    "Comcast",
    "Charter Communications",
    # Automotive / mobility
    "Rivian",
    "Lucid Motors",
]


def write_company_list(path: Path, companies: Iterable[str]) -> None:
    items = []
    for c in companies:
        name = str(c).strip()
        if not name:
            continue
        items.append(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(items) + "\n", encoding="utf-8")

