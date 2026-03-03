Multi-API Job Scraper

Aggregate US job listings from free/public APIs and ATS endpoints.

Sources
- RemoteOK (public)
- Adzuna (key required, US)
- USAJobs (key required, US federal)
- CareerOneStop (key required, US)
- JSearch (RapidAPI, US filter)
- Greenhouse (public ATS)
- Lever (public ATS)
- SmartRecruiters (public ATS)
- The Muse (public, US filtered)
- JobSpy (opt-in scraper)

Install
- pip install -r requirements.txt

Config
- Copy config.example.yaml to config.yaml, or set env vars.
- Required keys: ADZUNA_APP_ID/ADZUNA_APP_KEY, USAJOBS_API_KEY/USAJOBS_USER_AGENT,
  CAREERONESTOP_API_KEY/CAREERONESTOP_USER_ID, RAPIDAPI_KEY/RAPIDAPI_HOST.
- ATS boards: GREENHOUSE_BOARDS, LEVER_SITES (comma-separated) or config.yaml.

CLI
- python -m job_scraper.cli search "python developer" --sources remoteok,adzuna
- python -m job_scraper.cli init-db
- python -m job_scraper.cli ingest --sources usajobs,adzuna -m 100
- python -m job_scraper.cli serve

Notes
- US-only filtering is best-effort based on location text.
- JobSpy is opt-in and may be rate-limited.
- Visa/H1B/OPT tagging is best-effort (tags like `visa_friendly`) and is not a guarantee of sponsorship.
