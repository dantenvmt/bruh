from job_scraper.storage import get_session
from job_scraper.config import Config
from job_scraper.scraping.models import ScrapeSite
cfg = Config()
session = get_session(cfg.db_dsn)
sites = session.query(ScrapeSite).filter(ScrapeSite.enabled == True).all()
for s in sites:
    print(s.company_name, s.selector_hints)
