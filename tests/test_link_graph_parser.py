"""
Tests for link-graph parser URL filtering.
"""

from job_scraper.scraping.parsers.link_graph import _is_job_url, parse_link_graph


def test_is_job_url_rejects_off_domain_careers_content():
    base = "https://www.pfizer.com/about/careers"
    url = "https://money.usnews.com/careers/companies/rankings/best-companies"
    assert not _is_job_url(url, base)


def test_is_job_url_rejects_marketing_careers_pages():
    base = "https://www.apple.com/careers/us"
    assert not _is_job_url("https://www.apple.com/careers/us/work-at-apple.html", base)
    assert not _is_job_url("https://www.apple.com/careers/us/life-at-apple.html", base)
    assert not _is_job_url("https://www.apple.com/careers/us/accessibility.html", base)


def test_is_job_url_accepts_generic_job_path():
    base = "https://example.com/careers"
    assert _is_job_url("https://example.com/jobs/senior-software-engineer", base)


def test_is_job_url_accepts_careers_role_slug():
    base = "https://example.com/careers"
    assert _is_job_url("https://example.com/careers/senior-software-engineer", base)


def test_parse_link_graph_filters_garbage_links():
    html = """
    <html><body>
      <a href="/careers/us/work-at-apple.html">Work at Apple</a>
      <a href="/careers/us/life-at-apple.html">Life at Apple</a>
      <a href="https://money.usnews.com/careers/companies/rankings/best-companies">Best Companies</a>
      <a href="/jobs/senior-data-engineer">Senior Data Engineer</a>
    </body></html>
    """
    jobs = parse_link_graph(html, "https://www.apple.com/careers/us")
    assert len(jobs) == 1
    assert jobs[0].title == "Senior Data Engineer"
    assert jobs[0].url == "https://www.apple.com/jobs/senior-data-engineer"
