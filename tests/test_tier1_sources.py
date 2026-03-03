import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from job_scraper.apis.ashby import AshbyAPI
from job_scraper.apis.builtin import BuiltInAPI
from job_scraper.apis.weworkremotely import WeWorkRemotelyAPI


@pytest.mark.asyncio
async def test_ashby_tracking_returns_board_results():
    api = AshbyAPI(companies=["notion"])

    response = MagicMock()
    response.status_code = 200
    response.content = True
    response.headers = {}
    response.json.return_value = {
        "data": {
            "jobBoard": {
                "jobPostings": [
                    {
                        "id": "job-1",
                        "title": "Data Engineer",
                        "locationName": "New York, NY",
                        "workplaceType": "Remote",
                        "employmentType": "FullTime",
                        "secondaryLocations": [],
                        "compensationTierSummary": None,
                    },
                    {
                        "id": "job-2",
                        "title": "Backend Engineer",
                        "locationName": "San Francisco, CA",
                        "workplaceType": "Hybrid",
                        "employmentType": "FullTime",
                        "secondaryLocations": [],
                        "compensationTierSummary": None,
                    },
                ]
            }
        }
    }
    response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=response)
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    with patch("httpx.AsyncClient", return_value=mock_client):
        tracked_jobs, board_results = await api.search_jobs_with_tracking(max_results=50)

    assert len(tracked_jobs) == 2
    assert len(board_results) == 1
    assert board_results[0].source == "ashby"
    assert board_results[0].board_token == "notion"
    assert board_results[0].jobs_fetched == 2
    assert tracked_jobs[0].board_token == "notion"
    assert tracked_jobs[0].job.source == "Ashby"


@pytest.mark.asyncio
async def test_weworkremotely_rss_parsing():
    api = WeWorkRemotelyAPI()

    rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Acme: Senior Data Engineer</title>
      <region>United States</region>
      <country>USA</country>
      <skills>Python, SQL</skills>
      <category>Programming</category>
      <type>Full-Time</type>
      <description>Remote role</description>
      <pubDate>Fri, 13 Feb 2026 22:01:39 +0000</pubDate>
      <guid>https://weworkremotely.com/remote-jobs/acme-senior-data-engineer</guid>
      <link>https://weworkremotely.com/remote-jobs/acme-senior-data-engineer</link>
    </item>
  </channel>
</rss>"""

    response = MagicMock()
    response.status_code = 200
    response.content = True
    response.text = rss
    response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=response)
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    with patch("httpx.AsyncClient", return_value=mock_client):
        jobs = await api.search_jobs(max_results=10)

    assert len(jobs) == 1
    assert jobs[0].company == "Acme"
    assert jobs[0].title == "Senior Data Engineer"
    assert jobs[0].source == "WeWorkRemotely"
    assert jobs[0].posted_date == "Fri, 13 Feb 2026 22:01:39 +0000"


def test_builtin_extract_jobs_from_listing_page():
    api = BuiltInAPI(domains=["https://www.builtinnyc.com"], max_pages=1)
    html = """
<script type="application/ld+json">
{"@context":"https://schema.org","@graph":[{"@type":"ItemList","itemListElement":[{"@type":"ListItem","url":"https://www.builtinnyc.com/job/sample-role/123","description":"Design data pipelines"}]}]}
</script>
<div id="job-card-123" data-id="job-card">
  <div>
    <a data-id="company-title" data-builtin-track-job-id="123"><span>Acme Corp</span></a>
    <h2><a href="/job/sample-role/123" data-id="job-card-title">Senior Data Engineer</a></h2>
  </div>
</div>
<script>
window.bix.eventTracking.logBuiltinTrackEvent('job_board_view', {'jobs':[{'id':123,'published_date':'2026-02-10T06:15:19'}]});
</script>
"""

    jobs = api._extract_jobs(html, "https://www.builtinnyc.com", "New York, NY")
    assert len(jobs) == 1
    assert jobs[0].job_id == "123"
    assert jobs[0].company == "Acme Corp"
    assert jobs[0].title == "Senior Data Engineer"
    assert jobs[0].posted_date == "2026-02-10T06:15:19"
    assert jobs[0].url == "https://www.builtinnyc.com/job/sample-role/123"
    assert jobs[0].source == "BuiltIn"
