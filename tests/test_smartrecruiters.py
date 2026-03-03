from job_scraper.apis.smartrecruiters import SmartRecruitersAPI


def test_smartrecruiters_api_is_configured_with_companies():
    api = SmartRecruitersAPI(companies=["uber", "  ", None])  # type: ignore[arg-type]
    assert api.is_configured() is True
    assert api.companies == ["uber"]


def test_smartrecruiters_extract_description_combines_sections():
    api = SmartRecruitersAPI(companies=["x"], include_content=True)
    detail = {
        "jobAd": {
            "sections": {
                "jobDescription": {"title": "Job Description", "text": "<p>Hello</p>"},
                "qualifications": {"title": "Qualifications", "text": "<ul><li>World</li></ul>"},
                "additionalInformation": {"title": "Additional Information", "text": ""},
            }
        }
    }
    desc = api._extract_description(detail)
    assert desc is not None
    assert "Hello" in desc
    assert "World" in desc

