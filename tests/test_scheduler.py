from job_scraper.scheduler import _resolve_scheduled_sources


def test_resolve_scheduled_sources_include_only():
    sources = _resolve_scheduled_sources("greenhouse,lever,ashby", "")
    assert sources == ["greenhouse", "lever", "ashby"]


def test_resolve_scheduled_sources_include_minus_exclude():
    sources = _resolve_scheduled_sources("greenhouse,lever,ashby", "ashby")
    assert sources == ["greenhouse", "lever"]


def test_resolve_scheduled_sources_exclude_only():
    sources = _resolve_scheduled_sources(None, "ashby")
    assert sources is not None
    assert "ashby" not in [s.lower() for s in sources]
