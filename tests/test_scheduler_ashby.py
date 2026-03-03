import os
from unittest.mock import patch

from job_scraper.scheduler_ashby import _to_list, _run_ashby_batch


def test_to_list_parses_csv_values():
    assert _to_list("openai, notion , ,vanta") == ["openai", "notion", "vanta"]
    assert _to_list("") == []
    assert _to_list(None) == []


def test_run_ashby_batch_sets_and_restores_env():
    previous = os.environ.get("ASHBY_COMPANIES")
    os.environ["ASHBY_COMPANIES"] = "existing"

    with patch("job_scraper.scheduler_ashby.run_ingest", return_value="test-run-id") as mocked:
        _run_ashby_batch("A", ["openai", "vanta"], 999999)
        mocked.assert_called_once()
        kwargs = mocked.call_args.kwargs
        assert kwargs["sources"] == ["ashby"]
        assert kwargs["max_per_source"] == 999999

    assert os.environ.get("ASHBY_COMPANIES") == "existing"

    if previous is None:
        os.environ.pop("ASHBY_COMPANIES", None)
    else:
        os.environ["ASHBY_COMPANIES"] = previous
