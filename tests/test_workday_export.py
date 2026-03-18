from __future__ import annotations

import yaml

from job_scraper.discovery.workday_export import (
    collect_workday_sites,
    export_workday_sites_to_yaml,
)


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, url: str = "", payload=None, content: bytes | None = None):
        self.status_code = status_code
        self.url = url
        self._payload = payload if payload is not None else {}
        if content is None:
            content = b"{}" if payload is not None else b""
        self.content = content
        self.cookies = type("Cookies", (), {"jar": []})()

    def json(self):
        return self._payload


def test_export_workday_sites_to_yaml_writes_empty_payload(tmp_path):
    output = tmp_path / "workday_sites.yaml"

    result = export_workday_sites_to_yaml([], output, validate=False)

    assert result["exported"] == 0
    assert output.exists()
    data = yaml.safe_load(output.read_text(encoding="utf-8"))
    assert data == {"workday": {"sites": []}}


def test_collect_workday_sites_redirect_fallback(monkeypatch):
    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def get(self, url, **kwargs):
            return _FakeResponse(
                status_code=200,
                url="https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite",
            )

        def post(self, url, **kwargs):
            return _FakeResponse(status_code=200, payload={"jobPostings": [], "total": 0})

    monkeypatch.setattr("job_scraper.discovery.workday_export.httpx.Client", lambda **kwargs: _FakeClient())

    sites, stats = collect_workday_sites(["https://www.nvidia.com/careers"], validate=False)
    assert len(sites) == 1
    assert sites[0]["host"] == "nvidia.wd5.myworkdayjobs.com"
    assert sites[0]["tenant"] == "nvidia"
    assert sites[0]["site"] == "NVIDIAExternalCareerSite"
    assert stats["resolved"] == 1


def test_collect_workday_sites_validation_rejects_422(monkeypatch):
    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def get(self, url, **kwargs):
            return _FakeResponse(status_code=200, url=url)

        def post(self, url, **kwargs):
            return _FakeResponse(status_code=422, content=b"")

    monkeypatch.setattr("job_scraper.discovery.workday_export.httpx.Client", lambda **kwargs: _FakeClient())

    sites, stats = collect_workday_sites(
        ["https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite"],
        validate=True,
    )
    assert sites == []
    assert stats["resolved"] == 1
    assert stats["rejected"] == 1
