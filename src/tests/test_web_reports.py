"""GET /api/reports lists generated reports; /reports/* serves the HTML."""

import importlib


def _write_report(data_dir):
    p = (data_dir / "reports" / "product=PROD" / "test_category=CP"
         / "lot_id=LOT1" / "report.html")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("<html><body>RPT-OK</body></html>", encoding="utf-8")


def _client(tmp_path, monkeypatch):
    monkeypatch.setenv("STDF_DATA_DIR", str(tmp_path))
    import stdf_platform.web.server as server
    importlib.reload(server)
    from fastapi.testclient import TestClient
    return TestClient(server.app)


def test_reports_endpoint_empty(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        resp = client.get("/api/reports")
        assert resp.status_code == 200
        assert resp.json() == []


def test_reports_endpoint_lists_and_serves(tmp_path, monkeypatch):
    _write_report(tmp_path)
    with _client(tmp_path, monkeypatch) as client:
        listing = client.get("/api/reports").json()
        assert len(listing) == 1
        row = listing[0]
        assert row["product"] == "PROD"
        assert row["test_category"] == "CP"
        assert row["lot_id"] == "LOT1"
        assert row["url"] == "/reports/product=PROD/test_category=CP/lot_id=LOT1/report.html"
        served = client.get(row["url"])
        assert served.status_code == 200
        assert "RPT-OK" in served.text


def test_reports_dir_created_on_startup(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch):
        assert (tmp_path / "reports").exists()
