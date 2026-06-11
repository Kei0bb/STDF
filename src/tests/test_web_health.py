"""Per-request connection + /health tests for the web server."""

import importlib

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient


def _write_lots(data_dir):
    path = (
        data_dir / "lots" / "product=PROD" / "test_category=CP"
        / "sub_process=CP1" / "lot_id=LOT1" / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT1"], "product": ["PROD"], "test_category": ["CP"],
        "sub_process": ["CP1"], "part_type": ["X"], "job_name": ["J"], "job_rev": ["1"],
    }), path)


def _client(tmp_path, monkeypatch):
    _write_lots(tmp_path)
    monkeypatch.setenv("STDF_DATA_DIR", str(tmp_path))
    import stdf_platform.web.server as server
    importlib.reload(server)
    return TestClient(server.app)


def test_health_ok(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


def test_products_endpoint_per_request(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        resp = client.get("/api/products")
        assert resp.status_code == 200
        assert "PROD" in resp.json()
