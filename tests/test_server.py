"""Query-server endpoint + connection-lockdown tests over synthetic Parquet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi.testclient import TestClient

from stdf_platform.config import Config, ServerConfig, StorageConfig
from stdf_platform.server import create_app
from synth_data import _write_cp


def _client(tmp_path, **server_kw) -> TestClient:
    _write_cp(tmp_path)
    cfg = Config(
        storage=StorageConfig(data_dir=tmp_path),
        server=ServerConfig(**server_kw),
    )
    return TestClient(create_app(cfg))


def test_health(tmp_path):
    resp = _client(tmp_path).get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_views_lists_final_views(tmp_path):
    resp = _client(tmp_path).get("/api/views")
    views = resp.json()["views"]
    assert "parts_final" in views
    assert "wafer_yield_final" in views


def test_query_returns_rows(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query",
        json={"sql": "SELECT COUNT(*) AS n FROM parts_final WHERE lot_id = 'LOT1'"},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["columns"] == ["n"]
    assert payload["rows"] == [[4]]     # 4 distinct dies, retest-aware
    assert payload["truncated"] is False


def test_query_limit_truncates(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query", json={"sql": "SELECT * FROM parts", "limit": 2}
    )
    payload = resp.json()
    assert payload["row_count"] == 2
    assert payload["truncated"] is True


def test_server_max_rows_caps_even_without_limit(tmp_path):
    resp = _client(tmp_path, max_rows=2).post(
        "/api/query", json={"sql": "SELECT * FROM parts"}
    )
    payload = resp.json()
    assert payload["row_count"] == 2
    assert payload["truncated"] is True


def test_query_csv_format(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query",
        json={"sql": "SELECT lot_id FROM lots LIMIT 1", "format": "csv"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    lines = resp.text.strip().splitlines()
    assert lines[0] == "lot_id"
    assert lines[1] == "LOT1"


def test_non_select_rejected(tmp_path):
    client = _client(tmp_path)
    for sql in [
        "COPY (SELECT 1) TO 'out.csv'",
        "CREATE TABLE t (x INT)",
        "SET enable_external_access = true",
        "SELECT 1; SELECT 2",
    ]:
        resp = client.post("/api/query", json={"sql": sql})
        assert resp.status_code == 400, sql


def test_filesystem_locked_outside_data_dir(tmp_path):
    secret = tmp_path.parent / "server_secret.txt"
    secret.write_text("SECRET")
    resp = _client(tmp_path).post(
        "/api/query",
        json={"sql": f"SELECT * FROM read_text('{secret.as_posix()}')"},
    )
    assert resp.status_code == 400
    assert "SECRET" not in resp.text


def test_invalid_sql_returns_duckdb_message(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query", json={"sql": "SELECT * FROM no_such_view"}
    )
    assert resp.status_code == 400
    assert "no_such_view" in resp.json()["detail"]
