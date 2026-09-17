"""Query-server endpoint + connection-lockdown tests over synthetic Parquet."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb
import pytest
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


@pytest.mark.parametrize("path, content_type, marker", [
    ("/health", "application/json", '"ok"'),
    ("/api/views", "application/json", "wafer_yield_final"),
    ("/api", "text/plain", "POST /api/query"),
    ("/", "text/html", "SELECT"),
])
def test_get_endpoints(tmp_path, path, content_type, marker):
    resp = _client(tmp_path).get(path)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith(content_type)
    assert marker in resp.text


def test_schema_lists_views_and_columns(tmp_path):
    resp = _client(tmp_path).get("/api/schema")
    assert resp.status_code == 200
    tables = {t["name"]: t for t in resp.json()["tables"]}
    assert "parts" in tables
    cols = {c["name"] for c in tables["parts"]["columns"]}
    assert "lot_id" in cols


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


@pytest.mark.parametrize("server_kw, body_kw", [
    ({}, {"limit": 2}),                 # request limit
    ({"max_rows": 2}, {}),              # server cap applies even without limit
])
def test_row_cap_truncates(tmp_path, server_kw, body_kw):
    resp = _client(tmp_path, **server_kw).post(
        "/api/query", json={"sql": "SELECT * FROM parts", **body_kw}
    )
    payload = resp.json()
    assert payload["row_count"] == 2
    assert payload["truncated"] is True


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


def test_session_applies_server_limits(tmp_path):
    from stdf_platform.server.app import _open_locked_session

    _write_cp(tmp_path)
    cfg = Config(storage=StorageConfig(data_dir=tmp_path),
                 server=ServerConfig(memory_limit="1GB", threads=1))
    session = _open_locked_session(cfg)
    try:
        threads, memory = session.conn.execute(
            "SELECT current_setting('threads'), current_setting('memory_limit')"
        ).fetchone()
        assert threads == 1
        ref = duckdb.connect()
        ref.execute("SET memory_limit = '1GB'")
        assert memory == ref.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        ref.close()
    finally:
        session.close()


def test_query_timeout_returns_504(tmp_path):
    client = _client(tmp_path, query_timeout_seconds=1)
    resp = client.post("/api/query", json={
        "sql": "SELECT COUNT(*) FROM range(100000000) a, range(100000000) b",
    })
    assert resp.status_code == 504
    assert "timed out" in resp.json()["detail"]


@pytest.mark.parametrize("sql, expected", [
    ("SELECT [double 'nan', 1.0] AS v", [[[None, 1.0]]]),
    (r"SELECT {'b': '\xFF'::BLOB} AS v", [[{"b": "ff"}]]),
], ids=["nested_nan_is_null", "nested_blob_is_hex"])
def test_nested_values_are_json_safe(tmp_path, sql, expected):
    resp = _client(tmp_path).post("/api/query", json={"sql": sql})
    assert resp.status_code == 200
    assert resp.json()["rows"] == expected


def test_memory_limit_validation_accepts_yaml_number():
    """YAML が memory_limit: 2 を int にしても TypeError で 500 にしない。"""
    from stdf_platform.server.app import validate_memory_limit

    assert validate_memory_limit(2) == "2"
    assert validate_memory_limit("512MB") == "512MB"
    assert validate_memory_limit(" 80% ") == "80%"
    with pytest.raises(ValueError):
        validate_memory_limit("2 gigabytes")


def test_csv_export_neutralizes_formulas_without_mangling_negatives(tmp_path):
    """数式インジェクション対策で '-' 始まりの正当な値を壊さない。"""
    r = _client(tmp_path).post("/api/query", json={
        "sql": "SELECT '-3.5' AS neg, '=1+1' AS formula, '-TEST_A' AS name",
        "format": "csv",
    })
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    header, body = r.text.splitlines()[:2]
    assert header == "neg,formula,name"
    assert "-3.5" in body and "'-3.5" not in body
    assert "-TEST_A" in body and "'-TEST_A" not in body
    assert "'=1+1" in body
