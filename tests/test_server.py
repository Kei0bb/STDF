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


def test_index_serves_console(tmp_path):
    resp = _client(tmp_path).get("/")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")
    assert "stdf" in resp.text and "SELECT" in resp.text


def test_console_asset_ships_in_wheel():
    from importlib.resources import files

    assert (files("stdf_platform.server") / "console.html").is_file()


def test_console_sidebar_columns_start_collapsed(tmp_path):
    """Column lists are hidden until their disclosure toggle is clicked.

    With every view registered, always-open column lists pushed the later
    ones far below the fold. `.table-cols` is display:none until `.open` is added, and
    the toggle carries aria-expanded so it is operable without a mouse.
    """
    html = _client(tmp_path).get("/").text
    assert ".table-cols { display: none;" in html
    assert ".table-cols.open { display: block; }" in html
    assert 'disc.setAttribute("aria-expanded"' in html
    # The name still inserts SQL — that stayed the primary click target.
    assert '"SELECT * FROM " + t.name + " LIMIT 100"' in html


def test_api_index_serves_plaintext(tmp_path):
    resp = _client(tmp_path).get("/api")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "stdf query server" in resp.text
    assert "POST /api/query" in resp.text


def test_schema_survives_broken_view(tmp_path, monkeypatch):
    """A view registered at session-open time can still fail DESCRIBE later,
    since read_parquet globs resolve lazily on each query (not at CREATE VIEW
    time) — e.g. its backing file vanishes between registration and the
    schema() DESCRIBE loop. That must surface as one error entry, not a 500
    that blanks the whole sidebar.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    chipid_file = (
        tmp_path / "chipid" / "product=PROD" / "test_category=CP"
        / "lot_id=LOT1" / "data.parquet"
    )
    chipid_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["LOT1"], "part_id": ["A"], "part_txt": [""],
        "chip_occurrence_index": [0], "efuse_raw": ["0b" + "0" * 64],
        "valid": [True], "origin_fab_code": [1], "origin_fab": ["TSMC1"],
        "origin_lot": ["LOT1"], "origin_wafer": [1],
        "origin_x": [0], "origin_y": [0],
        "reserved_bits": ["00"], "retest_num": [0],
    }), chipid_file)

    import stdf_platform.server.app as app_module

    real_open_session = app_module._open_locked_session

    def broken_open_session(config):
        session = real_open_session(config)
        chipid_file.unlink()  # break the glob after "chipid" was registered
        return session

    monkeypatch.setattr(app_module, "_open_locked_session", broken_open_session)

    resp = _client(tmp_path).get("/api/schema")
    assert resp.status_code == 200
    tables = {t["name"]: t for t in resp.json()["tables"]}
    assert "parts" in tables and "error" not in tables["parts"]
    assert "chipid" in tables
    assert "error" in tables["chipid"]
    assert "columns" not in tables["chipid"]


def test_session_applies_server_limits(tmp_path):
    from stdf_platform.server.app import _open_locked_session

    _write_cp(tmp_path)
    cfg = Config(storage=StorageConfig(data_dir=tmp_path),
                 server=ServerConfig(memory_limit="1GB", threads=1))
    session = _open_locked_session(cfg)
    try:
        assert session.conn.execute(
            "SELECT current_setting('threads')"
        ).fetchone()[0] == 1
    finally:
        session.close()


def test_query_timeout_returns_504(tmp_path):
    client = _client(tmp_path, query_timeout_seconds=1)
    resp = client.post("/api/query", json={
        "sql": "SELECT COUNT(*) FROM range(100000000) a, range(100000000) b",
    })
    assert resp.status_code == 504
    assert "timed out" in resp.json()["detail"]


def test_nested_nan_is_serialized_as_null(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query", json={"sql": "SELECT [double 'nan', 1.0] AS v"}
    )
    assert resp.status_code == 200
    assert resp.json()["rows"] == [[[None, 1.0]]]


def test_nested_blob_is_hex_encoded(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query", json={"sql": r"SELECT {'b': '\xFF'::BLOB} AS v"}
    )
    assert resp.status_code == 200
    assert resp.json()["rows"] == [[{"b": "ff"}]]


def test_csv_formula_injection_is_neutralized(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query", json={"sql": "SELECT '=1+1' AS v", "format": "csv"}
    )
    assert resp.status_code == 200
    assert resp.text.strip().splitlines()[1] == "'=1+1"


def test_invalid_format_rejected(tmp_path):
    resp = _client(tmp_path).post(
        "/api/query", json={"sql": "SELECT 1", "format": "xml"}
    )
    assert resp.status_code == 400
