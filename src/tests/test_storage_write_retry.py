"""_write_parquet's os.replace retry loop (concurrency-race fix).

Same-lot write races are prevented upstream by per-lot serialization in
worker.py (see test_worker_pool.py); this retry loop is a safety net for
transient locks held by EXTERNAL processes (antivirus scanners, a reader
briefly holding the destination open on Windows), which surface as
PermissionError from os.replace. Verifies the retry succeeds once the lock
clears, and that time.sleep is used for backoff (monkeypatched away here so
the test runs instantly).
"""

import pyarrow as pa
import pyarrow.parquet as pq

from stdf_platform import storage as storage_module
from stdf_platform.storage import ParquetStorage
from stdf_platform.config import StorageConfig


def _make_storage(tmp_path) -> ParquetStorage:
    cfg = StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb")
    return ParquetStorage(cfg)


def test_write_parquet_retries_permission_error_then_succeeds(tmp_path, monkeypatch):
    real_replace = storage_module.os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise PermissionError("simulated external file lock")
        return real_replace(src, dst)

    sleeps = []
    monkeypatch.setattr(storage_module.os, "replace", flaky_replace)
    monkeypatch.setattr(storage_module.time, "sleep", lambda s: sleeps.append(s))

    store = _make_storage(tmp_path)
    table = pa.table({"a": [1, 2, 3]})
    out_path = tmp_path / "out" / "data.parquet"

    store._write_parquet(table, out_path, compression="zstd")

    # Retried exactly twice (failed on attempts 1 and 2, succeeded on 3) with
    # the documented exponential backoff.
    assert calls["n"] == 3
    assert sleeps == [0.1, 0.2]

    # File landed and is readable with the expected content.
    assert out_path.exists()
    read_back = pq.read_table(out_path)
    assert read_back.column("a").to_pylist() == [1, 2, 3]

    # No leftover temp file.
    leftovers = [p.name for p in out_path.parent.iterdir() if p.name != "data.parquet"]
    assert leftovers == []


def test_write_parquet_reraises_and_cleans_tmp_after_exhausting_retries(tmp_path, monkeypatch):
    def always_fails(src, dst):
        raise PermissionError("persistent external lock")

    monkeypatch.setattr(storage_module.os, "replace", always_fails)
    monkeypatch.setattr(storage_module.time, "sleep", lambda s: None)

    store = _make_storage(tmp_path)
    table = pa.table({"a": [1]})
    out_path = tmp_path / "out2" / "data.parquet"

    try:
        store._write_parquet(table, out_path, compression="zstd")
        assert False, "expected PermissionError to propagate"
    except PermissionError:
        pass

    # Destination was never created, and the temp file was cleaned up.
    assert not out_path.exists()
    assert list(out_path.parent.iterdir()) == []
