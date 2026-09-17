"""_write_parquet's os.replace retry loop.

Same-lot write races are prevented upstream by per-lot serialization in
worker.py (see test_worker_pool.py); this retry loop is a safety net for
transient locks held by EXTERNAL processes (antivirus scanners, a reader
briefly holding the destination open on Windows), which surface as
PermissionError from os.replace. time.sleep is monkeypatched away so the test
runs instantly.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stdf_platform import storage as storage_module
from stdf_platform.config import StorageConfig
from stdf_platform.storage import ParquetStorage


@pytest.mark.parametrize("failures", [2, None], ids=["lock-clears", "lock-persists"])
def test_write_parquet_retries_permission_error(tmp_path, monkeypatch, failures):
    real_replace = storage_module.os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if failures is None or calls["n"] <= failures:
            raise PermissionError("simulated external file lock")
        return real_replace(src, dst)

    monkeypatch.setattr(storage_module.os, "replace", flaky_replace)
    monkeypatch.setattr(storage_module.time, "sleep", lambda s: None)

    store = ParquetStorage(StorageConfig(data_dir=tmp_path, database=tmp_path / "db.duckdb"))
    out_path = tmp_path / "out" / "data.parquet"

    if failures is None:
        # Retries exhausted: the error propagates, no destination, no temp file.
        with pytest.raises(PermissionError):
            store._write_parquet(pa.table({"a": [1]}), out_path, compression="zstd")
        assert not out_path.exists()
        assert list(out_path.parent.iterdir()) == []
    else:
        # Lock clears on the 3rd attempt: file lands, no temp file left behind.
        store._write_parquet(pa.table({"a": [1, 2, 3]}), out_path, compression="zstd")
        assert calls["n"] == failures + 1
        assert pq.read_table(out_path).column("a").to_pylist() == [1, 2, 3]
        assert [p.name for p in out_path.parent.iterdir()] == ["data.parquet"]
