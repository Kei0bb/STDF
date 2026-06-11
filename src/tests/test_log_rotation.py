"""Size-capped rotation of data/ingest_worker.log in worker._run_single."""

import subprocess

from stdf_platform import worker


class _FakeProc:
    def __init__(self, stderr):
        self.returncode = 0
        self._stderr = stderr

    def communicate(self, timeout=None):
        return '{"sub_process": "CP1", "test_category": "CP"}', self._stderr

    def kill(self):
        pass


def test_log_rotates_when_over_cap(tmp_path, monkeypatch):
    log_path = tmp_path / "ingest_worker.log"
    log_path.write_text("X" * (worker.LOG_MAX_BYTES + 10), encoding="utf-8")

    monkeypatch.setattr(
        worker.subprocess, "Popen",
        lambda cmd, **kw: _FakeProc(stderr="new error line\n"),
    )

    result = worker._run_single(
        local_path=tmp_path / "a.stdf", product="PROD", data_dir=tmp_path,
        compression="zstd", timeout=5, log_path=log_path,
    )
    assert result.success

    # Rotated: a .1 backup exists and the active log is back under the cap.
    assert (tmp_path / "ingest_worker.log.1").exists()
    assert log_path.stat().st_size <= worker.LOG_MAX_BYTES
