"""Tests for the concurrent ingest worker pool (worker.run_ingest_pool)."""

import json
import subprocess
from pathlib import Path

from stdf_platform import worker


class _FakeProc:
    """Minimal stand-in for subprocess.Popen used by worker._run_single."""

    def __init__(self, *, returncode=0, stdout="", stderr="", timeout=False):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr
        self._timeout = timeout
        self.killed = False

    def communicate(self, timeout=None):
        if self._timeout and not self.killed:
            raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout)
        return self._stdout, self._stderr

    def kill(self):
        self.killed = True
        self._timeout = False  # second communicate() returns drained output


def test_run_ingest_pool_success(tmp_path, monkeypatch):
    payload = json.dumps({"sub_process": "CP1", "test_category": "CP"})

    def fake_popen(cmd, **kwargs):
        return _FakeProc(returncode=0, stdout=payload, stderr="")

    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

    files = [(None, tmp_path / "a.stdf", "PROD", "CP")]
    successes, failures = worker.run_ingest_pool(
        files=files, data_dir=tmp_path, compression="zstd", max_workers=1, timeout=5
    )

    assert len(successes) == 1
    assert not failures
    assert successes[0].sub_process == "CP1"
    assert successes[0].test_category == "CP"


def test_run_ingest_pool_timeout(tmp_path, monkeypatch):
    def fake_popen(cmd, **kwargs):
        return _FakeProc(timeout=True, stderr="Hanging forever...")

    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

    files = [(None, tmp_path / "slow.stdf", "PROD", "CP")]
    successes, failures = worker.run_ingest_pool(
        files=files, data_dir=tmp_path, compression="zstd", max_workers=1, timeout=2
    )

    assert not successes
    assert len(failures) == 1
    assert "timed out" in failures[0].error
