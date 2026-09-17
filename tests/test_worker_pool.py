"""Ingest worker pool (worker.run_ingest_pool / worker._run_single).

1. The parent Popen must survive non-UTF-8 (e.g. cp932) bytes on the child's
   stderr, and must pin the child's stdio to UTF-8 via PYTHONIOENCODING.
2. on_success fires once per successful file so callers can persist progress
   incrementally.
3. Files belonging to the same lot are serialized (in measurement-time order)
   because they share mutable on-disk state (retest_num directory scan,
   retest_flag demotion); different lots still run concurrently.
4. A hung child is killed after `timeout` and reported as a failure.

Tests 1-3 use a real subprocess.Popen, redirected to a tiny child script in
tmp_path with all kwargs (encoding, errors, env) forwarded unchanged, so the
real decoding path is exercised.
"""

import subprocess
import sys

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


def test_on_success_called_once_and_env_pins_utf8(tmp_path, monkeypatch):
    """on_success fires exactly once, for the one successful file; the child
    env carries PYTHONIOENCODING=utf-8; and a child writing raw cp932 bytes to
    stderr yields a clean failure result instead of a UnicodeDecodeError.
    """
    success_script = tmp_path / "success_child.py"
    success_script.write_text(
        "import json, sys\n"
        "print(json.dumps({'sub_process': 'CP1', 'test_category': 'CP'}))\n"
        "sys.exit(0)\n",
        encoding="utf-8",
    )
    fail_script = tmp_path / "fail_child.py"
    fail_script.write_text(
        "import os, sys\n"
        "os.write(2, b'\\x83G\\x83\\x89\\x81[')\n"  # cp932 bytes; invalid UTF-8
        "sys.exit(1)\n",
        encoding="utf-8",
    )

    ok_path = tmp_path / "ok.stdf"
    bad_path = tmp_path / "bad.stdf"
    script_map = {str(ok_path): success_script, str(bad_path): fail_script}
    envs_seen = []

    real_popen = subprocess.Popen

    def fake_popen(cmd, **kwargs):
        local_path_str = cmd[3]  # cmd = [exe, "-m", "stdf_platform._ingest_worker", local_path, ...]
        script = script_map[local_path_str]
        envs_seen.append(kwargs.get("env"))
        new_cmd = [sys.executable, str(script)]
        return real_popen(new_cmd, **kwargs)

    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

    calls = []
    files = [
        ("remote/ok.stdf", ok_path, "PROD", "CP"),
        ("remote/bad.stdf", bad_path, "PROD", "CP"),
    ]

    successes, failures = worker.run_ingest_pool(
        files=files,
        data_dir=tmp_path,
        compression="zstd",
        max_workers=2,
        timeout=10,
        on_success=lambda r: calls.append(r),
    )

    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0].error, str) and failures[0].error
    assert len(calls) == 1
    assert calls[0].remote_path == "remote/ok.stdf"
    assert calls[0].success is True

    assert envs_seen
    for env in envs_seen:
        assert env is not None
        assert env.get("PYTHONIOENCODING") == "utf-8"


# --- Same-lot serialization -------------------------------------------------

_GROUP_CHILD_SCRIPT = """
import sys
import time

name = sys.argv[1]
log_path = sys.argv[2]
sleep_dur = float(sys.argv[3])

with open(log_path, "a", encoding="utf-8") as f:
    f.write(f"start {name} {time.time()!r}\\n")

time.sleep(sleep_dur)

with open(log_path, "a", encoding="utf-8") as f:
    f.write(f"end {name} {time.time()!r}\\n")
"""


def test_same_lot_files_serialize_in_timestamp_order_other_lots_parallel(tmp_path, monkeypatch):
    """LOTA's two files must never overlap in execution and must run in
    #timestamp order (the #1 file before the #2 file). LOTB (a different
    lot) must still be able to run concurrently with LOTA under
    max_workers=2 — proving grouping serializes WITHIN a lot but not
    ACROSS lots.
    """
    child_script = tmp_path / "group_child.py"
    child_script.write_text(_GROUP_CHILD_SCRIPT, encoding="utf-8")
    log_file = tmp_path / "events.log"

    # Filenames follow the real convention: lot prefix before first '_',
    # timestamp after last '#'.
    lota_1 = tmp_path / "LOTA_y_SC0G29A_@FT1_1#000000000001.std"  # earlier ts
    lota_2 = tmp_path / "LOTA_x_SC0G29A_@FT1_1#000000000002.std"  # later ts
    lotb_1 = tmp_path / "LOTB_z_SC0G29A_@FT1_1#000000000001.std"

    name_map = {
        str(lota_1): "lota_1",
        str(lota_2): "lota_2",
        str(lotb_1): "lotb_1",
    }
    sleep_map = {
        "lota_1": 1.0,   # cross-lot parallelism の断言に十分なマージン
        "lota_2": 0.1,
        "lotb_1": 0.2,
    }

    real_popen = subprocess.Popen

    def fake_popen(cmd, **kwargs):
        local_path_str = cmd[3]
        name = name_map[local_path_str]
        new_cmd = [
            sys.executable, str(child_script), name, str(log_file), str(sleep_map[name]),
        ]
        return real_popen(new_cmd, **kwargs)

    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

    files = [
        ("remote/lota_2.std", lota_2, "PROD", "FT"),
        ("remote/lota_1.std", lota_1, "PROD", "FT"),
        ("remote/lotb_1.std", lotb_1, "PROD", "FT"),
    ]

    successes, failures = worker.run_ingest_pool(
        files=files,
        data_dir=tmp_path,
        compression="zstd",
        max_workers=2,
        timeout=10,
    )

    assert len(failures) == 0
    assert len(successes) == 3

    events: dict[str, dict[str, float]] = {}
    for line in log_file.read_text(encoding="utf-8").splitlines():
        kind, name, ts = line.split(" ", 2)
        events.setdefault(name, {})[kind] = float(ts)

    for name in ("lota_1", "lota_2", "lotb_1"):
        assert "start" in events[name] and "end" in events[name]

    # (a) Within-lot ORDER: LOTA's #1 file starts before its #2 file starts,
    # and LOTA#1 fully finishes before LOTA#2 starts (no overlap at all).
    assert events["lota_1"]["start"] < events["lota_2"]["start"]
    assert events["lota_1"]["end"] <= events["lota_2"]["start"]

    # (b) Cross-lot PARALLELISM: LOTB starts while LOTA#1 is still running,
    # i.e. LOTB is not serialized behind the LOTA group. lota_1 sleeps 1.0s,
    # so this has a full second of margin and does not flake when the test
    # host is under load (the old assertion compared against lota_2's start,
    # which had only ~0.3s of margin).
    assert events["lotb_1"]["start"] < events["lota_1"]["end"]


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
