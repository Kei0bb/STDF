"""Windows/Japanese-locale robustness of the ingest worker pool.

Covers three bug fixes:
1. worker._run_single's Popen must survive non-UTF-8 (e.g. cp932) bytes on
   the child's stderr without crashing the reader thread, and must pin the
   child's stdio to UTF-8 via PYTHONIOENCODING.
2. worker.run_ingest_pool's on_success callback fires once per successful
   file (on the pool's consumer thread) so callers can persist progress
   incrementally instead of only after the whole pool finishes.
3. Files belonging to the same lot must be serialized (in measurement-time
   order) within one worker, because they share mutable on-disk state
   (lots table rewrite, retest_num directory scan, retest_flag demotion) —
   see worker.run_ingest_pool's docstring. Different lots must still run
   concurrently.

These use a real subprocess.Popen (not a fake stand-in for communicate())
by wrapping Popen to redirect the command to a tiny child script written to
tmp_path, forwarding all kwargs (encoding, errors, env) unchanged — this
exercises the real UTF-8 decoding path, which a mocked communicate() would
not.
"""

import subprocess
import sys
from pathlib import Path

from stdf_platform import worker


def test_cp932_stderr_does_not_crash_reader_thread(tmp_path, monkeypatch):
    """Raw cp932 bytes on stderr must not raise UnicodeDecodeError.

    Before the fix, encoding="utf-8" with strict error handling on the
    parent Popen would blow up the reader thread on Japanese Windows,
    surfacing only "exit code 1" and losing the real error message.
    """
    script = tmp_path / "cp932_child.py"
    script.write_text(
        "import os, sys\n"
        "os.write(2, b'\\x83G\\x83\\x89\\x81[')\n"  # cp932 for an error string; invalid UTF-8
        "sys.exit(1)\n",
        encoding="utf-8",
    )

    real_popen = subprocess.Popen  # captured before monkeypatching

    def fake_popen(cmd, **kwargs):
        new_cmd = [sys.executable, str(script)]
        return real_popen(new_cmd, **kwargs)

    monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)

    result = worker._run_single(
        local_path=tmp_path / "a.stdf",
        product="PROD",
        data_dir=tmp_path,
        compression="zstd",
        timeout=10,
        log_path=None,
    )

    # The reader thread survived: we get a clean failure result, not an
    # unhandled UnicodeDecodeError propagating out of _run_single.
    assert result.success is False
    assert isinstance(result.error, str)
    assert result.error != ""


def test_on_success_called_once_and_env_pins_utf8(tmp_path, monkeypatch):
    """on_success fires exactly once, for the one successful file, and the
    child env carries PYTHONIOENCODING=utf-8 for every spawned subprocess.
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
        "import sys\n"
        "sys.stderr.write('boom\\n')\n"
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
    assert len(calls) == 1
    assert calls[0].remote_path == "remote/ok.stdf"
    assert calls[0].success is True

    assert envs_seen
    for env in envs_seen:
        assert env is not None
        assert env.get("PYTHONIOENCODING") == "utf-8"


# --- _lot_key / _ts_key unit tests -----------------------------------------

def test_lot_key_splits_on_first_underscore():
    p = Path("2613-X03_00_SC0G29A_L000_@FT1_1#202604050254.std")
    assert worker._lot_key(p) == "2613-X03"


def test_lot_key_no_underscore_is_whole_filename():
    """No '_' at all: the file gets its own single-file group (the whole
    filename is the key), rather than colliding with anything else.
    """
    p = Path("nounderscore.std")
    assert worker._lot_key(p) == "nounderscore.std"


def test_ts_key_uses_text_after_last_hash():
    p = Path("2613-X03_00_SC0G29A_@FT1_1#202604050254.std")
    assert worker._ts_key(p) == "202604050254"


def test_ts_key_no_hash_falls_back_to_filename():
    p = Path("no_hash_here.std")
    assert worker._ts_key(p) == "no_hash_here.std"


def test_ts_key_multiple_hashes_uses_last_segment():
    p = Path("LOT_a#b#202604050308.std")
    assert worker._ts_key(p) == "202604050308"


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
        "lota_1": 0.3,
        "lota_2": 0.1,
        "lotb_1": 0.3,
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

    # (b) Cross-lot PARALLELISM: LOTB starts before LOTA's group is done,
    # i.e. it does not wait for LOTA#2 to finish. With max_workers=2 both
    # group futures (LOTA-group, LOTB-group) are submitted together, so
    # LOTB should start around the same time as LOTA#1, well before
    # LOTA#2 even starts.
    assert events["lotb_1"]["start"] < events["lota_2"]["start"]
