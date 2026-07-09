"""Windows/Japanese-locale robustness of the ingest worker pool.

Covers two bug fixes:
1. worker._run_single's Popen must survive non-UTF-8 (e.g. cp932) bytes on
   the child's stderr without crashing the reader thread, and must pin the
   child's stdio to UTF-8 via PYTHONIOENCODING.
2. worker.run_ingest_pool's on_success callback fires once per successful
   file (on the pool's consumer thread) so callers can persist progress
   incrementally instead of only after the whole pool finishes.

These use a real subprocess.Popen (not a fake stand-in for communicate())
by wrapping Popen to redirect the command to a tiny child script written to
tmp_path, forwarding all kwargs (encoding, errors, env) unchanged — this
exercises the real UTF-8 decoding path, which a mocked communicate() would
not.
"""

import subprocess
import sys

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
