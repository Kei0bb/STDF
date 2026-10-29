"""Diagnose: why does saving a notebook in the shared marimo fail with errno 13?

marimo's save does two things that can raise PermissionError on Windows:
  1. takes a lock file under <home>\\.marimo\\notebook-locks (creates the folder first)
  2. writes the notebook .py under workspace\\

This script repeats both with marimo's own functions and prints one line per step.
It writes nothing but empty temp files (removed again) and a lock file; it opens existing
notebooks in append mode without changing them. Run it as the same account that runs
start_marimo.bat:

    scripts\\diag_marimo_save.bat

  every step OK  -> the save problem is not these two; send the exact error text + operation
  FAIL line      -> that step is the cause (the path and error are on the line)
"""

from __future__ import annotations

import getpass
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT / "workspace"
failures = 0


def step(name: str, fn) -> None:
    global failures
    try:
        detail = fn()
    except Exception as exc:  # noqa: BLE001 - report every failure kind
        failures += 1
        print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
    else:
        print(f"ok    {name}" + (f"  [{detail}]" if detail else ""))


def info() -> None:
    print(f"platform    : {sys.platform}")
    print(f"user        : {getpass.getuser()}")
    print(f"python      : {sys.executable}")
    for key in ("USERPROFILE", "HOME", "HOMEDRIVE", "HOMEPATH", "XDG_STATE_HOME"):
        print(f"{key:<12}: {os.environ.get(key, '(not set)')}")
    try:
        print(f"Path.home() : {Path.home()}")
    except Exception as exc:  # noqa: BLE001
        print(f"Path.home() : error {exc!r}")
    print(f"workspace   : {WORKSPACE}")
    print()


def state_dir() -> Path:
    try:
        from marimo._utils.xdg import marimo_state_dir

        return marimo_state_dir()
    except Exception:  # noqa: BLE001 - marimo missing: fall back to the Windows rule
        return Path.home().resolve() / ".marimo"


def make_lock_dir() -> str:
    d = state_dir() / "notebook-locks"
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    return str(d)


def take_lock() -> str:
    from marimo._environments.script_metadata import notebook_file_lock

    probe = WORKSPACE / "_diag_probe.py"  # only its hash is used; the file is not created
    with notebook_file_lock(str(probe)):
        return "lock acquired and released"


def can_write_dir(d: Path):
    def run() -> str:
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".diag_", suffix=".tmp")
        os.close(fd)
        os.unlink(tmp)
        return "create + delete a temp file"

    return run


def can_append(p: Path):
    def run() -> str:
        with open(p, "ab"):
            pass
        read_only = not os.access(p, os.W_OK)
        if read_only:
            raise PermissionError("os.access says not writable (read-only attribute?)")
        return ""

    return run


def main() -> int:
    info()
    step("create <home>\\.marimo\\notebook-locks", make_lock_dir)
    step("take a notebook lock (marimo's own code)", take_lock)
    step("write into workspace\\", can_write_dir(WORKSPACE))
    for sub in sorted(p for p in WORKSPACE.iterdir() if p.is_dir() and not p.name.startswith((".", "__"))):
        step(f"write into workspace\\{sub.name}\\", can_write_dir(sub))
    for nb in sorted(WORKSPACE.rglob("*.py")):
        if any(part.startswith((".", "__")) for part in nb.relative_to(WORKSPACE).parts):
            continue
        step(f"open for writing: {nb.relative_to(WORKSPACE)}", can_append(nb))
    print()
    print("RESULT      :", "PASS" if not failures else f"FAIL ({failures} step(s))")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
