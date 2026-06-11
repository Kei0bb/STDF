"""Atomic JSON file writes (temp file in same dir + os.replace)."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write_json(path: Path, obj: Any) -> None:
    """Write `obj` as JSON to `path` atomically.

    Writes to a temp file in the destination's directory, then os.replace —
    readers never observe a partial file. Cleans up the temp file on failure.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
