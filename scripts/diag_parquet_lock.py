"""Diagnose: does a long DuckDB Parquet scan block storage.py's retest demotion?

ingest 後に古い retest ファイルを降格するとき、storage.py は一時ファイルを
`os.replace` で対象に上書きする(約3秒だけリトライ)。Windows では、他プロセス
(共有 marimo のカーネル)が同じ Parquet を開いている間は `os.replace` が
PermissionError になり得る。失敗すると新旧両方が retest_flag=0 のまま残り、
test_data_final に重複が永続する(移行手段は無い)。

このスクリプトは、その状況を合成ファイルで再現して PASS/FAIL を出す。実データは使わず、
何も出力しない(件数と成否のみ)。共有 marimo を採用する前に Win11 で1回流す:

    uv run python scripts/diag_parquet_lock.py

  PASS = スキャン中でも置換できた → 現状の降格で問題なし
  FAIL = 置換できなかった → ingest 中は marimo を止める運用にする(docs/marimo-server.md)

Linux では PASS になる(開いたファイルを置換できる)。意味があるのは Windows での結果。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 2_000_000
HOLD_SECONDS = 8.0


def _write(path: Path, seed: int) -> None:
    pq.write_table(pa.table({"x": list(range(ROWS)), "seed": [seed] * ROWS}), path)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="stdf_lock_diag_") as d:
        target = Path(d) / "data.parquet"
        tmp = Path(d) / "data.parquet.tmp"
        _write(target, 1)
        _write(tmp, 2)

        started = threading.Event()
        stop = threading.Event()

        def scan() -> None:
            # 開いたまま長く読み続ける利用者のクエリを模す。
            con = duckdb.connect(":memory:")
            cur = con.execute(f"SELECT x, seed FROM read_parquet('{target.as_posix()}')")
            started.set()
            deadline = time.monotonic() + HOLD_SECONDS
            while not stop.is_set() and time.monotonic() < deadline:
                if not cur.fetchmany(10_000):
                    cur = con.execute(
                        f"SELECT x, seed FROM read_parquet('{target.as_posix()}')")
                time.sleep(0.05)
            con.close()

        t = threading.Thread(target=scan, daemon=True)
        t.start()
        started.wait(10)
        time.sleep(0.5)

        replaced = False
        error = ""
        for _attempt in range(4):  # storage.py と同程度(約3秒)のリトライ
            try:
                os.replace(tmp, target)
                replaced = True
                break
            except OSError as exc:
                error = f"{type(exc).__name__}: {exc.winerror if hasattr(exc, 'winerror') else exc.errno}"
                time.sleep(1.0)
        stop.set()
        t.join(timeout=HOLD_SECONDS + 5)

    print(f"platform      : {sys.platform}")
    print(f"os.replace    : {'ok' if replaced else 'BLOCKED (' + error + ')'}")
    print("RESULT        :", "PASS" if replaced else "FAIL")
    return 0 if replaced else 1


if __name__ == "__main__":
    raise SystemExit(main())
