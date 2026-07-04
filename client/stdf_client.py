"""Thin client for the stdf query server — VSCode / Jupyter cell use.

Standalone file: copy it anywhere, `pip install requests pandas`, done.
No stdf_platform / DuckDB / data-share access required; the server owns the
data and the yield definitions.

    # %%
    from stdf_client import q
    df = q("SELECT * FROM wafer_yield_final WHERE lot_id = 'LOT1'")

Server URL resolution: `server=` argument → STDF_SERVER env var → localhost.
"""

import os

import pandas as pd
import requests

DEFAULT_SERVER = os.environ.get("STDF_SERVER", "http://localhost:8555")


def q(sql: str, limit: int | None = None, server: str | None = None) -> pd.DataFrame:
    """Run a SELECT on the stdf server and return a DataFrame.

    Results are capped at the server's max_rows; a warning is printed when
    the cap truncated the result (narrow the query or use to_csv).
    """
    resp = requests.post(
        f"{server or DEFAULT_SERVER}/api/query",
        json={"sql": sql, "limit": limit},
        timeout=600,
    )
    _raise_with_detail(resp)
    payload = resp.json()
    if payload["truncated"]:
        print(f"warning: result truncated at {payload['row_count']} rows")
    return pd.DataFrame(payload["rows"], columns=payload["columns"])


def to_csv(sql: str, path: str, limit: int | None = None,
           server: str | None = None) -> str:
    """Run a SELECT and save the result as CSV. Returns the path."""
    resp = requests.post(
        f"{server or DEFAULT_SERVER}/api/query",
        json={"sql": sql, "limit": limit, "format": "csv"},
        timeout=600,
    )
    _raise_with_detail(resp)
    with open(path, "wb") as f:
        f.write(resp.content)
    return path


def views(server: str | None = None) -> list[str]:
    """List the views available on the server (lots, parts_final, ...)."""
    resp = requests.get(f"{server or DEFAULT_SERVER}/api/views", timeout=60)
    _raise_with_detail(resp)
    return resp.json()["views"]


def _raise_with_detail(resp: requests.Response) -> None:
    """Surface the server's error detail (DuckDB message) instead of a bare 400."""
    if resp.ok:
        return
    try:
        detail = resp.json().get("detail", resp.text)
    except ValueError:
        detail = resp.text
    raise RuntimeError(f"stdf server error ({resp.status_code}): {detail}")
