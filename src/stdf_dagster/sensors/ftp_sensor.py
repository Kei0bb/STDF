"""FTP sensor: detects new STDF files on the FTP server."""

import json
from pathlib import Path

from dagster import (
    sensor,
    RunRequest,
    RunConfig,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
)

from stdf_dagster.resources.ftp import FTPResource
from stdf_dagster.resources.stdf_config import STDFConfigResource


@sensor(
    description="FTPサーバーを定期的にポーリングし、新規STDFファイルを検知したらパイプラインを起動",
    minimum_interval_seconds=300,  # 5分間隔
    default_status=DefaultSensorStatus.STOPPED,  # 手動で有効化
)
def ftp_new_file_sensor(
    context: SensorEvaluationContext,
    ftp: FTPResource,
    stdf_config: STDFConfigResource,
):
    """Poll FTP server for new STDF files and trigger pipeline runs.

    Compares FTP file listing against sync_history.json to find
    files that haven't been downloaded yet.
    """
    config = stdf_config.load_config()

    # Load already-downloaded remote paths from sync history
    history_file = config.storage.data_dir / "sync_history.json"
    downloaded: set[str] = set()
    if history_file.exists():
        try:
            with open(history_file, "r") as f:
                history = json.load(f)
            downloaded = set(history.get("files", {}).keys())
        except (json.JSONDecodeError, IOError):
            pass

    # Check for new files
    try:
        new_files = ftp.list_new_files(downloaded)
    except Exception as e:
        context.log.error(f"FTP connection error: {e}")
        yield SkipReason(f"FTP connection error: {e}")
        return

    if not new_files:
        yield SkipReason("No new STDF files on FTP server")
        return

    context.log.info(f"Found {len(new_files)} new STDF files on FTP")

    # Use cursor to avoid duplicate runs (store last check timestamp)
    yield RunRequest(
        run_key=f"ftp-{len(new_files)}-{context.cursor or '0'}",
    )

    # Update cursor
    context.update_cursor(str(len(downloaded) + len(new_files)))
