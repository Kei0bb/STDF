"""FTP sensor: detects new STDF files on the FTP server."""

import json

from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
)

from stdf_dagster.resources.ftp import FTPResource
from stdf_dagster.resources.stdf_config import STDFConfigResource


@sensor(
    description="FTPサーバーを3時間間隔でポーリングし、新規STDFファイルを検知したらパイプラインを起動",
    minimum_interval_seconds=10800,  # 3時間間隔
    default_status=DefaultSensorStatus.STOPPED,  # 手動で有効化
)
def ftp_new_file_sensor(
    context: SensorEvaluationContext,
    ftp: FTPResource,
    stdf_config: STDFConfigResource,
):
    """Poll FTP server for new STDF files and trigger pipeline runs.

    - FTP接続情報: config.yaml の ftp セクション
    - 取得製品フィルタ: config.yaml の filters セクション
    - 差分検知: sync_history.json と比較
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

    # Check for new files (applies config.yaml filters)
    try:
        new_files = ftp.list_new_files(downloaded, config=config)
    except Exception as e:
        context.log.error(f"FTP connection error: {e}")
        yield SkipReason(f"FTP connection error: {e}")
        return

    if not new_files:
        yield SkipReason("No new STDF files on FTP server")
        return

    context.log.info(f"Found {len(new_files)} new STDF files on FTP")

    # Use cursor to avoid duplicate runs
    yield RunRequest(
        run_key=f"ftp-{len(new_files)}-{context.cursor or '0'}",
    )

    # Update cursor
    context.update_cursor(str(len(downloaded) + len(new_files)))
