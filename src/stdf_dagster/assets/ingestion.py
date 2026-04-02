"""Ingestion assets: FTP download."""

from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    RetryPolicy,
    Backoff,
)

from stdf_dagster.resources.ftp import FTPResource
from stdf_dagster.resources.stdf_config import STDFConfigResource
from stdf_platform.sync_manager import SyncManager


@asset(
    description="FTPサーバーから新規STDFファイルをダウンロードし、ローカルパスのリストを返す",
    group_name="ingestion",
    kinds={"ftp"},
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
    ),
)
def raw_stdf_files(
    context: AssetExecutionContext,
    ftp: FTPResource,
    stdf_config: STDFConfigResource,
) -> list[dict]:
    """Download new STDF files from FTP server.

    Checks sync history to skip already-downloaded files.
    Returns a list of dicts containing local_path, product, and test_type.
    """
    config = stdf_config.load_config()
    sync = SyncManager(config.storage.data_dir / "sync_history.json")

    # Collect already-downloaded remote paths
    downloaded = set()
    history_file = config.storage.data_dir / "sync_history.json"
    if history_file.exists():
        import json
        with open(history_file, "r") as f:
            history = json.load(f)
        downloaded = set(history.get("files", {}).keys())

    context.log.info(f"Already downloaded: {len(downloaded)} files")

    # Apply product/test_type filters from config.yaml
    if config.filters:
        context.log.info(
            f"Product filters: {[f.product for f in config.filters]}"
        )

    # List new files on FTP (applies config.yaml filters)
    new_files = ftp.list_new_files(downloaded, config=config)
    context.log.info(f"New files found on FTP: {len(new_files)}")

    if not new_files:
        context.log.info("No new STDF files found on FTP")
        return []

    # Download each file
    results = []
    for file_info in new_files:
        remote_path = file_info["remote_path"]
        product = file_info["product"]
        test_type = file_info["test_type"]
        filename = file_info["filename"]

        local_dir = config.storage.download_dir / product / test_type
        context.log.info(f"Downloading: {filename} → {local_dir}")

        local_path = ftp.download(remote_path, local_dir)

        # Track in sync history
        sync.mark_downloaded(
            remote_path=remote_path,
            local_path=local_path,
            product=product,
            test_type=test_type,
        )

        results.append({
            "local_path": str(local_path),
            "product": product,
            "test_type": test_type,
            "remote_path": remote_path,
            "filename": filename,
        })

    context.log.info(f"Downloaded {len(results)} files")
    return results



