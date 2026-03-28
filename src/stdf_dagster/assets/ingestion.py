"""Ingestion assets: FTP download and STDF parsing."""

import gzip
import tempfile
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Output,
    RetryPolicy,
    Backoff,
)

from stdf_dagster.resources.ftp import FTPResource
from stdf_dagster.resources.stdf_parser import STDFParserResource
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


@asset(
    description="STDFバイナリファイルを解析し、メタデータ付きの結果リストを返す",
    group_name="ingestion",
    kinds={"python", "rust"},
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=5,
    ),
)
def parsed_stdf_data(
    context: AssetExecutionContext,
    stdf_parser: STDFParserResource,
    raw_stdf_files: list[dict],
) -> list[dict]:
    """Parse downloaded STDF files into structured data.

    Each STDF file is parsed using the Rust parser (with Python fallback).
    Returns a list of dicts containing the parsed STDFData and metadata.
    """
    if not raw_stdf_files:
        context.log.info("No new files to parse")
        return []

    context.log.info(
        f"Parser: {'Rust' if stdf_parser.uses_rust else 'Python'}"
    )

    results = []
    errors = []

    for file_info in raw_stdf_files:
        file_path = Path(file_info["local_path"])
        filename = file_info["filename"]

        context.log.info(f"Parsing: {filename}")

        try:
            # Handle .gz files
            if file_path.suffix == ".gz":
                with tempfile.NamedTemporaryFile(suffix=".stdf", delete=False) as tmp:
                    with gzip.open(file_path, "rb") as gz:
                        tmp.write(gz.read())
                    tmp_path = Path(tmp.name)
                data = stdf_parser.parse(tmp_path)
                tmp_path.unlink(missing_ok=True)
            else:
                data = stdf_parser.parse(file_path)

            results.append({
                "data": data,
                "product": file_info["product"],
                "test_type": file_info["test_type"],
                "remote_path": file_info["remote_path"],
                "filename": filename,
                "lot_id": data.lot_id,
                "wafer_count": len(data.wafers),
                "part_count": len(data.parts),
                "test_count": len(data.tests),
            })

            context.log.info(
                f"  ✓ {filename}: lot={data.lot_id}, "
                f"wafers={len(data.wafers)}, parts={len(data.parts)}, "
                f"tests={len(data.tests)}"
            )

        except Exception as e:
            context.log.error(f"  ✗ Failed to parse {filename}: {e}")
            errors.append({"filename": filename, "error": str(e)})

    context.log.info(
        f"Parsed: {len(results)} success, {len(errors)} errors"
    )

    return results
