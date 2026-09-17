"""A corrupt .gz on the FTP server must not abort the whole fetch run.

Regression test for the 2026-08-07 outage: one file whose gzip stream was
broken at the source raised BadGzipFile inside the download loop, which had no
per-file error handling. `fetch` died on the first candidate and the remaining
135 files were never downloaded - every night, for ten days, because a failed
file is never recorded in sync_history and so is retried first each run.
"""

import gzip
import os

import pytest

from stdf_platform.config import Config, FTPConfig, StorageConfig
from stdf_platform.ftp_client import CorruptDownloadError, FTPClient
from stdf_platform.sync_manager import SyncManager


def _bad_crc_gz_bytes(payload: bytes = b"STDF" * 5000) -> bytes:
    """Decodes to the end, but the stored CRC does not match -> BadGzipFile."""
    raw = bytearray(gzip.compress(payload))
    raw[-8] ^= 0xFF  # corrupt the stored CRC32 in the trailer
    return bytes(raw)


def _broken_stream_gz_bytes(payload: bytes = b"STDF" * 5000) -> bytes:
    """The deflate stream itself is damaged -> zlib.error, as seen on the real
    2026-08-07 file where corruption faked an early end-of-stream."""
    raw = bytearray(gzip.compress(payload))
    raw[len(raw) // 2] ^= 0x01
    return bytes(raw)


def _truncated_gz_bytes() -> bytes:
    """A short/interrupted file raises EOFError rather than BadGzipFile."""
    full = gzip.compress(os.urandom(50_000))  # incompressible, so it stays large
    return full[: len(full) // 2]


class _FakeFTP:
    """Minimal stand-in for ftplib.FTP.retrbinary."""

    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.fetched: list[str] = []

    def retrbinary(self, cmd: str, callback) -> None:
        remote = cmd.split(" ", 1)[1]
        self.fetched.append(remote)
        callback(self.files[remote])


def _client(files: dict[str, bytes]) -> FTPClient:
    client = FTPClient(FTPConfig())
    client._ftp = _FakeFTP(files)
    return client


@pytest.mark.parametrize("make_bytes", [
    _bad_crc_gz_bytes, _broken_stream_gz_bytes, _truncated_gz_bytes,
], ids=["bad_crc", "broken_deflate", "truncated"])
def test_corrupt_gz_raises_typed_error_and_leaves_no_partial_stdf(tmp_path, make_bytes):
    """The half-written .stdf is the dangerous leftover: `ingest-all` would
    silently ingest it as if it were a complete file. The .gz itself is kept
    and handed to the caller so it can be quarantined."""
    client = _client({"/r/bad.stdf.gz": make_bytes()})

    with pytest.raises(CorruptDownloadError) as excinfo:
        client.download_file("/r/bad.stdf.gz", tmp_path, decompress=True)

    assert excinfo.value.remote_path == "/r/bad.stdf.gz"
    assert not (tmp_path / "bad.stdf").exists()
    assert excinfo.value.compressed_path == tmp_path / "bad.stdf.gz"
    assert excinfo.value.compressed_path.exists()


def _config(tmp_path) -> Config:
    return Config(
        ftp=FTPConfig(),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            database=tmp_path / "data" / "db.duckdb",
            download_dir=tmp_path / "downloads",
        ),
    )


def test_transient_error_is_not_quarantined(tmp_path):
    """A network drop is not the file's fault - it must stay retryable."""
    from stdf_platform.cli import _download_files

    config = _config(tmp_path)
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    sync = SyncManager(config.storage.data_dir / "sync_history.json")

    class _DroppingFTP(_FakeFTP):
        def retrbinary(self, cmd, callback):
            raise ConnectionResetError("connection reset by peer")

    client = FTPClient(FTPConfig())
    client._ftp = _DroppingFTP({})

    downloaded, corrupt, failed = _download_files(
        client, config, sync,
        [("/r/x.stdf.gz", "P", "FT", "x.stdf.gz")],
        verbose=False,
    )

    assert downloaded == [] and corrupt == []
    assert [f[0] for f in failed] == ["/r/x.stdf.gz"]
    assert not sync.is_corrupt("/r/x.stdf.gz")  # retried next run
    assert not sync.is_downloaded("/r/x.stdf.gz")


def test_fetch_command_survives_and_then_skips(tmp_path, monkeypatch):
    """End-to-end through the real `stdf fetch`: the bad file must not stop the
    run, must exit non-zero the first time, and must not be re-downloaded."""
    from click.testing import CliRunner

    from stdf_platform import cli

    config = _config(tmp_path)
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "/r/bad.stdf.gz": _bad_crc_gz_bytes(),
        "/r/good.stdf.gz": gzip.compress(b"fine" * 100),
    }
    listing = [
        ("/r/bad.stdf.gz", "P", "FT", "bad.stdf.gz"),
        ("/r/good.stdf.gz", "P", "FT", "good.stdf.gz"),
    ]
    fake = _FakeFTP(payloads)

    class _FakeClient(FTPClient):
        def __init__(self, cfg):
            super().__init__(cfg)

        def connect(self):
            self._ftp = fake

        def disconnect(self):
            self._ftp = None

        def list_stdf_files(self, **kwargs):
            return iter(listing)

    monkeypatch.setattr("stdf_platform.ftp_client.FTPClient", _FakeClient)
    monkeypatch.setattr(cli.Config, "load", classmethod(lambda cls, path=None: config))

    runner = CliRunner()
    first = runner.invoke(cli.main, ["fetch", "--no-ingest"])
    assert first.exit_code == 1, first.output          # bad file reported
    assert "bad.stdf.gz" in first.output
    assert fake.fetched == ["/r/bad.stdf.gz", "/r/good.stdf.gz"]  # good one still fetched
    assert (config.storage.download_dir / "P" / "FT" / "good.stdf").exists()
    assert (config.storage.download_dir / cli._CORRUPT_DIR / "bad.stdf.gz").exists()

    second = runner.invoke(cli.main, ["fetch", "--no-ingest"])
    assert second.exit_code == 0, second.output        # nothing bad happened
    assert fake.fetched == ["/r/bad.stdf.gz", "/r/good.stdf.gz"]  # no re-download


def test_partial_download_is_removed_on_transfer_failure(tmp_path):
    """RETR が途中で落ちた非 gz の .stdf を残さない。残留すると ingest-all が
    切り詰めファイルを黙って取り込む（gz は decompress 失敗で消えるが、
    非 gz はそのまま残っていた）。"""

    class _FailingFTP:
        def retrbinary(self, cmd, callback):
            callback(b"partial")
            raise OSError("connection reset")

    client = FTPClient(FTPConfig())
    client._ftp = _FailingFTP()

    with pytest.raises(OSError):
        client.download_file("/r/LOT.stdf", tmp_path, decompress=True)

    assert not (tmp_path / "LOT.stdf").exists()
