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


_corrupt_gz_bytes = _bad_crc_gz_bytes


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


# ── download_file leaves no half-written artifacts ────────────────────────


def test_bad_crc_raises_typed_error(tmp_path):
    client = _client({"/r/bad.stdf.gz": _bad_crc_gz_bytes()})

    with pytest.raises(CorruptDownloadError) as excinfo:
        client.download_file("/r/bad.stdf.gz", tmp_path, decompress=True)

    assert excinfo.value.remote_path == "/r/bad.stdf.gz"
    assert "CRC" in str(excinfo.value)


def test_broken_deflate_stream_raises_typed_error(tmp_path):
    """zlib.error must be caught too - it is what the real bad file produced."""
    client = _client({"/r/bad.stdf.gz": _broken_stream_gz_bytes()})

    with pytest.raises(CorruptDownloadError) as excinfo:
        client.download_file("/r/bad.stdf.gz", tmp_path, decompress=True)

    assert excinfo.value.remote_path == "/r/bad.stdf.gz"
    assert not (tmp_path / "bad.stdf").exists()


def test_truncated_gz_raises_typed_error(tmp_path):
    """A short/interrupted file raises EOFError rather than BadGzipFile."""
    full = gzip.compress(os.urandom(50_000))  # incompressible, so it stays large
    client = _client({"/r/short.stdf.gz": full[: len(full) // 2]})

    with pytest.raises(CorruptDownloadError):
        client.download_file("/r/short.stdf.gz", tmp_path, decompress=True)

    assert not (tmp_path / "short.stdf").exists()


def test_corrupt_gz_leaves_no_partial_stdf(tmp_path):
    """The half-written .stdf is the dangerous leftover: `ingest-all` would
    silently ingest it as if it were a complete file. The .gz itself is kept
    and handed to the caller so it can be quarantined."""
    client = _client({"/r/bad.stdf.gz": _corrupt_gz_bytes()})

    with pytest.raises(CorruptDownloadError) as excinfo:
        client.download_file("/r/bad.stdf.gz", tmp_path, decompress=True)

    assert not (tmp_path / "bad.stdf").exists()
    assert excinfo.value.compressed_path == tmp_path / "bad.stdf.gz"
    assert excinfo.value.compressed_path.exists()


def test_healthy_gz_still_decompresses(tmp_path):
    payload = b"STDF" * 5000
    client = _client({"/r/ok.stdf.gz": gzip.compress(payload)})

    out = client.download_file("/r/ok.stdf.gz", tmp_path, decompress=True)

    assert out == tmp_path / "ok.stdf"
    assert out.read_bytes() == payload
    assert not (tmp_path / "ok.stdf.gz").exists()


# ── sync history records the quarantine ───────────────────────────────────


def test_mark_corrupt_is_persisted(tmp_path):
    history = tmp_path / "sync_history.json"
    sync = SyncManager(history)
    assert not sync.is_corrupt("/r/bad.stdf.gz")

    sync.mark_corrupt("/r/bad.stdf.gz", product="P", test_type="FT", error="CRC check failed")

    assert sync.is_corrupt("/r/bad.stdf.gz")
    assert SyncManager(history).is_corrupt("/r/bad.stdf.gz")


def test_corrupt_files_are_not_counted_as_downloaded(tmp_path):
    """A quarantined file must never look ingestable."""
    sync = SyncManager(tmp_path / "sync_history.json")
    sync.mark_corrupt("/r/bad.stdf.gz", product="P", test_type="FT", error="boom")

    assert not sync.is_downloaded("/r/bad.stdf.gz")
    assert sync.get_pending_ingest() == []


def test_get_corrupt_lists_entries(tmp_path):
    sync = SyncManager(tmp_path / "sync_history.json")
    sync.mark_corrupt("/r/bad.stdf.gz", product="P", test_type="FT", error="boom")

    entries = sync.get_corrupt()
    assert [e["remote_path"] for e in entries] == ["/r/bad.stdf.gz"]
    assert entries[0]["error"] == "boom"


def test_clear_corrupt_allows_a_retry(tmp_path):
    """The source will be re-exported to the same path, so the skip must be
    reversible - otherwise the fixed file is ignored forever."""
    sync = SyncManager(tmp_path / "sync_history.json")
    sync.mark_corrupt("/r/bad.stdf.gz", product="P", test_type="FT", error="boom")

    assert sync.clear_corrupt() == 1
    assert not sync.is_corrupt("/r/bad.stdf.gz")


# ── the fetch loop survives one bad file ──────────────────────────────────


def _config(tmp_path) -> Config:
    return Config(
        ftp=FTPConfig(),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            database=tmp_path / "data" / "db.duckdb",
            download_dir=tmp_path / "downloads",
        ),
    )


def test_download_loop_continues_past_corrupt_file(tmp_path):
    from stdf_platform.cli import _download_files

    config = _config(tmp_path)
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    sync = SyncManager(config.storage.data_dir / "sync_history.json")

    files = {
        "/r/bad.stdf.gz": _corrupt_gz_bytes(),
        "/r/good1.stdf.gz": gzip.compress(b"one" * 100),
        "/r/good2.stdf.gz": gzip.compress(b"two" * 100),
    }
    client = _client(files)
    candidates = [
        ("/r/bad.stdf.gz", "P", "FT", "bad.stdf.gz"),
        ("/r/good1.stdf.gz", "P", "FT", "good1.stdf.gz"),
        ("/r/good2.stdf.gz", "P", "FT", "good2.stdf.gz"),
    ]

    downloaded, corrupt, failed = _download_files(client, config, sync, candidates, verbose=False)

    # The two healthy files following the bad one must still arrive.
    assert [d[0] for d in downloaded] == ["/r/good1.stdf.gz", "/r/good2.stdf.gz"]
    assert [c[0] for c in corrupt] == ["/r/bad.stdf.gz"]
    assert failed == []
    assert sync.is_corrupt("/r/bad.stdf.gz")
    assert sync.is_downloaded("/r/good1.stdf.gz")


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


def test_corrupt_file_is_quarantined_not_left_in_downloads(tmp_path):
    from stdf_platform.cli import _download_files

    config = _config(tmp_path)
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    sync = SyncManager(config.storage.data_dir / "sync_history.json")
    client = _client({"/r/bad.stdf.gz": _corrupt_gz_bytes()})

    _download_files(
        client, config, sync,
        [("/r/bad.stdf.gz", "P", "FT", "bad.stdf.gz")],
        verbose=False,
    )

    landing = config.storage.download_dir / "P" / "FT"
    assert not (landing / "bad.stdf").exists()
    assert not (landing / "bad.stdf.gz").exists()
    quarantined = config.storage.download_dir / "_corrupt" / "bad.stdf.gz"
    assert quarantined.exists()


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
    assert (config.storage.download_dir / _corrupt_dir() / "bad.stdf.gz").exists()

    second = runner.invoke(cli.main, ["fetch", "--no-ingest"])
    assert second.exit_code == 0, second.output        # nothing bad happened
    assert fake.fetched == ["/r/bad.stdf.gz", "/r/good.stdf.gz"]  # no re-download


def _corrupt_dir() -> str:
    from stdf_platform.cli import _CORRUPT_DIR

    return _CORRUPT_DIR


def test_quarantined_file_is_skipped_on_the_next_run(tmp_path):
    """The whole point: the poison pill must not be re-downloaded every night."""
    from stdf_platform.cli import _download_files

    config = _config(tmp_path)
    config.storage.data_dir.mkdir(parents=True, exist_ok=True)
    sync = SyncManager(config.storage.data_dir / "sync_history.json")
    client = _client({"/r/bad.stdf.gz": _corrupt_gz_bytes()})
    candidates = [("/r/bad.stdf.gz", "P", "FT", "bad.stdf.gz")]

    _download_files(client, config, sync, candidates, verbose=False)
    first_round = list(client._ftp.fetched)

    remaining = [c for c in candidates if not sync.is_corrupt(c[0])]
    assert remaining == []

    _download_files(client, config, sync, remaining, verbose=False)
    assert client._ftp.fetched == first_round  # no second download
