import json

import pytest

from stdf_platform.sync_manager import SyncManager


def test_ingest_batch_persists_history_when_pool_aborts(tmp_path, monkeypatch):
    """Ctrl+C / 例外でバッチが落ちても、ingest 済みの印は残さねばならない。

    残らないと次回 fetch が同じファイルを再 ingest し、retest_num が
    勝手に繰り上がって偽の retest={n} パーティションができる。
    """
    from stdf_platform import cli
    from stdf_platform.config import Config, StorageConfig
    from stdf_platform.worker import IngestResult

    sm = SyncManager(tmp_path / "sync_history.json")
    sm.mark_downloaded(remote_path="/r/a.stdf", local_path=tmp_path / "a.stdf",
                       product="P", test_type="CP")

    def fake_pool(*, on_success, **kw):
        on_success(IngestResult(local_path=tmp_path / "a.stdf", success=True,
                                remote_path="/r/a.stdf"))
        raise KeyboardInterrupt

    monkeypatch.setattr("stdf_platform.worker.run_ingest_pool", fake_pool)
    config = Config(storage=StorageConfig(data_dir=tmp_path))
    with pytest.raises(KeyboardInterrupt):
        cli._run_ingest_batch(config, sm, [("/r/a.stdf", tmp_path / "a.stdf", "P", "CP")],
                              cleanup=False, verbose=False)

    on_disk = json.loads((tmp_path / "sync_history.json").read_text())
    assert on_disk["files"]["/r/a.stdf"]["ingested"] is True
