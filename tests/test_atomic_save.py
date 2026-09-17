"""IngestHistory persistence (ingest-all resume) is atomic and round-trips."""

from stdf_platform.ingest_history import IngestHistory


def test_ingest_history_save_is_atomic_and_clean(tmp_path):
    hist = tmp_path / "ingest_history.json"
    h = IngestHistory(hist)
    f = tmp_path / "x.stdf"
    f.write_text("x")
    h.mark_done_batch([f])

    assert IngestHistory(hist).is_done(f)
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name not in {"ingest_history.json", "x.stdf"}]
    assert leftovers == []
