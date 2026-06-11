import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent))
from stdf_platform.analysis import AnalysisSession, spatial  # noqa: E402


def _write_grid(data_dir: Path):
    xs, ys, passed, pid = [], [], [], []
    i = 0
    for x in range(5):
        for y in range(5):
            xs.append(x); ys.append(y)
            edge = x in (0, 4) or y in (0, 4)
            passed.append(not edge)        # outer ring fails
            pid.append(f"d{i}"); i += 1
    p = (data_dir / "parts" / "product=PROD" / "test_category=CP"
         / "sub_process=CP1" / "lot_id=G1" / "wafer_id=W1"
         / "retest=0" / "data.parquet")
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "lot_id": ["G1"]*25, "wafer_id": ["W1"]*25,
        "part_id": pid, "part_txt": [""]*25,
        "x_coord": xs, "y_coord": ys,
        "hard_bin": [1 if p_ else 4 for p_ in passed],
        "soft_bin": [1 if p_ else 4 for p_ in passed],
        "passed": passed, "retest_num": [0]*25,
    }), p)
    td = (data_dir / "test_data" / "product=PROD" / "test_category=CP"
          / "sub_process=CP1" / "lot_id=G1" / "data.parquet")
    td.parent.mkdir(parents=True, exist_ok=True)
    # result grows with radius → mean increases outward
    res = [float(abs(x-2)+abs(y-2)) for x in range(5) for y in range(5)]
    pq.write_table(pa.table({
        "lot_id": ["G1"]*25, "wafer_id": ["W1"]*25,
        "part_id": pid, "part_txt": [""]*25,
        "x_coord": xs, "y_coord": ys,
        "test_num": [7]*25, "pin_num": [0]*25,
        "test_name": ["R"]*25, "rec_type": ["PTR"]*25,
        "lo_limit": [0.0]*25, "hi_limit": [10.0]*25, "units": ["V"]*25,
        "result": res, "passed": ["P"]*25, "retest_num": [0]*25,
    }), td)


def _sess(tmp_path):
    _write_grid(tmp_path)
    return AnalysisSession(tmp_path)


def test_zone_yield_three_zones(tmp_path):
    with _sess(tmp_path) as s:
        df = spatial.zone_yield(s, "PROD", "G1", n_zones=3).set_index("zone")
        assert set(df.index) <= {"center", "mid", "edge"}
        # center fully passes; edge (outer ring) fails → lower yield
        assert float(df.loc["center", "yield_pct"]) == 100.0
        assert float(df.loc["edge", "yield_pct"]) < 100.0


def test_zone_yield_total_conserved(tmp_path):
    with _sess(tmp_path) as s:
        df = spatial.zone_yield(s, "PROD", "G1", n_zones=3)
        assert int(df["total"].sum()) == 25


def test_zone_yield_n_zones_param(tmp_path):
    with _sess(tmp_path) as s:
        df = spatial.zone_yield(s, "PROD", "G1", n_zones=5)
        assert df["zone_idx"].max() <= 4
        assert set(df["zone"]) <= {f"zone_{i}" for i in range(5)}


def test_radial_profile_increases(tmp_path):
    with _sess(tmp_path) as s:
        df = spatial.radial_profile(s, "PROD", "G1", 7).set_index("radius_bin")
        assert int(df["n"].sum()) == 25
        # mean of the inner-most bin < outer-most bin (result grows with radius)
        assert df["mean"].iloc[0] < df["mean"].iloc[-1]


def test_param_wafermap_fig_smoke(tmp_path):
    with _sess(tmp_path) as s:
        fig = spatial.param_wafermap_fig(s, "PROD", "G1", "W1", 7)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) == 1
        assert fig.layout.yaxis.autorange == "reversed"
