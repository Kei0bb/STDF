"""Parquet storage for STDF data."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .parser import STDFData
from .config import StorageConfig


# PyArrow schemas for each table
LOTS_SCHEMA = pa.schema([
    ("lot_id", pa.string()),
    ("part_type", pa.string()),
    ("job_name", pa.string()),
    ("job_rev", pa.string()),
    ("start_time", pa.int64()),
    ("finish_time", pa.int64()),
    ("tester_type", pa.string()),
    ("operator", pa.string()),
])

WAFERS_SCHEMA = pa.schema([
    ("wafer_id", pa.string()),
    ("lot_id", pa.string()),
    ("head_num", pa.int64()),
    ("start_time", pa.int64()),
    ("finish_time", pa.int64()),
    ("part_count", pa.int64()),
    ("good_count", pa.int64()),
    ("rtst_count", pa.int64()),
    ("abrt_count", pa.int64()),
])

PARTS_SCHEMA = pa.schema([
    ("part_id", pa.string()),
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("head_num", pa.int64()),
    ("site_num", pa.int64()),
    ("x_coord", pa.int64()),
    ("y_coord", pa.int64()),
    ("hard_bin", pa.int64()),
    ("soft_bin", pa.int64()),
    ("passed", pa.bool_()),
    ("test_count", pa.int64()),
    ("test_time", pa.int64()),
])

TESTS_SCHEMA = pa.schema([
    ("test_num", pa.int64()),
    ("lot_id", pa.string()),
    ("test_name", pa.string()),
    ("lo_limit", pa.float64()),
    ("hi_limit", pa.float64()),
    ("units", pa.string()),
    ("test_type", pa.string()),
])

TEST_RESULTS_SCHEMA = pa.schema([
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("part_id", pa.string()),
    ("test_num", pa.int64()),
    ("head_num", pa.int64()),
    ("site_num", pa.int64()),
    ("result", pa.float64()),
    ("passed", pa.bool_()),
    ("alarm_id", pa.string()),
])


class ParquetStorage:
    """Parquet storage for STDF data with Hive-style partitioning."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.data_dir = config.data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _get_table_path(self, table_name: str) -> Path:
        """Get path for a table."""
        return self.data_dir / table_name

    def save_stdf_data(self, data: STDFData, compression: str = "snappy") -> dict[str, int]:
        """
        Save STDF data to Parquet files.

        Args:
            data: Parsed STDF data
            compression: Parquet compression method

        Returns:
            Dictionary with counts of records saved per table
        """
        counts = {}

        # Save lot info
        lot_path = self._get_table_path("lots") / f"lot_id={data.lot_id}"
        lot_path.mkdir(parents=True, exist_ok=True)

        lot_table = pa.table({
            "lot_id": [data.lot_id],
            "part_type": [data.part_type],
            "job_name": [data.job_name],
            "job_rev": [data.job_rev],
            "start_time": [data.start_time],
            "finish_time": [data.finish_time],
            "tester_type": [data.tester_type],
            "operator": [data.operator],
        }, schema=LOTS_SCHEMA)
        pq.write_table(lot_table, lot_path / "data.parquet", compression=compression)
        counts["lots"] = 1

        # Save wafers
        if data.wafers:
            wafer_groups: dict[str, list] = {}
            for wafer in data.wafers:
                wafer_id = wafer.get("wafer_id", "")
                if wafer_id not in wafer_groups:
                    wafer_groups[wafer_id] = []
                wafer_groups[wafer_id].append(wafer)

            for wafer_id, wafers in wafer_groups.items():
                wafer_path = self._get_table_path("wafers") / f"lot_id={data.lot_id}" / f"wafer_id={wafer_id}"
                wafer_path.mkdir(parents=True, exist_ok=True)

                wafer_table = pa.table({
                    "wafer_id": [w.get("wafer_id", "") for w in wafers],
                    "lot_id": [data.lot_id for _ in wafers],
                    "head_num": [w.get("head_num", 0) for w in wafers],
                    "start_time": [w.get("start_time", 0) for w in wafers],
                    "finish_time": [w.get("finish_time", 0) for w in wafers],
                    "part_count": [w.get("part_count", 0) for w in wafers],
                    "good_count": [w.get("good_count", 0) for w in wafers],
                    "rtst_count": [w.get("rtst_count", 0) for w in wafers],
                    "abrt_count": [w.get("abrt_count", 0) for w in wafers],
                }, schema=WAFERS_SCHEMA)
                pq.write_table(wafer_table, wafer_path / "data.parquet", compression=compression)

            counts["wafers"] = len(data.wafers)

        # Save parts (partitioned by lot_id, wafer_id)
        if data.parts:
            part_groups: dict[tuple, list] = {}
            for part in data.parts:
                key = (part.get("lot_id", ""), part.get("wafer_id", ""))
                if key not in part_groups:
                    part_groups[key] = []
                part_groups[key].append(part)

            for (lot_id, wafer_id), parts in part_groups.items():
                part_path = self._get_table_path("parts") / f"lot_id={lot_id}" / f"wafer_id={wafer_id}"
                part_path.mkdir(parents=True, exist_ok=True)

                part_table = pa.table({
                    "part_id": [p.get("part_id", "") for p in parts],
                    "lot_id": [p.get("lot_id", "") for p in parts],
                    "wafer_id": [p.get("wafer_id", "") for p in parts],
                    "head_num": [p.get("head_num", 0) for p in parts],
                    "site_num": [p.get("site_num", 0) for p in parts],
                    "x_coord": [p.get("x_coord", -32768) for p in parts],
                    "y_coord": [p.get("y_coord", -32768) for p in parts],
                    "hard_bin": [p.get("hard_bin", 0) for p in parts],
                    "soft_bin": [p.get("soft_bin", 0) for p in parts],
                    "passed": [p.get("passed", False) for p in parts],
                    "test_count": [p.get("test_count", 0) for p in parts],
                    "test_time": [p.get("test_time", 0) for p in parts],
                }, schema=PARTS_SCHEMA)
                pq.write_table(part_table, part_path / "data.parquet", compression=compression)

            counts["parts"] = len(data.parts)

        # Save tests
        if data.tests:
            tests_path = self._get_table_path("tests") / f"lot_id={data.lot_id}"
            tests_path.mkdir(parents=True, exist_ok=True)

            tests_list = list(data.tests.values())
            tests_table = pa.table({
                "test_num": [t.get("test_num", 0) for t in tests_list],
                "lot_id": [data.lot_id for _ in tests_list],
                "test_name": [t.get("test_name", "") for t in tests_list],
                "lo_limit": [t.get("lo_limit") for t in tests_list],
                "hi_limit": [t.get("hi_limit") for t in tests_list],
                "units": [t.get("units", "") for t in tests_list],
                "test_type": [t.get("test_type", "") for t in tests_list],
            }, schema=TESTS_SCHEMA)
            pq.write_table(tests_table, tests_path / "data.parquet", compression=compression)
            counts["tests"] = len(data.tests)

        # Save test results (partitioned by lot_id, wafer_id)
        if data.test_results:
            result_groups: dict[tuple, list] = {}
            for result in data.test_results:
                key = (result.get("lot_id", ""), result.get("wafer_id", ""))
                if key not in result_groups:
                    result_groups[key] = []
                result_groups[key].append(result)

            for (lot_id, wafer_id), results in result_groups.items():
                result_path = self._get_table_path("test_results") / f"lot_id={lot_id}" / f"wafer_id={wafer_id}"
                result_path.mkdir(parents=True, exist_ok=True)

                result_table = pa.table({
                    "lot_id": [r.get("lot_id", "") for r in results],
                    "wafer_id": [r.get("wafer_id", "") for r in results],
                    "part_id": [r.get("part_id", "") for r in results],
                    "test_num": [r.get("test_num", 0) for r in results],
                    "head_num": [r.get("head_num", 0) for r in results],
                    "site_num": [r.get("site_num", 0) for r in results],
                    "result": [r.get("result") for r in results],
                    "passed": [r.get("passed", False) for r in results],
                    "alarm_id": [r.get("alarm_id", "") for r in results],
                }, schema=TEST_RESULTS_SCHEMA)
                pq.write_table(result_table, result_path / "data.parquet", compression=compression)

            counts["test_results"] = len(data.test_results)

        return counts

    def get_lots(self) -> list[str]:
        """Get list of lot IDs in storage."""
        lots_path = self._get_table_path("lots")
        if not lots_path.exists():
            return []

        lots = []
        for p in lots_path.iterdir():
            if p.is_dir() and p.name.startswith("lot_id="):
                lots.append(p.name[7:])  # Remove "lot_id=" prefix
        return lots
