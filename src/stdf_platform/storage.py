"""Parquet storage for STDF data."""

from pathlib import Path
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from .parser import STDFData
from .config import StorageConfig


# Default timestamp for invalid values (compatible with Parquet viewers)
_DEFAULT_DATETIME = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _unix_to_datetime(unix_ts: int) -> datetime:
    """Convert Unix timestamp to datetime. Returns default for invalid values."""
    if unix_ts is None or unix_ts <= 0:
        return _DEFAULT_DATETIME
    try:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    except (OSError, ValueError):
        return _DEFAULT_DATETIME


# PyArrow schemas for each table
LOTS_SCHEMA = pa.schema([
    ("lot_id", pa.string()),
    ("product", pa.string()),
    ("test_category", pa.string()),  # CP, FT, OTHER
    ("sub_process", pa.string()),    # CP11, FT2, etc.
    ("part_type", pa.string()),
    ("job_name", pa.string()),
    ("job_rev", pa.string()),
    ("start_time", pa.timestamp("ms", tz="UTC")),
    ("finish_time", pa.timestamp("ms", tz="UTC")),
    ("tester_type", pa.string()),
    ("operator", pa.string()),
])

WAFERS_SCHEMA = pa.schema([
    ("wafer_id", pa.string()),
    ("lot_id", pa.string()),
    ("head_num", pa.int64()),
    ("start_time", pa.timestamp("ms", tz="UTC")),
    ("finish_time", pa.timestamp("ms", tz="UTC")),
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

# Unified test data schema (merged tests + test_results)
TEST_DATA_SCHEMA = pa.schema([
    # Part identification
    ("lot_id", pa.string()),
    ("wafer_id", pa.string()),
    ("part_id", pa.string()),
    ("x_coord", pa.int64()),
    ("y_coord", pa.int64()),
    # Test identification
    ("test_num", pa.int64()),
    ("test_name", pa.string()),
    ("rec_type", pa.string()),  # PTR, MPR, FTR
    # Test parameters
    ("lo_limit", pa.float64()),
    ("hi_limit", pa.float64()),
    ("units", pa.string()),
    # Test result
    ("result", pa.float64()),
    ("passed", pa.bool_()),
])


def _get_test_category(test_type: str) -> str:
    """Extract test category from test type (CP1 -> CP, FT2 -> FT)."""
    test_type_upper = test_type.upper()
    if test_type_upper.startswith("CP"):
        return "CP"
    elif test_type_upper.startswith("FT"):
        return "FT"
    return "OTHER"


def extract_sub_process_from_filename(filename: str) -> str | None:
    """
    Extract sub-process (CP11, FT2, etc.) from filename.
    
    Looks for patterns like _CP11_, _FT2_, _CP1_, etc. in the filename.
    Returns None if no sub-process is found.
    
    Examples:
        "A_SPT_CP11_F5009AF0002_20250127.stdf.gz" -> "CP11"
        "LOT001_FT2_001.stdf" -> "FT2"
    """
    import re
    # Match _CP followed by digits or _FT followed by digits
    pattern = r'[_\-](CP\d+|FT\d+)[_\-\.]'
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


class ParquetStorage:
    """Parquet storage for STDF data with Hive-style partitioning."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.data_dir = config.data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _get_table_path(
        self, 
        table_name: str, 
        product: str = "", 
        test_category: str = "",
        sub_process: str = ""
    ) -> Path:
        """
        Get path for a table with product/test_category/sub_process partitioning.
        
        Hierarchy: product -> test_category (CP/FT) -> sub_process (CP11/FT2)
        """
        base = self.data_dir / table_name
        if product and test_category:
            path = base / f"product={product}" / f"test_category={test_category}"
            if sub_process:
                path = path / f"sub_process={sub_process}"
            return path
        return base

    def _write_parquet(self, table: pa.Table, path: Path, compression: str = "gzip"):
        """
        Write Parquet file with maximum compatibility settings.
        
        Uses Parquet 1.0 format and conservative options for JMP/Excel/viewer compatibility.
        """
        pq.write_table(
            table,
            path,
            compression=compression,
            version="1.0",  # Maximum compatibility
            use_dictionary=True,
            write_statistics=True,
            coerce_timestamps="ms",  # Millisecond precision
            allow_truncated_timestamps=True,
        )

    def save_stdf_data(
        self,
        data: STDFData,
        product: str = "UNKNOWN",
        test_category: str = "UNKNOWN",
        sub_process: str = "",
        compression: str = "snappy",
    ) -> dict[str, int]:
        """
        Save STDF data to Parquet files.

        Args:
            data: Parsed STDF data
            product: Product name (extracted from file path)
            test_category: Test category - CP or FT
            sub_process: Sub-process - CP11, FT2, etc. (extracted from filename)
            compression: Parquet compression method

        Returns:
            Dictionary with counts of records saved per table
        """
        counts = {}

        # Save lot info
        lot_path = self._get_table_path("lots", product, test_category, sub_process) / f"lot_id={data.lot_id}"
        lot_path.mkdir(parents=True, exist_ok=True)

        lot_table = pa.table({
            "lot_id": [data.lot_id],
            "product": [product],
            "test_category": [test_category],
            "sub_process": [sub_process or ""],
            "part_type": [data.part_type],
            "job_name": [data.job_name],
            "job_rev": [data.job_rev],
            "start_time": [_unix_to_datetime(data.start_time)],
            "finish_time": [_unix_to_datetime(data.finish_time)],
            "tester_type": [data.tester_type],
            "operator": [data.operator],
        }, schema=LOTS_SCHEMA)
        self._write_parquet(lot_table, lot_path / "data.parquet", compression)
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
                wafer_path = self._get_table_path("wafers", product, test_category, sub_process) / f"lot_id={data.lot_id}" / f"wafer_id={wafer_id}"
                wafer_path.mkdir(parents=True, exist_ok=True)

                wafer_table = pa.table({
                    "wafer_id": [w.get("wafer_id", "") for w in wafers],
                    "lot_id": [data.lot_id for _ in wafers],
                    "head_num": [w.get("head_num", 0) for w in wafers],
                    "start_time": [_unix_to_datetime(w.get("start_time", 0)) for w in wafers],
                    "finish_time": [_unix_to_datetime(w.get("finish_time", 0)) for w in wafers],
                    "part_count": [w.get("part_count", 0) for w in wafers],
                    "good_count": [w.get("good_count", 0) for w in wafers],
                    "rtst_count": [w.get("rtst_count", 0) for w in wafers],
                    "abrt_count": [w.get("abrt_count", 0) for w in wafers],
                }, schema=WAFERS_SCHEMA)
                self._write_parquet(wafer_table, wafer_path / "data.parquet", compression)

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
                part_path = self._get_table_path("parts", product, test_category, sub_process) / f"lot_id={lot_id}" / f"wafer_id={wafer_id}"
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
                self._write_parquet(part_table, part_path / "data.parquet", compression)

            counts["parts"] = len(data.parts)

        # Save unified test data (merged tests + test_results)
        if data.test_results:
            # Build part_id -> (x_coord, y_coord) mapping from parts
            part_coords = {}
            for part in data.parts:
                part_id = part.get("part_id", "")
                part_coords[part_id] = (
                    part.get("x_coord", -32768),
                    part.get("y_coord", -32768),
                )

            # Group by lot_id, wafer_id
            result_groups: dict[tuple, list] = {}
            for result in data.test_results:
                key = (result.get("lot_id", ""), result.get("wafer_id", ""))
                if key not in result_groups:
                    result_groups[key] = []
                result_groups[key].append(result)

            for (lot_id, wafer_id), results in result_groups.items():
                result_path = self._get_table_path("test_data", product, test_category, sub_process) / f"lot_id={lot_id}" / f"wafer_id={wafer_id}"
                result_path.mkdir(parents=True, exist_ok=True)

                # Enrich results with test info and coordinates
                enriched = []
                for r in results:
                    test_num = r.get("test_num", 0)
                    test_info = data.tests.get(test_num, {})
                    part_id = r.get("part_id", "")
                    x_coord, y_coord = part_coords.get(part_id, (-32768, -32768))
                    
                    enriched.append({
                        "lot_id": r.get("lot_id", ""),
                        "wafer_id": r.get("wafer_id", ""),
                        "part_id": part_id,
                        "x_coord": x_coord,
                        "y_coord": y_coord,
                        "test_num": test_num,
                        "test_name": test_info.get("test_name", ""),
                        "rec_type": test_info.get("rec_type", "PTR"),
                        "lo_limit": test_info.get("lo_limit"),
                        "hi_limit": test_info.get("hi_limit"),
                        "units": test_info.get("units", ""),
                        "result": r.get("result"),
                        "passed": r.get("passed", False),
                    })

                result_table = pa.table({
                    "lot_id": [r["lot_id"] for r in enriched],
                    "wafer_id": [r["wafer_id"] for r in enriched],
                    "part_id": [r["part_id"] for r in enriched],
                    "x_coord": [r["x_coord"] for r in enriched],
                    "y_coord": [r["y_coord"] for r in enriched],
                    "test_num": [r["test_num"] for r in enriched],
                    "test_name": [r["test_name"] for r in enriched],
                    "rec_type": [r["rec_type"] for r in enriched],
                    "lo_limit": [r["lo_limit"] for r in enriched],
                    "hi_limit": [r["hi_limit"] for r in enriched],
                    "units": [r["units"] for r in enriched],
                    "result": [r["result"] for r in enriched],
                    "passed": [r["passed"] for r in enriched],
                }, schema=TEST_DATA_SCHEMA)
                self._write_parquet(result_table, result_path / "data.parquet", compression)

            counts["test_data"] = len(data.test_results)

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
