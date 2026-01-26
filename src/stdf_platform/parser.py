"""STDF file parser using pystdf."""

from pathlib import Path
from typing import Generator
from dataclasses import dataclass, field

from pystdf.IO import Parser
from pystdf import V4


# Mapping from pystdf record classes to record type names
RECORD_CLASS_MAP = {
    V4.far: "FAR",
    V4.atr: "ATR",
    V4.mir: "MIR",
    V4.mrr: "MRR",
    V4.pcr: "PCR",
    V4.hbr: "HBR",
    V4.sbr: "SBR",
    V4.pmr: "PMR",
    V4.pgr: "PGR",
    V4.plr: "PLR",
    V4.rdr: "RDR",
    V4.sdr: "SDR",
    V4.wir: "WIR",
    V4.wrr: "WRR",
    V4.wcr: "WCR",
    V4.pir: "PIR",
    V4.prr: "PRR",
    V4.tsr: "TSR",
    V4.ptr: "PTR",
    V4.mpr: "MPR",
    V4.ftr: "FTR",
    V4.bps: "BPS",
    V4.eps: "EPS",
    V4.gdr: "GDR",
    V4.dtr: "DTR",
}


@dataclass
class STDFData:
    """Parsed STDF data organized by record type."""

    # Metadata
    lot_id: str = ""
    part_type: str = ""
    job_name: str = ""
    job_rev: str = ""
    start_time: int = 0
    finish_time: int = 0
    tester_type: str = ""
    operator: str = ""

    # Records by type
    wafers: list[dict] = field(default_factory=list)
    parts: list[dict] = field(default_factory=list)
    tests: dict[int, dict] = field(default_factory=dict)  # test_num -> test info
    test_results: list[dict] = field(default_factory=list)
    bins_hard: dict[int, dict] = field(default_factory=dict)
    bins_soft: dict[int, dict] = field(default_factory=dict)
    sites: list[dict] = field(default_factory=list)

    # Internal state
    _current_wafer: str = ""
    _current_part_index: int = 0


class STDFParser:
    """Parser for STDF files."""

    def __init__(self):
        self.data = STDFData()
        self._part_counter = 0

    def _handle_record(self, record_class, field_values):
        """Handle a single record from pystdf."""
        if record_class not in RECORD_CLASS_MAP:
            return

        record_type = RECORD_CLASS_MAP[record_class]

        try:
            field_names = [f[0] for f in record_class.fieldMap]
            record = dict(zip(field_names, field_values))
        except Exception:
            return

        # Process based on record type
        if record_type == "MIR":
            self._handle_mir(record)
        elif record_type == "MRR":
            self._handle_mrr(record)
        elif record_type == "WIR":
            self._handle_wir(record)
        elif record_type == "WRR":
            self._handle_wrr(record)
        elif record_type == "PIR":
            self._handle_pir(record)
        elif record_type == "PRR":
            self._handle_prr(record)
        elif record_type == "PTR":
            self._handle_ptr(record)
        elif record_type == "FTR":
            self._handle_ftr(record)
        elif record_type == "MPR":
            self._handle_mpr(record)
        elif record_type == "HBR":
            self._handle_hbr(record)
        elif record_type == "SBR":
            self._handle_sbr(record)
        elif record_type == "SDR":
            self._handle_sdr(record)

    def _handle_mir(self, record: dict):
        """Handle Master Information Record."""
        self.data.lot_id = record.get("LOT_ID", "")
        self.data.part_type = record.get("PART_TYP", "")
        self.data.job_name = record.get("JOB_NAM", "")
        self.data.job_rev = record.get("JOB_REV", "")
        self.data.start_time = record.get("START_T", 0)
        self.data.tester_type = record.get("TSTR_TYP", "")
        self.data.operator = record.get("OPER_NAM", "")

    def _handle_mrr(self, record: dict):
        """Handle Master Results Record."""
        self.data.finish_time = record.get("FINISH_T", 0)

    def _handle_wir(self, record: dict):
        """Handle Wafer Information Record."""
        self.data._current_wafer = record.get("WAFER_ID", "")
        self.data.wafers.append({
            "wafer_id": self.data._current_wafer,
            "lot_id": self.data.lot_id,
            "head_num": record.get("HEAD_NUM", 0),
            "start_time": record.get("START_T", 0),
        })

    def _handle_wrr(self, record: dict):
        """Handle Wafer Results Record."""
        # Update the last wafer with results
        if self.data.wafers:
            self.data.wafers[-1].update({
                "finish_time": record.get("FINISH_T", 0),
                "part_count": record.get("PART_CNT", 0),
                "good_count": record.get("GOOD_CNT", 0),
                "rtst_count": record.get("RTST_CNT", 0),
                "abrt_count": record.get("ABRT_CNT", 0),
            })

    def _handle_pir(self, record: dict):
        """Handle Part Information Record."""
        self._part_counter += 1
        self.data._current_part_index = self._part_counter

    def _handle_prr(self, record: dict):
        """Handle Part Results Record."""
        part_flag = record.get("PART_FLG", 0)
        passed = (part_flag & 0x08) == 0  # Bit 3 = 0 means pass

        part = {
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "head_num": record.get("HEAD_NUM", 0),
            "site_num": record.get("SITE_NUM", 0),
            "x_coord": record.get("X_COORD", -32768),
            "y_coord": record.get("Y_COORD", -32768),
            "hard_bin": record.get("HARD_BIN", 0),
            "soft_bin": record.get("SOFT_BIN", 0),
            "passed": passed,
            "test_count": record.get("NUM_TEST", 0),
            "test_time": record.get("TEST_T", 0),
        }
        self.data.parts.append(part)

    def _handle_ptr(self, record: dict):
        """Handle Parametric Test Record."""
        test_num = record.get("TEST_NUM", 0)
        test_flag = record.get("TEST_FLG", 0)
        passed = (test_flag & 0x80) == 0  # Bit 7 = 0 means pass

        # Store test definition if not seen
        if test_num not in self.data.tests:
            self.data.tests[test_num] = {
                "test_num": test_num,
                "test_name": record.get("TEST_TXT", ""),
                "lo_limit": record.get("LO_LIMIT"),
                "hi_limit": record.get("HI_LIMIT"),
                "units": record.get("UNITS", ""),
                "test_type": "P",  # Parametric
            }

        # Store test result
        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "test_num": test_num,
            "head_num": record.get("HEAD_NUM", 0),
            "site_num": record.get("SITE_NUM", 0),
            "result": record.get("RESULT"),
            "passed": passed,
            "alarm_id": record.get("ALARM_ID", ""),
        })

    def _handle_ftr(self, record: dict):
        """Handle Functional Test Record."""
        test_num = record.get("TEST_NUM", 0)
        test_flag = record.get("TEST_FLG", 0)
        passed = (test_flag & 0x80) == 0

        if test_num not in self.data.tests:
            self.data.tests[test_num] = {
                "test_num": test_num,
                "test_name": record.get("TEST_TXT", ""),
                "test_type": "F",  # Functional
            }

        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "test_num": test_num,
            "head_num": record.get("HEAD_NUM", 0),
            "site_num": record.get("SITE_NUM", 0),
            "result": None,
            "passed": passed,
            "alarm_id": record.get("ALARM_ID", ""),
        })

    def _handle_mpr(self, record: dict):
        """Handle Multiple-Result Parametric Record."""
        test_num = record.get("TEST_NUM", 0)
        test_flag = record.get("TEST_FLG", 0)
        passed = (test_flag & 0x80) == 0

        if test_num not in self.data.tests:
            self.data.tests[test_num] = {
                "test_num": test_num,
                "test_name": record.get("TEST_TXT", ""),
                "lo_limit": record.get("LO_LIMIT"),
                "hi_limit": record.get("HI_LIMIT"),
                "units": record.get("UNITS", ""),
                "test_type": "M",  # Multiple
            }

        # MPR can have multiple results - store first one for simplicity
        results = record.get("RTN_RSLT", [])
        result = results[0] if results else None

        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "test_num": test_num,
            "head_num": record.get("HEAD_NUM", 0),
            "site_num": record.get("SITE_NUM", 0),
            "result": result,
            "passed": passed,
            "alarm_id": record.get("ALARM_ID", ""),
        })

    def _handle_hbr(self, record: dict):
        """Handle Hardware Bin Record."""
        bin_num = record.get("HBIN_NUM", 0)
        self.data.bins_hard[bin_num] = {
            "bin_num": bin_num,
            "bin_name": record.get("HBIN_NAM", ""),
            "bin_pf": record.get("HBIN_PF", ""),
            "bin_count": record.get("HBIN_CNT", 0),
        }

    def _handle_sbr(self, record: dict):
        """Handle Software Bin Record."""
        bin_num = record.get("SBIN_NUM", 0)
        self.data.bins_soft[bin_num] = {
            "bin_num": bin_num,
            "bin_name": record.get("SBIN_NAM", ""),
            "bin_pf": record.get("SBIN_PF", ""),
            "bin_count": record.get("SBIN_CNT", 0),
        }

    def _handle_sdr(self, record: dict):
        """Handle Site Description Record."""
        site_nums = record.get("SITE_NUM", [])
        if isinstance(site_nums, (list, tuple)):
            for site_num in site_nums:
                self.data.sites.append({
                    "site_num": site_num,
                    "head_num": record.get("HEAD_NUM", 0),
                })

    def parse(self, file_path: Path) -> STDFData:
        """
        Parse an STDF file.

        Args:
            file_path: Path to the STDF file

        Returns:
            Parsed STDF data
        """
        self.data = STDFData()
        self._part_counter = 0

        with open(file_path, "rb") as f:
            parser = Parser(inp=f)
            parser.addSink(self._handle_record)

            try:
                parser.parse()
            except Exception as e:
                raise RuntimeError(f"Failed to parse STDF file: {e}")

        return self.data


def parse_stdf(file_path: Path) -> STDFData:
    """
    Parse an STDF file.

    Args:
        file_path: Path to the STDF file

    Returns:
        Parsed STDF data
    """
    parser = STDFParser()
    return parser.parse(file_path)
