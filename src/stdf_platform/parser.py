"""STDF binary parser - no external dependencies."""

import struct
from pathlib import Path
from dataclasses import dataclass, field
from typing import BinaryIO


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
    test_code: str = ""  # CP1, FT2 等（MIR.TEST_CODから取得）

    # Records by type
    wafers: list[dict] = field(default_factory=list)
    parts: list[dict] = field(default_factory=list)
    tests: dict[int, dict] = field(default_factory=dict)
    test_results: list[dict] = field(default_factory=list)
    bins_hard: dict[int, dict] = field(default_factory=dict)
    bins_soft: dict[int, dict] = field(default_factory=dict)
    sites: list[dict] = field(default_factory=list)

    # Internal state
    _current_wafer: str = ""
    _current_part_index: int = 0


# STDF Record types (typ, sub)
REC_FAR = (0, 10)
REC_MIR = (1, 10)
REC_MRR = (1, 20)
REC_PCR = (1, 30)
REC_HBR = (1, 40)
REC_SBR = (1, 50)
REC_PMR = (1, 60)
REC_WIR = (2, 10)
REC_WRR = (2, 20)
REC_WCR = (2, 30)
REC_PIR = (5, 10)
REC_PRR = (5, 20)
REC_TSR = (10, 30)
REC_PTR = (15, 10)
REC_MPR = (15, 15)
REC_FTR = (15, 20)
REC_SDR = (1, 80)


class STDFParser:
    """Binary STDF V4 parser."""

    def __init__(self):
        self.data = STDFData()
        self._part_counter = 0
        self._endian = "<"  # Little endian by default

    def _read_u1(self, f: BinaryIO) -> int:
        """Read unsigned 1-byte integer."""
        data = f.read(1)
        if len(data) < 1:
            raise EOFError()
        return struct.unpack(self._endian + "B", data)[0]

    def _read_u2(self, f: BinaryIO) -> int:
        """Read unsigned 2-byte integer."""
        data = f.read(2)
        if len(data) < 2:
            raise EOFError()
        return struct.unpack(self._endian + "H", data)[0]

    def _read_u4(self, f: BinaryIO) -> int:
        """Read unsigned 4-byte integer."""
        data = f.read(4)
        if len(data) < 4:
            raise EOFError()
        return struct.unpack(self._endian + "I", data)[0]

    def _read_i1(self, f: BinaryIO) -> int:
        """Read signed 1-byte integer."""
        data = f.read(1)
        if len(data) < 1:
            raise EOFError()
        return struct.unpack(self._endian + "b", data)[0]

    def _read_i2(self, f: BinaryIO) -> int:
        """Read signed 2-byte integer."""
        data = f.read(2)
        if len(data) < 2:
            raise EOFError()
        return struct.unpack(self._endian + "h", data)[0]

    def _read_r4(self, f: BinaryIO) -> float:
        """Read 4-byte float."""
        data = f.read(4)
        if len(data) < 4:
            raise EOFError()
        return struct.unpack(self._endian + "f", data)[0]

    def _read_cn(self, f: BinaryIO) -> str:
        """Read character string (length-prefixed)."""
        length = self._read_u1(f)
        if length == 0:
            return ""
        data = f.read(length)
        try:
            return data.decode("ascii", errors="replace")
        except Exception:
            return ""

    def _read_header(self, f: BinaryIO) -> tuple[int, int, int]:
        """Read record header. Returns (rec_len, rec_typ, rec_sub)."""
        data = f.read(4)
        if len(data) < 4:
            raise EOFError()
        rec_len = struct.unpack(self._endian + "H", data[0:2])[0]
        rec_typ = data[2]
        rec_sub = data[3]
        return rec_len, rec_typ, rec_sub

    def _parse_far(self, f: BinaryIO, rec_len: int):
        """Parse File Attributes Record."""
        cpu_type = self._read_u1(f)
        stdf_ver = self._read_u1(f)
        # Set endianness based on CPU type
        if cpu_type == 1:
            self._endian = ">"  # Big endian (Sun)
        else:
            self._endian = "<"  # Little endian (x86)

    def _parse_mir(self, f: BinaryIO, rec_len: int):
        """Parse Master Information Record."""
        start_pos = f.tell()
        
        setup_t = self._read_u4(f)
        start_t = self._read_u4(f)
        stat_num = self._read_u1(f)
        mode_cod = chr(self._read_u1(f)) if f.tell() - start_pos < rec_len else ""
        rtst_cod = chr(self._read_u1(f)) if f.tell() - start_pos < rec_len else ""
        prot_cod = chr(self._read_u1(f)) if f.tell() - start_pos < rec_len else ""
        burn_tim = self._read_u2(f) if f.tell() - start_pos < rec_len else 0
        cmod_cod = chr(self._read_u1(f)) if f.tell() - start_pos < rec_len else ""
        lot_id = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        part_typ = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        node_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        tstr_typ = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        job_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        job_rev = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        
        # Additional optional fields
        sblot_id = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        oper_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        exec_typ = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        exec_ver = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        test_cod = self._read_cn(f) if f.tell() - start_pos < rec_len else ""  # CP1, FT2等

        self.data.lot_id = lot_id
        self.data.part_type = part_typ
        self.data.job_name = job_nam
        self.data.job_rev = job_rev
        self.data.start_time = start_t
        self.data.tester_type = tstr_typ
        self.data.operator = oper_nam
        self.data.test_code = test_cod

        # Skip remaining optional fields
        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_mrr(self, f: BinaryIO, rec_len: int):
        """Parse Master Results Record."""
        start_pos = f.tell()
        finish_t = self._read_u4(f)
        self.data.finish_time = finish_t
        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_wir(self, f: BinaryIO, rec_len: int):
        """Parse Wafer Information Record."""
        start_pos = f.tell()
        head_num = self._read_u1(f)
        site_grp = self._read_u1(f) if f.tell() - start_pos < rec_len else 0
        start_t = self._read_u4(f) if f.tell() - start_pos < rec_len else 0
        wafer_id = self._read_cn(f) if f.tell() - start_pos < rec_len else ""

        self.data._current_wafer = wafer_id
        self.data.wafers.append({
            "wafer_id": wafer_id,
            "lot_id": self.data.lot_id,
            "head_num": head_num,
            "start_time": start_t,
        })

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_wrr(self, f: BinaryIO, rec_len: int):
        """Parse Wafer Results Record."""
        start_pos = f.tell()
        head_num = self._read_u1(f)
        site_grp = self._read_u1(f) if f.tell() - start_pos < rec_len else 0
        finish_t = self._read_u4(f) if f.tell() - start_pos < rec_len else 0
        part_cnt = self._read_u4(f) if f.tell() - start_pos < rec_len else 0
        rtst_cnt = self._read_u4(f) if f.tell() - start_pos < rec_len else 0
        abrt_cnt = self._read_u4(f) if f.tell() - start_pos < rec_len else 0
        good_cnt = self._read_u4(f) if f.tell() - start_pos < rec_len else 0
        func_cnt = self._read_u4(f) if f.tell() - start_pos < rec_len else 0

        if self.data.wafers:
            self.data.wafers[-1].update({
                "finish_time": finish_t,
                "part_count": part_cnt,
                "good_count": good_cnt,
                "rtst_count": rtst_cnt,
                "abrt_count": abrt_cnt,
            })

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_pir(self, f: BinaryIO, rec_len: int):
        """Parse Part Information Record."""
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        self._part_counter += 1
        self.data._current_part_index = self._part_counter

    def _parse_prr(self, f: BinaryIO, rec_len: int):
        """Parse Part Results Record."""
        start_pos = f.tell()
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        part_flg = self._read_u1(f)
        num_test = self._read_u2(f)
        hard_bin = self._read_u2(f)
        soft_bin = self._read_u2(f) if f.tell() - start_pos < rec_len else 0
        x_coord = self._read_i2(f) if f.tell() - start_pos < rec_len else -32768
        y_coord = self._read_i2(f) if f.tell() - start_pos < rec_len else -32768
        test_t = self._read_u4(f) if f.tell() - start_pos < rec_len else 0

        passed = (part_flg & 0x08) == 0

        part = {
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "head_num": head_num,
            "site_num": site_num,
            "x_coord": x_coord,
            "y_coord": y_coord,
            "hard_bin": hard_bin,
            "soft_bin": soft_bin,
            "passed": passed,
            "test_count": num_test,
            "test_time": test_t,
        }
        self.data.parts.append(part)

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_ptr(self, f: BinaryIO, rec_len: int):
        """Parse Parametric Test Record."""
        start_pos = f.tell()
        test_num = self._read_u4(f)
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        test_flg = self._read_u1(f)
        parm_flg = self._read_u1(f)
        result = self._read_r4(f) if f.tell() - start_pos < rec_len else None
        test_txt = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        alarm_id = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        
        # Read optional fields
        opt_flag = self._read_u1(f) if f.tell() - start_pos < rec_len else 0xFF
        res_scal = self._read_i1(f) if f.tell() - start_pos < rec_len else 0
        llm_scal = self._read_i1(f) if f.tell() - start_pos < rec_len else 0
        hlm_scal = self._read_i1(f) if f.tell() - start_pos < rec_len else 0
        lo_limit = self._read_r4(f) if f.tell() - start_pos < rec_len else None
        hi_limit = self._read_r4(f) if f.tell() - start_pos < rec_len else None
        units = self._read_cn(f) if f.tell() - start_pos < rec_len else ""

        passed = (test_flg & 0x80) == 0

        if test_num not in self.data.tests:
            self.data.tests[test_num] = {
                "test_num": test_num,
                "test_name": test_txt,
                "lo_limit": lo_limit,
                "hi_limit": hi_limit,
                "units": units,
                "test_type": "P",
                "rec_type": "PTR",
            }

        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": result,
            "passed": passed,
            "alarm_id": alarm_id,
        })

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_ftr(self, f: BinaryIO, rec_len: int):
        """Parse Functional Test Record."""
        start_pos = f.tell()
        test_num = self._read_u4(f)
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        test_flg = self._read_u1(f)

        passed = (test_flg & 0x80) == 0

        if test_num not in self.data.tests:
            self.data.tests[test_num] = {
                "test_num": test_num,
                "test_name": "",
                "test_type": "F",
                "rec_type": "FTR",
            }

        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": None,
            "passed": passed,
            "alarm_id": "",
        })

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_mpr(self, f: BinaryIO, rec_len: int):
        """Parse Multiple-Result Parametric Record (STDF V4)."""
        start_pos = f.tell()
        
        # Required fields
        test_num = self._read_u4(f)
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        test_flg = self._read_u1(f)
        parm_flg = self._read_u1(f)
        rtn_icnt = self._read_u2(f) if f.tell() - start_pos < rec_len else 0
        rslt_cnt = self._read_u2(f) if f.tell() - start_pos < rec_len else 0
        
        # RTN_STAT: Array of return states (nibbles)
        rtn_stat = []
        if rtn_icnt > 0 and f.tell() - start_pos < rec_len:
            num_bytes = (rtn_icnt + 1) // 2
            for _ in range(num_bytes):
                if f.tell() - start_pos >= rec_len:
                    break
                byte = self._read_u1(f)
                rtn_stat.append(byte & 0x0F)
                if len(rtn_stat) < rtn_icnt:
                    rtn_stat.append((byte >> 4) & 0x0F)
        
        # RTN_RSLT: Array of results (R*4)
        results = []
        for _ in range(rslt_cnt):
            if f.tell() - start_pos >= rec_len:
                break
            results.append(self._read_r4(f))
        
        # Optional fields (order per STDF V4 spec)
        test_txt = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        alarm_id = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        opt_flag = self._read_u1(f) if f.tell() - start_pos < rec_len else 0xFF
        res_scal = self._read_i1(f) if f.tell() - start_pos < rec_len else 0
        llm_scal = self._read_i1(f) if f.tell() - start_pos < rec_len else 0
        hlm_scal = self._read_i1(f) if f.tell() - start_pos < rec_len else 0
        lo_limit = self._read_r4(f) if f.tell() - start_pos < rec_len else None
        hi_limit = self._read_r4(f) if f.tell() - start_pos < rec_len else None
        start_in = self._read_r4(f) if f.tell() - start_pos < rec_len else 0.0
        incr_in = self._read_r4(f) if f.tell() - start_pos < rec_len else 0.0
        
        # RTN_INDX: Array of pin indexes (U*2) - comes after incr_in per spec
        rtn_indx = []
        for _ in range(rtn_icnt):
            if f.tell() - start_pos >= rec_len:
                break
            rtn_indx.append(self._read_u2(f))
        
        # UNITS, C_RESFMT, C_LLMFMT, C_HLMFMT, LO_SPEC, HI_SPEC are last
        units = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        
        passed = (test_flg & 0x80) == 0

        # Register test definition
        if test_num not in self.data.tests:
            self.data.tests[test_num] = {
                "test_num": test_num,
                "test_name": test_txt,
                "lo_limit": lo_limit,
                "hi_limit": hi_limit,
                "units": units,
                "test_type": "M",
                "rec_type": "MPR",
            }

        # Store single result (use first result)
        result_val = results[0] if results else None
        
        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}",
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": result_val,
            "passed": passed,
            "alarm_id": alarm_id,
        })

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_hbr(self, f: BinaryIO, rec_len: int):
        """Parse Hardware Bin Record."""
        start_pos = f.tell()
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        hbin_num = self._read_u2(f)
        hbin_cnt = self._read_u4(f)
        hbin_pf = chr(self._read_u1(f)) if f.tell() - start_pos < rec_len else ""
        hbin_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""

        self.data.bins_hard[hbin_num] = {
            "bin_num": hbin_num,
            "bin_name": hbin_nam,
            "bin_pf": hbin_pf,
            "bin_count": hbin_cnt,
        }

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_sbr(self, f: BinaryIO, rec_len: int):
        """Parse Software Bin Record."""
        start_pos = f.tell()
        head_num = self._read_u1(f)
        site_num = self._read_u1(f)
        sbin_num = self._read_u2(f)
        sbin_cnt = self._read_u4(f)
        sbin_pf = chr(self._read_u1(f)) if f.tell() - start_pos < rec_len else ""
        sbin_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""

        self.data.bins_soft[sbin_num] = {
            "bin_num": sbin_num,
            "bin_name": sbin_nam,
            "bin_pf": sbin_pf,
            "bin_count": sbin_cnt,
        }

        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def parse(self, file_path: Path) -> STDFData:
        """Parse an STDF file."""
        self.data = STDFData()
        self._part_counter = 0

        with open(file_path, "rb") as f:
            while True:
                try:
                    rec_len, rec_typ, rec_sub = self._read_header(f)
                    rec_key = (rec_typ, rec_sub)
                    start_pos = f.tell()

                    if rec_key == REC_FAR:
                        self._parse_far(f, rec_len)
                    elif rec_key == REC_MIR:
                        self._parse_mir(f, rec_len)
                    elif rec_key == REC_MRR:
                        self._parse_mrr(f, rec_len)
                    elif rec_key == REC_WIR:
                        self._parse_wir(f, rec_len)
                    elif rec_key == REC_WRR:
                        self._parse_wrr(f, rec_len)
                    elif rec_key == REC_PIR:
                        self._parse_pir(f, rec_len)
                    elif rec_key == REC_PRR:
                        self._parse_prr(f, rec_len)
                    elif rec_key == REC_PTR:
                        self._parse_ptr(f, rec_len)
                    elif rec_key == REC_MPR:
                        self._parse_mpr(f, rec_len)
                    elif rec_key == REC_FTR:
                        self._parse_ftr(f, rec_len)
                    elif rec_key == REC_HBR:
                        self._parse_hbr(f, rec_len)
                    elif rec_key == REC_SBR:
                        self._parse_sbr(f, rec_len)
                    else:
                        # Skip unknown record
                        f.read(rec_len)

                    # Ensure we consumed exactly rec_len bytes
                    consumed = f.tell() - start_pos
                    if consumed < rec_len:
                        f.read(rec_len - consumed)

                except EOFError:
                    break
                except Exception as e:
                    # Skip problematic record and continue
                    continue

        return self.data


def _parse_stdf_python(file_path: Path) -> STDFData:
    """Parse an STDF file using the Python parser."""
    parser = STDFParser()
    return parser.parse(file_path)


def _convert_rust_result(d: dict) -> STDFData:
    """Convert Rust parser dict result to STDFData."""
    data = STDFData()
    data.lot_id = d.get("lot_id", "")
    data.part_type = d.get("part_type", "")
    data.job_name = d.get("job_name", "")
    data.job_rev = d.get("job_rev", "")
    data.start_time = d.get("start_time", 0)
    data.finish_time = d.get("finish_time", 0)
    data.tester_type = d.get("tester_type", "")
    data.operator = d.get("operator", "")
    data.test_code = d.get("test_code", "")
    data.wafers = d.get("wafers", [])
    data.parts = d.get("parts", [])
    data.test_results = d.get("test_results", [])
    data.tests = {int(k): v for k, v in d.get("tests", {}).items()}
    data.bins_hard = {int(k): v for k, v in d.get("bins_hard", {}).items()}
    data.bins_soft = {int(k): v for k, v in d.get("bins_soft", {}).items()}
    return data


# Try Rust parser, fallback to Python
try:
    from stdf2pq_rs import parse_stdf as _parse_stdf_rs
    _USE_RUST = True
except ImportError:
    _USE_RUST = False


def parse_stdf(file_path: Path) -> STDFData:
    """Parse an STDF file. Uses Rust parser if available, Python fallback otherwise."""
    if _USE_RUST:
        result = _parse_stdf_rs(str(file_path))
        return _convert_rust_result(result)
    return _parse_stdf_python(file_path)
