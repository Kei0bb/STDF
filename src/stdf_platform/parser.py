"""STDF binary parser - pure Python, no external dependencies."""

import struct
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import BinaryIO

logger = logging.getLogger(__name__)

# Pre-compiled struct objects (module-level, endian-neutral sizes used for header)
_STRUCT_HEADER_LE = struct.Struct("<H")
_STRUCT_HEADER_BE = struct.Struct(">H")


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
    """Binary STDF V4 parser with pre-compiled struct objects for performance."""

    def __init__(self):
        self.data = STDFData()
        self._part_counter = 0
        self._cached_part_id = ""  # reused across all test results for current part
        self._set_endian("<")  # Little endian by default

    def _set_endian(self, endian: str):
        """Set endianness and rebuild all pre-compiled struct objects."""
        self._endian = endian
        self._s_u1 = struct.Struct(endian + "B")
        self._s_u2 = struct.Struct(endian + "H")
        self._s_u4 = struct.Struct(endian + "I")
        self._s_i1 = struct.Struct(endian + "b")
        self._s_i2 = struct.Struct(endian + "h")
        self._s_r4 = struct.Struct(endian + "f")
        # Pre-compiled headers for hot-path record types
        self._s_ftr_hdr = struct.Struct(endian + "IBBB")   # test_num, head, site, test_flg
        self._s_ptr_hdr = struct.Struct(endian + "IBBBB")  # test_num, head, site, test_flg, parm_flg

    def _read_u1(self, f: BinaryIO) -> int:
        data = f.read(1)
        if len(data) < 1:
            raise EOFError()
        return self._s_u1.unpack(data)[0]

    def _read_u2(self, f: BinaryIO) -> int:
        data = f.read(2)
        if len(data) < 2:
            raise EOFError()
        return self._s_u2.unpack(data)[0]

    def _read_u4(self, f: BinaryIO) -> int:
        data = f.read(4)
        if len(data) < 4:
            raise EOFError()
        return self._s_u4.unpack(data)[0]

    def _read_i1(self, f: BinaryIO) -> int:
        data = f.read(1)
        if len(data) < 1:
            raise EOFError()
        return self._s_i1.unpack(data)[0]

    def _read_i2(self, f: BinaryIO) -> int:
        data = f.read(2)
        if len(data) < 2:
            raise EOFError()
        return self._s_i2.unpack(data)[0]

    def _read_r4(self, f: BinaryIO) -> float:
        data = f.read(4)
        if len(data) < 4:
            raise EOFError()
        return self._s_r4.unpack(data)[0]

    def _read_cn(self, f: BinaryIO) -> str:
        """Read character string (length-prefixed)."""
        length = self._read_u1(f)
        if length == 0:
            return ""
        data = f.read(length)
        try:
            return data.decode("ascii", errors="replace").replace("\x00", "").strip()
        except Exception:
            return ""

    def _read_header(self, f: BinaryIO) -> tuple[int, int, int]:
        """Read 4-byte record header. Returns (rec_len, rec_typ, rec_sub)."""
        data = f.read(4)
        if len(data) < 4:
            raise EOFError()
        rec_len = self._s_u2.unpack(data[0:2])[0]
        rec_typ = data[2]
        rec_sub = data[3]
        return rec_len, rec_typ, rec_sub

    def _parse_far(self, f: BinaryIO, rec_len: int):
        """Parse File Attributes Record — sets endianness for all subsequent reads."""
        cpu_type = self._read_u1(f)
        _stdf_ver = self._read_u1(f)
        self._set_endian(">" if cpu_type == 1 else "<")

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
        # Cache part_id once per part so test records don't re-allocate the string 59k times
        self._cached_part_id = f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}"

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
        body = f.read(rec_len)
        if len(body) < 8:
            return
        test_num, head_num, site_num, test_flg, parm_flg = self._s_ptr_hdr.unpack_from(body, 0)
        offset = 8

        result = None
        if offset + 4 <= rec_len:
            result = self._s_r4.unpack_from(body, offset)[0]
            offset += 4

        # test_txt (Cn: 1 byte length prefix)
        test_txt = ""
        if offset < rec_len:
            n = body[offset]; offset += 1
            if n > 0 and offset + n <= rec_len:
                test_txt = body[offset:offset + n].decode("ascii", errors="replace").replace("\x00", "").strip()
                offset += n

        # alarm_id — skip bytes but don't store (almost always empty, not queried)
        if offset < rec_len:
            n = body[offset]; offset += 1
            offset += n

        # opt_flag(1) + res_scal(1) + llm_scal(1) + hlm_scal(1) = 4 bytes
        offset += 4

        lo_limit = None
        if offset + 4 <= rec_len:
            lo_limit = self._s_r4.unpack_from(body, offset)[0]
            offset += 4

        hi_limit = None
        if offset + 4 <= rec_len:
            hi_limit = self._s_r4.unpack_from(body, offset)[0]
            offset += 4

        units = ""
        if offset < rec_len:
            n = body[offset]; offset += 1
            if n > 0 and offset + n <= rec_len:
                units = body[offset:offset + n].decode("ascii", errors="replace").replace("\x00", "").strip()

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
            "part_id": self._cached_part_id,
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": result,
            "passed": passed,
            "alarm_id": "",
        })

    def _parse_ftr(self, f: BinaryIO, rec_len: int):
        """Parse Functional Test Record."""
        body = f.read(rec_len)
        if len(body) < 7:
            return
        test_num, head_num, site_num, test_flg = self._s_ftr_hdr.unpack_from(body, 0)
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
            "part_id": self._cached_part_id,
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": None,
            "passed": passed,
            "alarm_id": "",
        })

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
            "part_id": self._cached_part_id,
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

                    # rec_len=0 means zero-padded or corrupt data — skip
                    if rec_len == 0:
                        continue

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
                    logger.debug("Skipping record (typ=%s, sub=%s): %s", rec_typ, rec_sub, e)
                    continue

        return self.data


def parse_stdf(file_path: Path) -> STDFData:
    """Parse an STDF file using the optimized Python parser."""
    parser = STDFParser()
    return parser.parse(file_path)
