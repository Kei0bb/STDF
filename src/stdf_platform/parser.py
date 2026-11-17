"""STDF binary parser - pure Python, no external dependencies."""

import re
import struct
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import BinaryIO

logger = logging.getLogger(__name__)

# C0/C1 die suite inside a test-time GDR, e.g.
# "CHIPID_NRD0_C0:IP_FN_0_OTP_CID_NR_FRC_FT_12_05::". The separators were only
# seen through a blurry screenshot, so space/underscore and 0/O are all accepted.
_SUITE_RE = re.compile(r"(?:^|:)(CHIPID[ _]NRD[0O][ _](C[01]))(?::|$)")
# A CHIP ID candidate waits at most this many GDRs for its C0/C1 suite.
CHIPID_SUITE_WINDOW = 64
_UNRESOLVED_MSG = "CHIP ID found but no C0/C1 suite association found"


class _PartCtx:
    """State of one open part (PIR seen, PRR not yet) on one head/site."""

    __slots__ = ("part_id", "seq", "head", "site", "chips", "pending", "gdr_since")

    def __init__(self, part_id: str, seq: int, head: int, site: int):
        self.part_id = part_id
        self.seq = seq                   # PIR order within the file
        self.head = head
        self.site = site
        self.chips: list[dict] = []     # finished CHIP ID candidates
        self.pending: dict | None = None  # candidate waiting for its suite GDR
        self.gdr_since = 0               # GDRs seen since `pending` was opened

    def close_pending(self, warning: str = _UNRESOLVED_MSG) -> None:
        if self.pending is not None:
            self.pending["warnings"].append(warning)
            self.chips.append(self.pending)
            self.pending = None


def _part_chip_key(chips: list[dict]) -> str:
    """Package identity from its die eFuses: C0, else C1 (only an unambiguous one).

    All-0 / all-1 eFuses are unprogrammed and would merge every package into
    one key, so they never form a key.
    """
    from .chipid import normalize_efuse
    for die in ("C0", "C1"):
        vals = {normalize_efuse(c["efuse_raw"]) for c in chips if c["die_id"] == die}
        vals.discard(None)
        if len(vals) == 1:
            v = vals.pop()
            if v.count("1") not in (0, 64):
                return f"EF|{int(v, 2):016X}"
    return ""


# STDF V4 flag bits.
#   TEST_FLG: 0x02 result invalid, 0x10 test not executed, 0x40 pass/fail
#             flag invalid, 0x80 fail.  PART_FLG: 0x08 fail, 0x10 pass/fail
#             invalid.  OPT_FLAG (PTR/MPR): 0x10 LO_LIMIT invalid, 0x20
#             HI_LIMIT invalid, 0x40 no low limit, 0x80 no high limit.
def _test_passed(test_flg: int) -> bool | None:
    """None when the record carries no valid pass/fail (not executed / invalid)."""
    if test_flg & 0x50:
        return None
    return not test_flg & 0x80


def _limits(opt_flag: int | None, lo, hi):
    if opt_flag is None:
        return lo, hi
    return (None if opt_flag & 0x50 else lo), (None if opt_flag & 0xA0 else hi)


# GDR integer field type code -> (byte size, struct code)
_GDR_INT = {1: (1, "B"), 2: (2, "H"), 3: (4, "I"), 4: (1, "b"), 5: (2, "h"), 6: (4, "i")}

# Pre-compiled struct objects (module-level, endian-neutral sizes used for header)
_STRUCT_HEADER_LE = struct.Struct("<H")
_STRUCT_HEADER_BE = struct.Struct(">H")


class STDFFormatError(ValueError):
    """The file is not a usable STDF V4 file (no FAR/MIR, or truncated)."""


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
    node_name: str = ""  # MIR.NODE_NAM — テスター号機名（tester_type は機種）

    # Equipment identity from SDR (1/80). A file may carry several SDRs (one
    # per head x site group); each field below is the sorted, de-duplicated,
    # comma-joined set of the non-empty values seen across all of them, so a
    # single-value field stays a plain string and a genuinely mixed setup
    # reads as "SKT-A,SKT-B" instead of silently keeping one.
    handler_type: str = ""     # SDR.HAND_TYP
    handler_id: str = ""       # SDR.HAND_ID
    probe_card_type: str = ""  # SDR.CARD_TYP
    probe_card_id: str = ""    # SDR.CARD_ID
    loadboard_type: str = ""   # SDR.LOAD_TYP
    loadboard_id: str = ""     # SDR.LOAD_ID
    socket_type: str = ""      # SDR.CONT_TYP
    socket_id: str = ""        # SDR.CONT_ID

    # Records by type
    wafers: list[dict] = field(default_factory=list)
    parts: list[dict] = field(default_factory=list)
    tests: dict[int, dict] = field(default_factory=dict)
    test_results: list[dict] = field(default_factory=list)
    # EN-S0-CHIPID_R candidates from GDR (one row per die occurrence) with the
    # C0/C1 die resolved from the following CHIPID suite GDR — see _parse_gdr.
    chip_ids: list[dict] = field(default_factory=list)
    bins_hard: dict[int, dict] = field(default_factory=dict)
    bins_soft: dict[int, dict] = field(default_factory=dict)
    # PMR pin map: pmr_index -> pin_name (LOG_NAM > PHY_NAM > CHAN_NAM)
    pin_map: dict[int, str] = field(default_factory=dict)

    # True when the file ends inside a record (only kept with allow_truncated)
    truncated: bool = False

    # Internal state
    _current_wafer: str = ""


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
REC_GDR = (50, 10)

# STDFData attribute names filled from SDR, in the order the record stores them.
SDR_FIELDS = (
    "handler_type", "handler_id",
    "probe_card_type", "probe_card_id",
    "loadboard_type", "loadboard_id",
    "socket_type", "socket_id",
)


class STDFParser:
    """Binary STDF V4 parser with pre-compiled struct objects for performance."""

    def __init__(self):
        self.data = STDFData()
        self._part_counter = 0
        # Last opened part — fallback for records whose head/site has no PIR.
        self._cached_part_id = ""
        # Open parts by (head, site): multi-site testers interleave PIR s1,
        # PIR s2, results of both, PRR s1, PRR s2, so one "current part" is wrong.
        self._open_parts: dict[tuple[int, int], _PartCtx] = {}
        self._site_part: dict[tuple[int, int], str] = {}
        self._rec_start = 0  # byte offset of the record being parsed
        self._sdr_values = {name: set() for name in SDR_FIELDS}
        # Absolute end offset of the record currently being parsed; None outside
        # a record. _read_cn clamps to it so a corrupt length cannot overrun
        # into the next record (the parse loop also seek-resyncs afterward).
        self._rec_end: int | None = None
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
        if self._rec_end is not None:
            # 壊れた長さフィールドで次レコードへ読み越さない。はみ出した分は
            # 読み捨て、レコード側の復帰シークで整合させる。
            length = min(length, max(0, self._rec_end - f.tell()))
            if length == 0:
                return ""
        data = f.read(length)
        try:
            return data.decode("ascii", errors="replace").replace("\x00", "").strip()
        except Exception:
            return ""

    def _sniff_endian(self, f: BinaryIO) -> None:
        """Decide the byte order of the FAR header itself from the first 4 bytes.

        `_parse_far` sets the byte order for everything *after* the FAR from its
        CPU_TYPE byte — but the FAR's own REC_LEN (2 bytes) is already written in
        the file's byte order. Read little-endian, a big-endian file yields
        rec_len = 0x0200 and the parse loop skips 512 bytes past the FAR, losing
        the MIR (and with it lot_id). The FAR is always the first record, always
        rec_len=2, always (typ, sub) = (0, 10), so the length bytes identify the
        order unambiguously. A file that does not start with a FAR keeps the
        little-endian default, exactly as before.
        """
        head = f.read(4)
        f.seek(0)
        if len(head) == 4 and head[2] == 0 and head[3] == 10:
            if head[0] == 0 and head[1] == 2:
                self._set_endian(">")
            elif head[0] == 2 and head[1] == 0:
                self._set_endian("<")

    def _read_header(self, f: BinaryIO) -> tuple[int, int, int]:
        """Read 4-byte record header. Returns (rec_len, rec_typ, rec_sub)."""
        data = f.read(4)
        if len(data) < 4:
            self._cut_header = len(data) > 0
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

    def _parse_pmr(self, f: BinaryIO, rec_len: int):
        """Parse Pin Map Record — builds pin_map index → name for MPR resolution."""
        start_pos = f.tell()
        pmr_indx = self._read_u2(f)
        _chan_typ = self._read_u2(f) if f.tell() - start_pos < rec_len else 0
        chan_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        phy_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        log_nam = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        # Prefer LOG_NAM → PHY_NAM → CHAN_NAM as the human-readable pin name
        pin_name = log_nam or phy_nam or chan_nam
        self.data.pin_map[pmr_indx] = pin_name
        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

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
        self.data.node_name = node_nam

        # Skip remaining optional fields
        remaining = rec_len - (f.tell() - start_pos)
        if remaining > 0:
            f.read(remaining)

    def _parse_sdr(self, f: BinaryIO, rec_len: int):
        """Parse Site Description Record — handler / probe card / loadboard / socket.

        Field order is fixed: HEAD_NUM, SITE_GRP, SITE_CNT, SITE_NUM[SITE_CNT],
        then Cn pairs HAND_TYP/HAND_ID, CARD_TYP/CARD_ID, LOAD_TYP/LOAD_ID,
        DIB_TYP/DIB_ID, CABL_TYP/CABL_ID, CONT_TYP/CONT_ID, LASR_*, EXTR_*.
        Every Cn is optional-by-truncation — real testers stop writing partway
        through — so each read is bounded by rec_len and yields "" past the end.
        The DIB / CABL / LASR / EXTR pairs are read only to reach CONT_*.

        Values accumulate across every SDR in the file; `parse()` collapses
        them (see SDR_FIELDS).
        """
        start_pos = f.tell()

        self._read_u1(f)  # HEAD_NUM
        if f.tell() - start_pos < rec_len:
            self._read_u1(f)  # SITE_GRP
        site_cnt = self._read_u1(f) if f.tell() - start_pos < rec_len else 0
        if site_cnt:
            f.read(min(site_cnt, rec_len - (f.tell() - start_pos)))  # SITE_NUM[]

        def next_cn() -> str:
            return self._read_cn(f) if f.tell() - start_pos < rec_len else ""

        values = {
            "handler_type": next_cn(), "handler_id": next_cn(),
            "probe_card_type": next_cn(), "probe_card_id": next_cn(),
            "loadboard_type": next_cn(), "loadboard_id": next_cn(),
        }
        for _ in range(4):  # DIB_TYP/DIB_ID, CABL_TYP/CABL_ID — read past only
            next_cn()
        values["socket_type"] = next_cn()
        values["socket_id"] = next_cn()

        for name, value in values.items():
            if value:
                self._sdr_values[name].add(value)

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
        # Cache part_id once per part so test records don't re-allocate the string 59k times
        part_id = f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}"
        self._cached_part_id = part_id
        key = (head_num, site_num)
        if key in self._open_parts:
            logger.debug("PIR for head %d site %d without PRR for the previous part",
                         head_num, site_num)
        self._open_parts[key] = _PartCtx(part_id, self._part_counter, head_num, site_num)
        self._site_part[key] = part_id

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
        # PART_ID then PART_TXT (2D barcode) — display only, NOT FT identity (eFuse chip_key is)
        part_id_serial = self._read_cn(f) if f.tell() - start_pos < rec_len else ""
        part_txt = self._read_cn(f) if f.tell() - start_pos < rec_len else ""

        passed = None if part_flg & 0x10 else not part_flg & 0x08

        ctx = self._open_parts.pop((head_num, site_num), None)
        self._site_part.pop((head_num, site_num), None)
        if ctx is None:  # PRR without PIR: keep the old single-part behaviour
            synth_part_id = self._cached_part_id or (
                f"{self.data.lot_id}_{self.data._current_wafer}_{self._part_counter}")
            chips: list[dict] = []
            part_seq = self._part_counter
        else:
            synth_part_id = ctx.part_id
            part_seq = ctx.seq
            ctx.close_pending()
            chips = ctx.chips
        chip_key = _part_chip_key(chips)
        part = {
            "part_id": synth_part_id,
            "part_seq": part_seq,  # later occurrence of a die in one file wins
            "part_txt": part_txt,
            "part_serial": part_id_serial,  # PRR.PART_ID — 表示用（identity には使わない）
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "head_num": head_num,
            "site_num": site_num,
            "x_coord": x_coord,
            "y_coord": y_coord,
            "hard_bin": hard_bin,
            "soft_bin": soft_bin,
            "passed": passed,
            "part_flg": part_flg,
            "test_count": num_test,
            "test_time": test_t,
            "chip_key": chip_key,
        }
        self.data.parts.append(part)

        # Bind this part's CHIP ID candidates to it. Two candidates for the
        # same die are both kept (never overwritten) and flagged.
        dies = [c["die_id"] for c in chips]
        for occ_idx, chip in enumerate(chips):
            if chip["die_id"] != "UNKNOWN" and dies.count(chip["die_id"]) > 1:
                chip["warnings"].append(f"duplicate {chip['die_id']} CHIP ID in part")
            chip.update(
                lot_id=self.data.lot_id,
                wafer_id=self.data._current_wafer,
                part_id=synth_part_id,
                part_txt=part_txt,
                head_num=head_num,
                chip_occurrence_index=occ_idx,
                chip_key=chip_key,
            )
            self.data.chip_ids.append(chip)

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
        opt_flag = body[offset] if offset < rec_len else None
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

        passed = _test_passed(test_flg)
        if test_flg & 0x02:
            result = None
        self._register_test(test_num, test_txt, *_limits(opt_flag, lo_limit, hi_limit),
                            units, "P", "PTR")

        self.data.test_results.append({
            "lot_id": self.data.lot_id,
            "wafer_id": self.data._current_wafer,
            "part_id": self._site_part.get((head_num, site_num), self._cached_part_id),
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": result,
            "passed": passed,
            "test_flg": test_flg,
            "alarm_id": "",
        })

    def _register_test(self, test_num, test_name, lo, hi, units, test_type, rec_type):
        """Test definition from the first record; a limit missing there (no
        default given) is filled from a later record that carries one."""
        t = self.data.tests.get(test_num)
        if t is None:
            self.data.tests[test_num] = {
                "test_num": test_num, "test_name": test_name,
                "lo_limit": lo, "hi_limit": hi, "units": units,
                "test_type": test_type, "rec_type": rec_type,
            }
            return
        if t.get("lo_limit") is None and lo is not None:
            t["lo_limit"] = lo
        if t.get("hi_limit") is None and hi is not None:
            t["hi_limit"] = hi

    def _parse_ftr(self, f: BinaryIO, rec_len: int):
        """Parse Functional Test Record."""
        body = f.read(rec_len)
        if len(body) < 7:
            return
        test_num, head_num, site_num, test_flg = self._s_ftr_hdr.unpack_from(body, 0)
        passed = _test_passed(test_flg)

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
            "part_id": self._site_part.get((head_num, site_num), self._cached_part_id),
            "test_num": test_num,
            "head_num": head_num,
            "site_num": site_num,
            "result": None,
            "passed": passed,
            "test_flg": test_flg,
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

        passed = _test_passed(test_flg)
        if test_flg & 0x02:
            results = [None] * len(results)
        self._register_test(
            test_num, test_txt,
            *_limits(None if opt_flag == 0xFF and lo_limit is None else opt_flag,
                     lo_limit, hi_limit),
            units, "M", "MPR")

        part_id = self._site_part.get((head_num, site_num), self._cached_part_id)
        # Expand per-pin results: each pin becomes a separate row.
        # When both arrays are empty (pass/fail-only MPR with no measurements), emit
        # one summary row rather than a phantom row from max(..., 1).
        if not results and not rtn_indx:
            self.data.test_results.append({
                "lot_id": self.data.lot_id,
                "wafer_id": self.data._current_wafer,
                "part_id": part_id,
                "test_num": test_num,
                "head_num": head_num,
                "site_num": site_num,
                "result": None,
                "passed": passed,
                "test_flg": test_flg,
                "alarm_id": alarm_id,
                "pin_num": None,
                "pin_name": None,
            })
        else:
            if rtn_icnt != rslt_cnt:
                logger.debug(
                    "MPR rtn_icnt/rslt_cnt mismatch: icnt=%d rslt=%d test_num=%d",
                    rtn_icnt, rslt_cnt, test_num,
                )
            n = max(len(results), len(rtn_indx))
            for i in range(n):
                pin_idx = rtn_indx[i] if i < len(rtn_indx) else None
                self.data.test_results.append({
                    "lot_id": self.data.lot_id,
                    "wafer_id": self.data._current_wafer,
                    "part_id": part_id,
                    "test_num": test_num,
                    "head_num": head_num,
                    "site_num": site_num,
                    "result": results[i] if i < len(results) else None,
                    "passed": passed,
                    "test_flg": test_flg,
                    "alarm_id": alarm_id,
                    "pin_num": pin_idx,
                    "pin_name": self.data.pin_map.get(pin_idx) if pin_idx is not None else None,
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

    def _parse_gdr(self, f: BinaryIO, rec_len: int):
        """Parse Generic Data Record: CHIP ID candidates and their C0/C1 suite.

        A 2-die package logs, per die, ``GDR [site I4] "EN-S0-CHIPID_R"
        "0b<64 bits>"`` followed by a test-time GDR whose Cn names the suite
        ``CHIPID NRD0 C0`` / ``C1``. The tag is the same for both dies, so the
        die is taken from the suite name — never from GDR order. A candidate
        is paired with the first suite GDR of the same part/site, bounded by
        the next candidate, the PRR and CHIPID_SUITE_WINDOW GDRs; otherwise it
        is kept as UNKNOWN with a warning.

        Fields are walked by type bounded by rec_len (pad bytes counted
        inconsistently); the site is the last integer field before the first
        Cn, and the tester head/site is never confused with the die id.
        """
        start_pos = f.tell()
        end = start_pos + rec_len
        self._read_u2(f)  # FLD_CNT (advisory; we bound by rec_len instead)

        strings: list[str] = []
        site: int | None = None
        while f.tell() < end:
            type_code = self._read_u1(f)
            if type_code == 0:          # B*0 pad byte
                continue
            elif type_code == 10:       # C*n
                strings.append(self._read_cn(f))
            elif 1 <= type_code <= 6:   # U1 U2 U4 I1 I2 I4
                size, fmt = _GDR_INT[type_code]
                raw = f.read(size)
                if len(raw) < size:
                    break
                if not strings:
                    site = struct.unpack(self._endian + fmt, raw)[0]
            elif type_code == 7:        # R*4
                f.read(4)
            elif type_code == 8:        # R*8
                f.read(8)
            elif type_code == 11:       # B*n
                blen = self._read_u1(f)
                f.read(blen)
            elif type_code == 12:       # D*n (bit field)
                nbits = self._read_u2(f)
                f.read((nbits + 7) // 8)
            elif type_code == 13:       # N*1 (nibble)
                f.read(1)
            else:
                break  # unknown type — cannot determine length, stop safely

        # Age pending candidates first so a GDR never counts against itself.
        for ctx in self._open_parts.values():
            if ctx.pending is not None:
                ctx.gdr_since += 1
                if ctx.gdr_since > CHIPID_SUITE_WINDOW:
                    ctx.close_pending()

        from .chipid import CHIPID_KEYS
        value = None
        suite = None
        for i, text in enumerate(strings):
            if text in CHIPID_KEYS:
                value = strings[i + 1] if i + 1 < len(strings) else ""
                break
            if "CHIPID" in text:
                m = _SUITE_RE.search(text)
                if m:
                    suite = m
                    break
        if value is None and suite is None:
            return

        ctx, note = self._gdr_part(site)
        if value is not None:
            cand = {
                "efuse_raw": value,
                "site_num": site if site is not None else (ctx.site if ctx else None),
                "die_id": "UNKNOWN",
                "association": "unresolved",
                "suite_name": None,
                "gdr_offset": self._rec_start,
                "suite_gdr_offset": None,
                "warnings": [note] if note else [],
            }
            if ctx is None:
                # No part to bind to: keep the value, never guess a site.
                cand["warnings"].append(_UNRESOLVED_MSG)
                cand.update(lot_id=self.data.lot_id, wafer_id=self.data._current_wafer,
                            part_id="", part_txt="", head_num=None,
                            chip_occurrence_index=0, chip_key="")
                self.data.chip_ids.append(cand)
                return
            ctx.close_pending()  # the next candidate bounds the previous one
            ctx.pending = cand
            ctx.gdr_since = 0
        elif ctx is not None and ctx.pending is not None:
            cand = ctx.pending
            cand["die_id"] = suite.group(2)
            cand["suite_name"] = suite.group(1)
            cand["suite_gdr_offset"] = self._rec_start
            cand["association"] = ("following suite exact" if ctx.gdr_since == 1
                                   else "following suite window")
            ctx.chips.append(cand)
            ctx.pending = None

    def _gdr_part(self, site: int | None) -> tuple["_PartCtx | None", str]:
        """Open part a GDR belongs to (GDRs carry a site but no head)."""
        parts = self._open_parts
        if site is not None:
            hits = [c for c in parts.values() if c.site == site]
            if len(hits) == 1:
                return hits[0], ""
        if len(parts) == 1:
            only = next(iter(parts.values()))
            if site is None:
                return only, "site missing in GDR; taken from the only open part"
            return only, f"GDR site {site} has no open part; taken from the only open part"
        return None, ("GDR outside any part" if not parts
                      else f"GDR site {site} does not identify one open part")

    def _dedupe_chip_keys(self) -> None:
        """Resolve a chip_key that appears on several parts of one file.

        If every such part carries the same C0 AND the same C1 eFuse, it is
        one package inserted again (handler re-test in the same lot): the key
        is kept and the later part wins at flag recomputation (part_seq).
        Otherwise the key cannot be an identity — overlaying by it would merge
        different packages — so those parts fall back to the file-unique key
        (no overlay).
        """
        seen: dict[str, list[dict]] = {}
        for part in self.data.parts:
            k = part.get("chip_key", "")
            if k:
                seen.setdefault(k, []).append(part)
        shared = {k: ps for k, ps in seen.items() if len(ps) > 1}
        if not shared:
            return
        from .chipid import normalize_efuse
        by_part: dict[str, list[dict]] = {}
        for chip in self.data.chip_ids:
            by_part.setdefault(chip.get("part_id", ""), []).append(chip)

        def pair(part: dict) -> tuple | None:
            chips = by_part.get(part["part_id"], [])
            out = []
            for die in ("C0", "C1"):
                vals = {normalize_efuse(c["efuse_raw"]) for c in chips if c["die_id"] == die}
                vals.discard(None)
                if len(vals) != 1:
                    return None
                out.append(vals.pop())
            return tuple(out)

        cleared: set[str] = set()
        reinserted: set[str] = set()
        for k, ps in shared.items():
            pairs = {pair(p) for p in ps}
            if None not in pairs and len(pairs) == 1:
                reinserted.add(k)
            else:
                cleared.add(k)
        if reinserted:
            logger.warning("%d packages appear more than once in this file "
                           "(same C0 and C1); the later part wins", len(reinserted))
        if cleared:
            logger.warning("%d CHIP ID keys appear on more than one part with "
                           "different C0/C1; those parts are not overlaid "
                           "across retests", len(cleared))
        ids = {k: {p["part_id"] for p in ps} for k, ps in shared.items()}
        for part in self.data.parts:
            if part.get("chip_key") in cleared:
                part["chip_key"] = ""
        for chip in self.data.chip_ids:
            k = chip.get("chip_key")
            if k in cleared and chip.get("part_id") in ids[k]:
                chip["chip_key"] = ""
                chip["warnings"].append("CHIP ID shared by several parts in file")
            elif k in reinserted and chip.get("part_id") in ids[k]:
                chip["warnings"].append("package re-inserted in file")

    def parse(self, file_path: Path, allow_truncated: bool = False) -> STDFData:
        """Parse an STDF file.

        Raises STDFFormatError when the file does not start with a FAR, has no
        MIR, or ends inside a record (an interrupted transfer / tester crash);
        ``allow_truncated=True`` keeps a truncated file's complete records and
        sets ``data.truncated``.
        """
        self.data = STDFData()
        self._part_counter = 0
        self._cached_part_id = ""
        self._open_parts = {}
        self._site_part = {}
        self._sdr_values = {name: set() for name in SDR_FIELDS}
        self._rec_end = None
        self._set_endian("<")  # 前ファイルの FAR を持ち越さない

        file_size = Path(file_path).stat().st_size
        seen_mir = False
        truncated = False
        self._cut_header = False
        with open(file_path, "rb") as f:
            head = f.read(4)
            f.seek(0)
            if len(head) < 4 or head[2:4] != b"\x00\x0a" or head[:2] not in (b"\x02\x00", b"\x00\x02"):
                raise STDFFormatError(f"{Path(file_path).name}: not an STDF V4 file (no FAR)")
            self._sniff_endian(f)
            while True:
                # 例外ハンドラが直前レコードの末尾に巻き戻して同じヘッダを
                # 読み直す(無限ループ)のを防ぐため、毎周回クリアする。
                rec_end: int | None = None
                rec_typ = rec_sub = None
                try:
                    rec_len, rec_typ, rec_sub = self._read_header(f)

                    # rec_len=0 means zero-padded or corrupt data — skip
                    if rec_len == 0:
                        continue

                    rec_key = (rec_typ, rec_sub)
                    start_pos = f.tell()
                    rec_end = start_pos + rec_len
                    if rec_end > file_size:
                        truncated = True
                        break
                    self._rec_end = rec_end
                    self._rec_start = start_pos - 4

                    if rec_key == REC_FAR:
                        self._parse_far(f, rec_len)
                    elif rec_key == REC_PMR:
                        self._parse_pmr(f, rec_len)
                    elif rec_key == REC_MIR:
                        self._parse_mir(f, rec_len)
                        seen_mir = True
                    elif rec_key == REC_SDR:
                        self._parse_sdr(f, rec_len)
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
                    elif rec_key == REC_GDR:
                        self._parse_gdr(f, rec_len)
                    else:
                        # Skip unknown record
                        f.read(rec_len)

                    # Ensure we consumed exactly rec_len bytes. A malformed
                    # record (e.g. an over-long Cn) may have over-read into the
                    # next record; always seek to the boundary so one bad record
                    # cannot desync the rest of the stream.
                    if f.tell() != rec_end:
                        f.seek(rec_end)

                except EOFError:
                    # 0 bytes left = clean end; 1-3 bytes = a cut-off header
                    truncated = truncated or self._cut_header
                    break
                except Exception as e:
                    logger.debug("Skipping record (typ=%s, sub=%s): %s", rec_typ, rec_sub, e)
                    if rec_end is None:
                        break  # ヘッダすら読めていない — 位置が不明なので打ち切る
                    f.seek(rec_end)
                    continue

        self._rec_end = None
        name = Path(file_path).name
        if not seen_mir:
            raise STDFFormatError(f"{name}: no MIR record (lot unknown)")
        if truncated:
            if not allow_truncated:
                raise STDFFormatError(
                    f"{name}: file ends inside a record (truncated); "
                    "re-fetch it, or ingest with --allow-truncated")
            logger.warning("%s: truncated; keeping complete records only", name)
            self.data.truncated = True
        self._dedupe_chip_keys()
        # Collapse every SDR's values into one string per field (see SDR_FIELDS).
        for name in SDR_FIELDS:
            setattr(self.data, name, ",".join(sorted(self._sdr_values[name])))

        return self.data


def parse_stdf(file_path: Path, allow_truncated: bool = False) -> STDFData:
    """Parse an STDF file using the optimized Python parser."""
    parser = STDFParser()
    return parser.parse(file_path, allow_truncated=allow_truncated)
