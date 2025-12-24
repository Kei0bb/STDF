"""STDF Record type definitions and PyArrow schema mappings."""

import pyarrow as pa

# STDF Record type names mapped to their PyArrow schemas
# Based on STDF V4 specification

RECORD_SCHEMAS: dict[str, pa.Schema] = {
    # FAR - File Attributes Record
    "FAR": pa.schema([
        ("CPU_TYPE", pa.uint8()),
        ("STDF_VER", pa.uint8()),
    ]),

    # ATR - Audit Trail Record
    "ATR": pa.schema([
        ("MOD_TIM", pa.uint32()),
        ("CMD_LINE", pa.string()),
    ]),

    # MIR - Master Information Record
    "MIR": pa.schema([
        ("SETUP_T", pa.uint32()),
        ("START_T", pa.uint32()),
        ("STAT_NUM", pa.uint8()),
        ("MODE_COD", pa.string()),
        ("RTST_COD", pa.string()),
        ("PROT_COD", pa.string()),
        ("BURN_TIM", pa.uint16()),
        ("CMOD_COD", pa.string()),
        ("LOT_ID", pa.string()),
        ("PART_TYP", pa.string()),
        ("NODE_NAM", pa.string()),
        ("TSTR_TYP", pa.string()),
        ("JOB_NAM", pa.string()),
        ("JOB_REV", pa.string()),
        ("SBLOT_ID", pa.string()),
        ("OPER_NAM", pa.string()),
        ("EXEC_TYP", pa.string()),
        ("EXEC_VER", pa.string()),
        ("TEST_COD", pa.string()),
        ("TST_TEMP", pa.string()),
        ("USER_TXT", pa.string()),
        ("AUX_FILE", pa.string()),
        ("PKG_TYP", pa.string()),
        ("FAMLY_ID", pa.string()),
        ("DATE_COD", pa.string()),
        ("FACIL_ID", pa.string()),
        ("FLOOR_ID", pa.string()),
        ("PROC_ID", pa.string()),
        ("OPER_FRQ", pa.string()),
        ("SPEC_NAM", pa.string()),
        ("SPEC_VER", pa.string()),
        ("FLOW_ID", pa.string()),
        ("SETUP_ID", pa.string()),
        ("DSGN_REV", pa.string()),
        ("ENG_ID", pa.string()),
        ("ROM_COD", pa.string()),
        ("SERL_NUM", pa.string()),
        ("SUPR_NAM", pa.string()),
    ]),

    # MRR - Master Results Record
    "MRR": pa.schema([
        ("FINISH_T", pa.uint32()),
        ("DISP_COD", pa.string()),
        ("USR_DESC", pa.string()),
        ("EXC_DESC", pa.string()),
    ]),

    # PCR - Part Count Record
    "PCR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("PART_CNT", pa.uint32()),
        ("RTST_CNT", pa.uint32()),
        ("ABRT_CNT", pa.uint32()),
        ("GOOD_CNT", pa.uint32()),
        ("FUNC_CNT", pa.uint32()),
    ]),

    # HBR - Hardware Bin Record
    "HBR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("HBIN_NUM", pa.uint16()),
        ("HBIN_CNT", pa.uint32()),
        ("HBIN_PF", pa.string()),
        ("HBIN_NAM", pa.string()),
    ]),

    # SBR - Software Bin Record
    "SBR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("SBIN_NUM", pa.uint16()),
        ("SBIN_CNT", pa.uint32()),
        ("SBIN_PF", pa.string()),
        ("SBIN_NAM", pa.string()),
    ]),

    # PMR - Pin Map Record
    "PMR": pa.schema([
        ("PMR_INDX", pa.uint16()),
        ("CHAN_TYP", pa.uint16()),
        ("CHAN_NAM", pa.string()),
        ("PHY_NAM", pa.string()),
        ("LOG_NAM", pa.string()),
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
    ]),

    # PGR - Pin Group Record
    "PGR": pa.schema([
        ("GRP_INDX", pa.uint16()),
        ("GRP_NAM", pa.string()),
        ("INDX_CNT", pa.uint16()),
        ("PMR_INDX", pa.list_(pa.uint16())),
    ]),

    # PLR - Pin List Record
    "PLR": pa.schema([
        ("GRP_CNT", pa.uint16()),
        ("GRP_INDX", pa.list_(pa.uint16())),
        ("GRP_MODE", pa.list_(pa.uint16())),
        ("GRP_RADX", pa.list_(pa.uint8())),
        ("PGM_CHAR", pa.list_(pa.string())),
        ("RTN_CHAR", pa.list_(pa.string())),
        ("PGM_CHAL", pa.list_(pa.string())),
        ("RTN_CHAL", pa.list_(pa.string())),
    ]),

    # RDR - Retest Data Record
    "RDR": pa.schema([
        ("NUM_BINS", pa.uint16()),
        ("RTST_BIN", pa.list_(pa.uint16())),
    ]),

    # SDR - Site Description Record
    "SDR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_GRP", pa.uint8()),
        ("SITE_CNT", pa.uint8()),
        ("SITE_NUM", pa.list_(pa.uint8())),
        ("HAND_TYP", pa.string()),
        ("HAND_ID", pa.string()),
        ("CARD_TYP", pa.string()),
        ("CARD_ID", pa.string()),
        ("LOAD_TYP", pa.string()),
        ("LOAD_ID", pa.string()),
        ("DIB_TYP", pa.string()),
        ("DIB_ID", pa.string()),
        ("CABL_TYP", pa.string()),
        ("CABL_ID", pa.string()),
        ("CONT_TYP", pa.string()),
        ("CONT_ID", pa.string()),
        ("LASR_TYP", pa.string()),
        ("LASR_ID", pa.string()),
        ("EXTR_TYP", pa.string()),
        ("EXTR_ID", pa.string()),
    ]),

    # WIR - Wafer Information Record
    "WIR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_GRP", pa.uint8()),
        ("START_T", pa.uint32()),
        ("WAFER_ID", pa.string()),
    ]),

    # WRR - Wafer Results Record
    "WRR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_GRP", pa.uint8()),
        ("FINISH_T", pa.uint32()),
        ("PART_CNT", pa.uint32()),
        ("RTST_CNT", pa.uint32()),
        ("ABRT_CNT", pa.uint32()),
        ("GOOD_CNT", pa.uint32()),
        ("FUNC_CNT", pa.uint32()),
        ("WAFER_ID", pa.string()),
        ("FABWF_ID", pa.string()),
        ("FRAME_ID", pa.string()),
        ("MASK_ID", pa.string()),
        ("USR_DESC", pa.string()),
        ("EXC_DESC", pa.string()),
    ]),

    # WCR - Wafer Configuration Record
    "WCR": pa.schema([
        ("WAFR_SIZ", pa.float32()),
        ("DIE_HT", pa.float32()),
        ("DIE_WID", pa.float32()),
        ("WF_UNITS", pa.uint8()),
        ("WF_FLAT", pa.string()),
        ("CENTER_X", pa.int16()),
        ("CENTER_Y", pa.int16()),
        ("POS_X", pa.string()),
        ("POS_Y", pa.string()),
    ]),

    # PIR - Part Information Record
    "PIR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
    ]),

    # PRR - Part Results Record
    "PRR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("PART_FLG", pa.uint8()),
        ("NUM_TEST", pa.uint16()),
        ("HARD_BIN", pa.uint16()),
        ("SOFT_BIN", pa.uint16()),
        ("X_COORD", pa.int16()),
        ("Y_COORD", pa.int16()),
        ("TEST_T", pa.uint32()),
        ("PART_ID", pa.string()),
        ("PART_TXT", pa.string()),
        ("PART_FIX", pa.binary()),
    ]),

    # TSR - Test Synopsis Record
    "TSR": pa.schema([
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("TEST_TYP", pa.string()),
        ("TEST_NUM", pa.uint32()),
        ("EXEC_CNT", pa.uint32()),
        ("FAIL_CNT", pa.uint32()),
        ("ALRM_CNT", pa.uint32()),
        ("TEST_NAM", pa.string()),
        ("SEQ_NAME", pa.string()),
        ("TEST_LBL", pa.string()),
        ("OPT_FLAG", pa.uint8()),
        ("TEST_TIM", pa.float32()),
        ("TEST_MIN", pa.float32()),
        ("TEST_MAX", pa.float32()),
        ("TST_SUMS", pa.float32()),
        ("TST_SQRS", pa.float32()),
    ]),

    # PTR - Parametric Test Record
    "PTR": pa.schema([
        ("TEST_NUM", pa.uint32()),
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("TEST_FLG", pa.uint8()),
        ("PARM_FLG", pa.uint8()),
        ("RESULT", pa.float32()),
        ("TEST_TXT", pa.string()),
        ("ALARM_ID", pa.string()),
        ("OPT_FLAG", pa.uint8()),
        ("RES_SCAL", pa.int8()),
        ("LLM_SCAL", pa.int8()),
        ("HLM_SCAL", pa.int8()),
        ("LO_LIMIT", pa.float32()),
        ("HI_LIMIT", pa.float32()),
        ("UNITS", pa.string()),
        ("C_RESFMT", pa.string()),
        ("C_LLMFMT", pa.string()),
        ("C_HLMFMT", pa.string()),
        ("LO_SPEC", pa.float32()),
        ("HI_SPEC", pa.float32()),
    ]),

    # MPR - Multiple-Result Parametric Record
    "MPR": pa.schema([
        ("TEST_NUM", pa.uint32()),
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("TEST_FLG", pa.uint8()),
        ("PARM_FLG", pa.uint8()),
        ("RTN_ICNT", pa.uint16()),
        ("RSLT_CNT", pa.uint16()),
        ("RTN_STAT", pa.list_(pa.uint8())),
        ("RTN_RSLT", pa.list_(pa.float32())),
        ("TEST_TXT", pa.string()),
        ("ALARM_ID", pa.string()),
        ("OPT_FLAG", pa.uint8()),
        ("RES_SCAL", pa.int8()),
        ("LLM_SCAL", pa.int8()),
        ("HLM_SCAL", pa.int8()),
        ("LO_LIMIT", pa.float32()),
        ("HI_LIMIT", pa.float32()),
        ("START_IN", pa.float32()),
        ("INCR_IN", pa.float32()),
        ("RTN_INDX", pa.list_(pa.uint16())),
        ("UNITS", pa.string()),
        ("UNITS_IN", pa.string()),
        ("C_RESFMT", pa.string()),
        ("C_LLMFMT", pa.string()),
        ("C_HLMFMT", pa.string()),
        ("LO_SPEC", pa.float32()),
        ("HI_SPEC", pa.float32()),
    ]),

    # FTR - Functional Test Record
    "FTR": pa.schema([
        ("TEST_NUM", pa.uint32()),
        ("HEAD_NUM", pa.uint8()),
        ("SITE_NUM", pa.uint8()),
        ("TEST_FLG", pa.uint8()),
        ("OPT_FLAG", pa.uint8()),
        ("CYCL_CNT", pa.uint32()),
        ("REL_VADR", pa.uint32()),
        ("REPT_CNT", pa.uint32()),
        ("NUM_FAIL", pa.uint32()),
        ("XFAIL_AD", pa.int32()),
        ("YFAIL_AD", pa.int32()),
        ("VECT_OFF", pa.int16()),
        ("RTN_ICNT", pa.uint16()),
        ("PGM_ICNT", pa.uint16()),
        ("RTN_INDX", pa.list_(pa.uint16())),
        ("RTN_STAT", pa.list_(pa.uint8())),
        ("PGM_INDX", pa.list_(pa.uint16())),
        ("PGM_STAT", pa.list_(pa.uint8())),
        ("FAIL_PIN", pa.binary()),
        ("VECT_NAM", pa.string()),
        ("TIME_SET", pa.string()),
        ("OP_CODE", pa.string()),
        ("TEST_TXT", pa.string()),
        ("ALARM_ID", pa.string()),
        ("PROG_TXT", pa.string()),
        ("RSLT_TXT", pa.string()),
        ("PATG_NUM", pa.uint8()),
        ("SPIN_MAP", pa.binary()),
    ]),

    # BPS - Begin Program Section Record
    "BPS": pa.schema([
        ("SEQ_NAME", pa.string()),
    ]),

    # EPS - End Program Section Record
    "EPS": pa.schema([
        # EPS has no fields
    ]),

    # GDR - Generic Data Record
    "GDR": pa.schema([
        ("FLD_CNT", pa.uint16()),
        ("GEN_DATA", pa.binary()),
    ]),

    # DTR - Datalog Text Record
    "DTR": pa.schema([
        ("TEXT_DAT", pa.string()),
    ]),
}


# Record type names for reference
RECORD_TYPES = list(RECORD_SCHEMAS.keys())


def get_schema(record_type: str) -> pa.Schema | None:
    """Get PyArrow schema for a record type."""
    return RECORD_SCHEMAS.get(record_type)
