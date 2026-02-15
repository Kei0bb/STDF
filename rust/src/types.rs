//! STDF data types and structures — pure Rust, no Python dependencies.

use std::collections::HashMap;

/// Record type constants (typ, sub).
pub const REC_FAR: (u8, u8) = (0, 10);
pub const REC_MIR: (u8, u8) = (1, 10);
pub const REC_MRR: (u8, u8) = (1, 20);
pub const REC_HBR: (u8, u8) = (1, 40);
pub const REC_SBR: (u8, u8) = (1, 50);
pub const REC_WIR: (u8, u8) = (2, 10);
pub const REC_WRR: (u8, u8) = (2, 20);
pub const REC_PIR: (u8, u8) = (5, 10);
pub const REC_PRR: (u8, u8) = (5, 20);
pub const REC_PTR: (u8, u8) = (15, 10);
pub const REC_MPR: (u8, u8) = (15, 15);
pub const REC_FTR: (u8, u8) = (15, 20);

/// Wafer record data.
#[derive(Clone, Debug)]
pub struct WaferData {
    pub wafer_id: String,
    pub head_num: i64,
    pub start_time: i64,
    pub finish_time: i64,
    pub part_count: i64,
    pub good_count: i64,
    pub rtst_count: i64,
    pub abrt_count: i64,
}

/// Part (die) record data.
#[derive(Clone, Debug)]
pub struct PartData {
    pub part_id: String,
    pub wafer_id: String,
    pub head_num: i64,
    pub site_num: i64,
    pub x_coord: i64,
    pub y_coord: i64,
    pub hard_bin: i64,
    pub soft_bin: i64,
    pub passed: bool,
    pub test_count: i64,
    pub test_time: i64,
}

/// Test definition (from PTR/MPR/FTR header info).
#[derive(Clone, Debug)]
pub struct TestDef {
    pub test_num: i64,
    pub test_name: String,
    pub rec_type: String,
    pub lo_limit: f64,
    pub hi_limit: f64,
    pub units: String,
}

/// Single test result row.
#[derive(Clone, Debug)]
pub struct TestResult {
    pub part_id: String,
    pub wafer_id: String,
    pub x_coord: i64,
    pub y_coord: i64,
    pub test_num: i64,
    pub test_name: String,
    pub rec_type: String,
    pub lo_limit: f64,
    pub hi_limit: f64,
    pub units: String,
    pub result: f64,
    pub passed: bool,
}

/// Bin record.
#[derive(Clone, Debug)]
pub struct BinData {
    pub bin_num: i64,
    pub bin_count: i64,
    pub bin_name: String,
    pub bin_pf: String,
}

/// Top-level parsed STDF data — mirrors Python STDFData.
#[derive(Clone, Debug)]
pub struct StdfData {
    pub lot_id: String,
    pub part_type: String,
    pub job_name: String,
    pub job_rev: String,
    pub start_time: i64,
    pub finish_time: i64,
    pub tester_type: String,
    pub operator: String,
    pub test_code: String,

    pub wafers: Vec<WaferData>,
    pub parts: Vec<PartData>,
    pub test_results: Vec<TestResult>,

    pub tests: HashMap<i64, TestDef>,
    pub bins_hard: HashMap<i64, BinData>,
    pub bins_soft: HashMap<i64, BinData>,
}

impl StdfData {
    pub fn new() -> Self {
        Self {
            lot_id: String::new(),
            part_type: String::new(),
            job_name: String::new(),
            job_rev: String::new(),
            start_time: 0,
            finish_time: 0,
            tester_type: String::new(),
            operator: String::new(),
            test_code: String::new(),
            wafers: Vec::new(),
            parts: Vec::new(),
            test_results: Vec::new(),
            tests: HashMap::new(),
            bins_hard: HashMap::new(),
            bins_soft: HashMap::new(),
        }
    }
}
