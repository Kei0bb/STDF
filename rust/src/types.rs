//! STDF data types and structures.

#[cfg(feature = "python")]
use pyo3::prelude::*;
use std::collections::HashMap;

/// Record type constants (typ, sub).
pub const REC_FAR: (u8, u8) = (0, 10);
pub const REC_MIR: (u8, u8) = (1, 10);
pub const REC_MRR: (u8, u8) = (1, 20);
pub const REC_PCR: (u8, u8) = (1, 30);
pub const REC_HBR: (u8, u8) = (1, 40);
pub const REC_SBR: (u8, u8) = (1, 50);
pub const REC_PMR: (u8, u8) = (1, 60);
pub const REC_WIR: (u8, u8) = (2, 10);
pub const REC_WRR: (u8, u8) = (2, 20);
pub const REC_WCR: (u8, u8) = (2, 30);
pub const REC_PIR: (u8, u8) = (5, 10);
pub const REC_PRR: (u8, u8) = (5, 20);
pub const REC_TSR: (u8, u8) = (10, 30);
pub const REC_PTR: (u8, u8) = (15, 10);
pub const REC_MPR: (u8, u8) = (15, 15);
pub const REC_FTR: (u8, u8) = (15, 20);
pub const REC_SDR: (u8, u8) = (1, 80);

/// Wafer record data.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyclass)]
pub struct WaferData {
    #[cfg_attr(feature = "python", pyo3(get))]
    pub wafer_id: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub head_num: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub start_time: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub finish_time: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub part_count: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub good_count: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub rtst_count: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub abrt_count: i64,
}

/// Part (die) record data.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "python", pyclass)]
pub struct PartData {
    #[cfg_attr(feature = "python", pyo3(get))]
    pub part_id: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub wafer_id: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub head_num: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub site_num: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub x_coord: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub y_coord: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub hard_bin: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub soft_bin: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub passed: bool,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub test_count: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
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
#[cfg_attr(feature = "python", pyclass)]
pub struct TestResult {
    #[cfg_attr(feature = "python", pyo3(get))]
    pub part_id: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub wafer_id: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub x_coord: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub y_coord: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub test_num: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub test_name: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub rec_type: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub lo_limit: f64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub hi_limit: f64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub units: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub result: f64,
    #[cfg_attr(feature = "python", pyo3(get))]
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
#[cfg_attr(feature = "python", pyclass)]
pub struct StdfData {
    #[cfg_attr(feature = "python", pyo3(get))]
    pub lot_id: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub part_type: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub job_name: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub job_rev: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub start_time: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub finish_time: i64,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub tester_type: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub operator: String,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub test_code: String,

    #[cfg_attr(feature = "python", pyo3(get))]
    pub wafers: Vec<WaferData>,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub parts: Vec<PartData>,
    #[cfg_attr(feature = "python", pyo3(get))]
    pub test_results: Vec<TestResult>,

    // Not exposed to Python directly
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

#[cfg(feature = "python")]
#[pymethods]
impl StdfData {
    /// Number of unique test definitions.
    #[getter]
    fn test_count(&self) -> usize {
        self.tests.len()
    }
}
