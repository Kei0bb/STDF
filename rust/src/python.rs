//! PyO3 bindings — converts Rust StdfData to Python dicts.

use pyo3::prelude::*;
use pyo3::exceptions::PyIOError;
use pyo3::types::PyDict;

use crate::parser;

/// Convert a WaferData to a Python dict.
fn wafer_to_dict(py: Python<'_>, w: &crate::types::WaferData) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("wafer_id", &w.wafer_id)?;
    d.set_item("lot_id", &w.lot_id)?;
    d.set_item("head_num", w.head_num)?;
    d.set_item("start_time", w.start_time)?;
    d.set_item("finish_time", w.finish_time)?;
    d.set_item("part_count", w.part_count)?;
    d.set_item("good_count", w.good_count)?;
    d.set_item("rtst_count", w.rtst_count)?;
    d.set_item("abrt_count", w.abrt_count)?;
    Ok(d.into())
}

/// Convert a PartData to a Python dict.
fn part_to_dict(py: Python<'_>, p: &crate::types::PartData) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("part_id", &p.part_id)?;
    d.set_item("lot_id", &p.lot_id)?;
    d.set_item("wafer_id", &p.wafer_id)?;
    d.set_item("head_num", p.head_num)?;
    d.set_item("site_num", p.site_num)?;
    d.set_item("x_coord", p.x_coord)?;
    d.set_item("y_coord", p.y_coord)?;
    d.set_item("hard_bin", p.hard_bin)?;
    d.set_item("soft_bin", p.soft_bin)?;
    d.set_item("passed", p.passed)?;
    d.set_item("test_count", p.test_count)?;
    d.set_item("test_time", p.test_time)?;
    Ok(d.into())
}

/// Convert a TestResult to a Python dict.
fn test_result_to_dict(py: Python<'_>, t: &crate::types::TestResult) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("lot_id", &t.lot_id)?;
    d.set_item("part_id", &t.part_id)?;
    d.set_item("wafer_id", &t.wafer_id)?;
    d.set_item("x_coord", t.x_coord)?;
    d.set_item("y_coord", t.y_coord)?;
    d.set_item("test_num", t.test_num)?;
    d.set_item("test_name", &t.test_name)?;
    d.set_item("rec_type", &t.rec_type)?;
    d.set_item("lo_limit", if t.lo_limit.is_nan() { None } else { Some(t.lo_limit) })?;
    d.set_item("hi_limit", if t.hi_limit.is_nan() { None } else { Some(t.hi_limit) })?;
    d.set_item("units", &t.units)?;
    d.set_item("result", if t.result.is_nan() { None } else { Some(t.result) })?;
    d.set_item("passed", t.passed)?;
    Ok(d.into())
}

/// Convert a TestDef to a Python dict.
fn test_def_to_dict(py: Python<'_>, t: &crate::types::TestDef) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("test_num", t.test_num)?;
    d.set_item("test_name", &t.test_name)?;
    d.set_item("rec_type", &t.rec_type)?;
    d.set_item("lo_limit", if t.lo_limit.is_nan() { None } else { Some(t.lo_limit) })?;
    d.set_item("hi_limit", if t.hi_limit.is_nan() { None } else { Some(t.hi_limit) })?;
    d.set_item("units", &t.units)?;
    Ok(d.into())
}

/// Convert a BinData to a Python dict.
fn bin_to_dict(py: Python<'_>, b: &crate::types::BinData) -> PyResult<Py<PyDict>> {
    let d = PyDict::new(py);
    d.set_item("bin_num", b.bin_num)?;
    d.set_item("bin_count", b.bin_count)?;
    d.set_item("bin_name", &b.bin_name)?;
    d.set_item("bin_pf", &b.bin_pf)?;
    Ok(d.into())
}

/// Parse an STDF file and return a dict matching Python STDFData fields.
///
/// Supports both `.stdf` and `.stdf.gz` files.
#[pyfunction]
fn parse_stdf(py: Python<'_>, path: &str) -> PyResult<Py<PyDict>> {
    let data = parser::parse_stdf(path)
        .map_err(|e| PyIOError::new_err(e.to_string()))?;

    let result = PyDict::new(py);
    result.set_item("lot_id", &data.lot_id)?;
    result.set_item("part_type", &data.part_type)?;
    result.set_item("job_name", &data.job_name)?;
    result.set_item("job_rev", &data.job_rev)?;
    result.set_item("start_time", data.start_time)?;
    result.set_item("finish_time", data.finish_time)?;
    result.set_item("tester_type", &data.tester_type)?;
    result.set_item("operator", &data.operator)?;
    result.set_item("test_code", &data.test_code)?;

    // Wafers
    let wafers: Vec<Py<PyDict>> = data.wafers.iter()
        .map(|w| wafer_to_dict(py, w))
        .collect::<PyResult<_>>()?;
    result.set_item("wafers", wafers)?;

    // Parts
    let parts: Vec<Py<PyDict>> = data.parts.iter()
        .map(|p| part_to_dict(py, p))
        .collect::<PyResult<_>>()?;
    result.set_item("parts", parts)?;

    // Test results
    let test_results: Vec<Py<PyDict>> = data.test_results.iter()
        .map(|t| test_result_to_dict(py, t))
        .collect::<PyResult<_>>()?;
    result.set_item("test_results", test_results)?;

    // Tests (definitions)
    let tests = PyDict::new(py);
    for (k, v) in &data.tests {
        tests.set_item(k, test_def_to_dict(py, v)?)?;
    }
    result.set_item("tests", tests)?;

    // Bins
    let bins_hard = PyDict::new(py);
    for (k, v) in &data.bins_hard {
        bins_hard.set_item(k, bin_to_dict(py, v)?)?;
    }
    result.set_item("bins_hard", bins_hard)?;

    let bins_soft = PyDict::new(py);
    for (k, v) in &data.bins_soft {
        bins_soft.set_item(k, bin_to_dict(py, v)?)?;
    }
    result.set_item("bins_soft", bins_soft)?;

    Ok(result.into())
}

/// Python module: stdf2pq_rs
#[pymodule]
fn stdf2pq_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_stdf, m)?)?;
    Ok(())
}
