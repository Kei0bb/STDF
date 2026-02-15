//! PyO3 bindings — exposes `parse_stdf_rs()` to Python.

use pyo3::prelude::*;
use pyo3::exceptions::PyIOError;

use crate::parser;
use crate::types::{StdfData, WaferData, PartData, TestResult};

/// Parse an STDF file and return structured data.
///
/// Supports both `.stdf` and `.stdf.gz` files.
#[pyfunction]
fn parse_stdf_rs(path: &str) -> PyResult<StdfData> {
    parser::parse_stdf(path).map_err(|e| PyIOError::new_err(e.to_string()))
}

/// Python module: stdf2pq_rs
#[pymodule]
fn stdf2pq_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_stdf_rs, m)?)?;
    m.add_class::<StdfData>()?;
    m.add_class::<WaferData>()?;
    m.add_class::<PartData>()?;
    m.add_class::<TestResult>()?;
    Ok(())
}
