//! stdf2pq-rs: High-performance STDF binary parser with optional Python bindings.

pub mod reader;
pub mod parser;
pub mod types;

#[cfg(feature = "python")]
mod python;
