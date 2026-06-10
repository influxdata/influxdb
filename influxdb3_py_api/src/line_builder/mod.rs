use std::ffi::CString;

use pyo3::{Python, types::PyAnyMethods};

const LINE_BUILDER_CODE: &str = include_str!("line_builder.py");

/// Sets up the LineBuilder class in Python's environment for access by all plugins.
///
/// Executes the LineBuilder code in `__main__` and adds it to builtins so it's accessible
/// from both single-file plugins and multi-file plugins.
pub(crate) fn setup_line_builder(py: Python<'_>) -> Result<(), anyhow::Error> {
    // specifically use globals = None which defaults to __main__ so LineBuilder is
    // accessible to all plugins
    py.run(&CString::new(LINE_BUILDER_CODE).unwrap(), None, None)
        .map_err(|e| anyhow::Error::new(e).context("failed to eval the LineBuilder API code"))?;

    // Make LineBuilder available to all modules by adding it to builtins.
    // This ensures multi-file plugins can access it without explicit imports.
    let builtins = py.import("builtins")?;
    let main_module = py.import("__main__")?;
    let line_builder = main_module.getattr("LineBuilder")?;
    builtins.setattr("LineBuilder", line_builder)?;

    Ok(())
}
