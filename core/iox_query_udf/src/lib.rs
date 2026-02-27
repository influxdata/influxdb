//! Bundled UDF payload.

use std::{collections::HashMap, sync::OnceLock};

use datafusion_udf_wasm_host::WasmComponentPrecompiled;
use datafusion_udf_wasm_query::{
    ComponentFn, Lang, UdfQueryParser, format::StripIndentationFormatter,
};

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use tokio as _;
use workspace_hack as _;

/// Python component.
///
/// Note that the returned [`WasmComponentPrecompiled`] is stateless and hence can (and should) be reused. State only
/// exists once it is turned into [`WasmScalarUdf`].
///
///
/// [`WasmScalarUdf`]: datafusion_udf_wasm_host::WasmScalarUdf
fn python_component() -> &'static WasmComponentPrecompiled {
    static COMPONENT: OnceLock<WasmComponentPrecompiled> = OnceLock::new();

    COMPONENT.get_or_init(|| {
        let data = include_bytes!(env!("BIN_PATH_PYTHON_WASM")).to_vec();

        // SAFETY: the binary was produced by our own build script
        let res = unsafe { WasmComponentPrecompiled::load(data) };

        res.unwrap()
    })
}

/// Pre-configured UDF parser.
pub fn udf_parser() -> UdfQueryParser<'static> {
    UdfQueryParser::new(HashMap::from([(
        "python".to_owned(),
        Lang {
            component: ComponentFn::eager(python_component()),
            formatter: Box::new(StripIndentationFormatter),
        },
    )]))
}
