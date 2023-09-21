//! Substituting dynamic values into a template as specified in various places
//! in the schema.

use crate::specification;
use chrono::prelude::*;
use handlebars::{
    Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext, RenderError,
};
use rand::rngs::SmallRng;
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng, RngCore};
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeMap, convert::TryInto};

/// Substitution-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while substituting values into templates.
#[derive(Snafu, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display(
        "Could not perform text substitution in `{}`, caused by:\n{}",
        template,
        source
    ))]
    CantCompileTemplate {
        #[snafu(source(from(handlebars::TemplateError, Box::new)))]
        source: Box<handlebars::TemplateError>,
        template: String,
    },

    #[snafu(display("Could not render template {}, caused by: {}", name, source))]
    CantRenderTemplate {
        name: String,
        #[snafu(source(from(handlebars::RenderError, Box::new)))]
        source: Box<handlebars::RenderError>,
    },

    #[snafu(display(
        "Could not perform text substitution in `{}`, caused by:\n{}",
        template,
        source
    ))]
    CantPerformSubstitution {
        #[snafu(source(from(handlebars::RenderError, Box::new)))]
        source: Box<handlebars::RenderError>,
        template: String,
    },
}

pub(crate) fn render_once(name: &str, template: impl Into<String>, data: &Value) -> Result<String> {
    let mut registry = new_handlebars_registry();
    registry.set_strict_mode(true);
    let template = template.into();
    registry
        .register_template_string(name, &template)
        .context(CantCompileTemplateSnafu { template })?;
    registry
        .render(name, data)
        .context(CantRenderTemplateSnafu { name })
}

pub(crate) fn new_handlebars_registry() -> Handlebars<'static> {
    let mut registry = Handlebars::new();
    registry.set_strict_mode(true);
    registry.register_helper("format-time", Box::new(FormatNowHelper));
    registry.register_helper("random", Box::new(RandomHelper));
    registry.register_helper("guid", Box::new(GuidHelper));
    registry
}

#[derive(Debug)]
pub(crate) struct RandomHelper;

impl HelperDef for RandomHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'_, '_>,
        _: &Handlebars<'_>,
        _: &Context,
        _: &mut RenderContext<'_, '_>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h
            .param(0)
            .ok_or_else(|| RenderError::new("`random` requires a parameter"))?
            .value()
            .as_u64()
            .ok_or_else(|| RenderError::new("`random`'s parameter must be an unsigned integer"))?
            .try_into()
            .map_err(|_| RenderError::new("`random`'s parameter must fit in a usize"))?;

        let mut rng = rand::thread_rng();

        let random: String = std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(param)
            .collect();

        out.write(&random)?;

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct FormatNowHelper;

impl HelperDef for FormatNowHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'_, '_>,
        _: &Handlebars<'_>,
        c: &Context,
        _: &mut RenderContext<'_, '_>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let format = h
            .param(0)
            .ok_or_else(|| RenderError::new("`format-time` requires a parameter"))?
            .render();

        let timestamp = c
            .data()
            .get("timestamp")
            .and_then(|t| t.as_i64())
            .expect("Caller of `render` should have set `timestamp` to an `i64` value");

        let datetime = Utc.timestamp_nanos(timestamp);

        out.write(&datetime.format(&format).to_string())?;

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct GuidHelper;

impl HelperDef for GuidHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        _h: &Helper<'_, '_>,
        _: &Handlebars<'_>,
        _: &Context,
        _: &mut RenderContext<'_, '_>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let mut rng = rand::thread_rng();

        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        let mut uid_builder = uuid::Builder::from_bytes(bytes);
        uid_builder.set_variant(uuid::Variant::RFC4122);
        uid_builder.set_version(uuid::Version::Random);
        let uid = uid_builder.into_uuid().to_string();

        out.write(&uid)?;

        Ok(())
    }
}

/// Given a random number generator and replacement specification, choose a
/// particular value from the list of possible values according to any specified
/// weights (or with equal probability if there are no weights).
pub fn pick_from_replacements<'a>(
    rng: &mut SmallRng,
    replacements: &'a [specification::Replacement],
) -> BTreeMap<&'a str, &'a str> {
    replacements
        .iter()
        .map(|replacement| {
            let chosen = replacement
                .with
                .choose_weighted(rng, |value| value.weight())
                .expect("`Replacement` `with` should have items")
                .value();

            (replacement.replace.as_str(), chosen)
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_now_valid_strftime() {
        let mut registry = new_handlebars_registry();
        registry
            .register_template_string("t", r#"the date is {{format-time "%Y-%m-%d"}}."#)
            .unwrap();

        let timestamp: i64 = 1599154445000000000;
        let value = registry
            .render("t", &json!({ "timestamp": timestamp }))
            .unwrap();

        assert_eq!(&value, "the date is 2020-09-03.");
    }

    #[test]
    #[should_panic(expected = "a Display implementation returned an error unexpectedly: Error")]
    fn format_now_invalid_strftime_panics() {
        let mut registry = new_handlebars_registry();
        registry
            .register_template_string("t", r#"the date is {{format-time "%-B"}}."#)
            .unwrap();

        let timestamp: i64 = 1599154445000000000;
        let _value = registry
            .render("t", &json!({ "timestamp": timestamp }))
            .expect("this is unreachable");
    }

    #[test]
    fn format_now_missing_strftime() {
        let mut registry = new_handlebars_registry();
        registry
            .register_template_string("t", r#"the date is {{format-time}}."#)
            .unwrap();

        let timestamp: i64 = 1599154445000000000;
        let result = registry.render("t", &json!({ "timestamp": timestamp }));

        assert!(result.is_err());
    }
}
