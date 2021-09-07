//! Substituting dynamic values into a template as specified in various places
//! in the schema.

use crate::{specification, DataGenRng, RandomNumberGenerator};
use chrono::prelude::*;
use handlebars::{
    Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext, RenderError,
};
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng};
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeMap, convert::TryInto, sync::Mutex};

/// Substitution-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while substituting values into templates.
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when substituting placeholder values
    #[snafu(display(
        "Could not perform text substitution in `{}`, caused by:\n{}",
        template,
        source
    ))]
    CantCompileTemplate {
        /// Underlying Handlebars error that caused this problem
        source: handlebars::TemplateError,
        /// Template that caused this problem
        template: String,
    },

    /// Error that may happen when substituting placeholder values
    #[snafu(display(
        "Could not perform text substitution in `{}`, caused by:\n{}",
        template,
        source
    ))]
    CantPerformSubstitution {
        /// Underlying Handlebars error that caused this problem
        source: handlebars::RenderError,
        /// Template that caused this problem
        template: String,
    },
}

#[derive(Debug)]
struct RandomHelper<T: DataGenRng>(Mutex<RandomNumberGenerator<T>>);

impl<T: DataGenRng> HelperDef for RandomHelper<T> {
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

        let rng = &mut *self.0.lock().expect("mutex poisoned");

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
struct FormatNowHelper;

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

/// Given a handlebars template containing placeholders within double curly
/// brackets like `{{placeholder}}` and a list of `(placeholder, substitution
/// value)` pairs, place the values in the template where the relevant
/// placeholder is.
#[derive(Debug)]
pub struct Substitute {
    handlebars: Handlebars<'static>,
    template: String,
}

impl Substitute {
    /// Compile and evaluate a template once. If you need to evaluate
    /// it multiple times, construct an instance via [`new`](Self::new).
    ///
    /// If a placeholder appears in a template but not in the list of
    /// substitution values, this will return an error.
    pub fn once(template: &str, values: &[(&str, &str)]) -> Result<String> {
        let values = values
            .iter()
            .map(|&(k, v)| (k, v))
            .collect::<BTreeMap<_, _>>();
        let me = Self::new_minimal(template)?;
        me.evaluate(&values)
    }

    /// Compiles the handlebars template once, then allows reusing the
    /// template multiple times via [`evaluate`](Self::evaluate). If you don't need to
    /// reuse the template, you can use [`once`](Self::once).
    pub fn new<T: DataGenRng>(
        template: impl Into<String>,
        rng: RandomNumberGenerator<T>,
    ) -> Result<Self> {
        let mut me = Self::new_minimal(template)?;
        me.set_random_number_generator(rng);
        Ok(me)
    }

    fn new_minimal(template: impl Into<String>) -> Result<Self> {
        let template = template.into();

        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);

        handlebars.register_helper("format-time", Box::new(FormatNowHelper));

        handlebars
            .register_template_string("template", &template)
            .context(CantCompileTemplate {
                template: &template,
            })?;

        Ok(Self {
            handlebars,
            template,
        })
    }

    fn set_random_number_generator<T: DataGenRng>(&mut self, rng: RandomNumberGenerator<T>) {
        self.handlebars
            .register_helper("random", Box::new(RandomHelper(Mutex::new(rng))));
    }

    /// Interpolates the values into the compiled template.
    ///
    /// If a placeholder appears in a template but not in the list of
    /// substitution values, this will return an error.
    pub fn evaluate(&self, values: &impl Serialize) -> Result<String> {
        self.handlebars
            .render("template", &values)
            .context(CantPerformSubstitution {
                template: &self.template,
            })
    }
}

/// Given a random number generator and replacement specification, choose a
/// particular value from the list of possible values according to any specified
/// weights (or with equal probability if there are no weights).
pub fn pick_from_replacements<'a, T: DataGenRng>(
    rng: &mut RandomNumberGenerator<T>,
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
    use crate::test_rng;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[derive(Serialize)]
    struct TimestampArgs {
        timestamp: i64,
    }

    #[test]
    fn format_now_valid_strftime() -> Result {
        let rng = test_rng();
        let args = TimestampArgs {
            timestamp: 1599154445000000000,
        };

        let substitute =
            Substitute::new(r#"the date is {{format-time "%Y-%m-%d"}}."#, rng).unwrap();

        let value = substitute.evaluate(&args)?;

        assert_eq!(value, "the date is 2020-09-03.");

        Ok(())
    }

    #[test]
    #[should_panic(expected = "a Display implementation returned an error unexpectedly: Error")]
    fn format_now_invalid_strftime_panics() {
        let rng = test_rng();
        let args = TimestampArgs {
            timestamp: 1599154445000000000,
        };

        let substitute = Substitute::new(r#"the date is {{format-time "%-B"}}."#, rng).unwrap();

        substitute.evaluate(&args).expect("This is unreachable");
    }

    #[test]
    fn format_now_missing_strftime() -> Result {
        let rng = test_rng();
        let args = TimestampArgs {
            timestamp: 1599154445000000000,
        };

        let substitute = Substitute::new(r#"the date is {{format-time}}."#, rng).unwrap();

        let result = substitute.evaluate(&args);

        // TODO: better matching on the error
        assert!(result.is_err());

        Ok(())
    }
}
