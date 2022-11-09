//! Module for generating tag key/value pairs to be used in the data generator

use crate::specification::TagPairSpec;
use crate::substitution::new_handlebars_registry;
use handlebars::Handlebars;
use serde_json::{json, Value};
use snafu::{ResultExt, Snafu};
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

/// Results specific to the tag_pair module
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating or regenerating tag pairs
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "Could not compile template for tag pair {} caused by: {}",
        tag_key,
        source
    ))]
    CantCompileTemplate {
        tag_key: String,
        #[snafu(source(from(handlebars::TemplateError, Box::new)))]
        source: Box<handlebars::TemplateError>,
    },

    #[snafu(display(
        "Could not render template for tag pair {}, cause by: {}",
        tag_key,
        source
    ))]
    CantRenderTemplate {
        tag_key: String,
        #[snafu(source(from(handlebars::RenderError, Box::new)))]
        source: Box<handlebars::RenderError>,
    },
}

#[derive(Debug)]
pub enum TagPair {
    Static(StaticTagPair),
    Regenerating(Box<Mutex<RegeneratingTagPair>>),
}

impl TagPair {
    pub fn pairs_from_specs(
        specs: &[TagPairSpec],
        mut template_data: Value,
    ) -> Result<Vec<Arc<Self>>> {
        let tag_pairs: Vec<_> = specs
            .iter()
            .map(|tag_pair_spec| {
                let tag_count = tag_pair_spec.count.unwrap_or(1);

                let tags: Vec<_> = (1..tag_count + 1)
                    .map(|tag_id| {
                        let tag_key = if tag_id == 1 {
                            tag_pair_spec.key.to_string()
                        } else {
                            format!("{}{}", tag_pair_spec.key, tag_id)
                        };

                        let data = template_data.as_object_mut().expect("data must be object");
                        data.insert("id".to_string(), json!(tag_id));
                        data.insert("line_number".to_string(), json!(1));

                        let mut template = new_handlebars_registry();
                        template
                            .register_template_string(&tag_key, &tag_pair_spec.template)
                            .context(CantCompileTemplateSnafu {
                                tag_key: &tag_pair_spec.key,
                            })?;

                        let value = template
                            .render(&tag_key, &template_data)
                            .context(CantRenderTemplateSnafu { tag_key: &tag_key })?;

                        let tag_pair = StaticTagPair {
                            key: Arc::new(tag_key),
                            value: Arc::new(value),
                        };

                        let tag_pair = if let Some(regenerate_after_lines) =
                            tag_pair_spec.regenerate_after_lines
                        {
                            let regenerating_pair = RegeneratingTagPair {
                                regenerate_after_lines,
                                tag_pair,
                                template,
                                line_number: 0,
                                data: template_data.clone(),
                            };

                            Self::Regenerating(Box::new(Mutex::new(regenerating_pair)))
                        } else {
                            Self::Static(tag_pair)
                        };

                        Ok(Arc::new(tag_pair))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(tags)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(tag_pairs.into_iter().flatten().collect())
    }

    pub fn key(&self) -> String {
        match self {
            Self::Static(p) => p.key.to_string(),
            Self::Regenerating(p) => {
                let p = p.lock().expect("mutex poisoned");
                p.tag_pair.key.to_string()
            }
        }
    }
}

/// A tag key/value pair
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct StaticTagPair {
    /// the key
    pub key: Arc<String>,
    /// the value
    pub value: Arc<String>,
}

impl std::fmt::Display for StaticTagPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

/// Used for tag pairs specified in either an agent or measurement specification. The
/// spec must be kept around to support regenerating the tag pair.
#[derive(Debug, Clone)]
pub struct RegeneratingTagPair {
    regenerate_after_lines: usize,
    tag_pair: StaticTagPair,
    template: Handlebars<'static>,
    data: Value,
    line_number: usize,
}

impl RegeneratingTagPair {
    pub fn tag_pair(&mut self) -> &StaticTagPair {
        self.line_number += 1;

        if self.should_regenerate() {
            let data = self.data.as_object_mut().expect("data must be object");
            data.insert("line_number".to_string(), json!(self.line_number));

            let value = self
                .template
                .render(self.tag_pair.key.as_str(), &self.data)
                .expect("this template has been rendered before so this shouldn't be possible");

            self.tag_pair = StaticTagPair {
                key: Arc::clone(&self.tag_pair.key),
                value: Arc::new(value),
            };
        }

        &self.tag_pair
    }

    fn should_regenerate(&self) -> bool {
        self.line_number % (self.regenerate_after_lines + 1) == 0
    }
}
