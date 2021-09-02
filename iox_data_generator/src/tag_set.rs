use crate::specification::{DataSpec, ForEachValueTag, ValuesSpec};
use crate::substitution::{FormatNowHelper, RandomHelper};
use crate::RandomNumberGenerator;
use handlebars::Handlebars;
use itertools::Itertools;
use serde_json::json;
use snafu::{OptionExt, ResultExt, Snafu};
use std::fmt::Formatter;
use std::iter::Peekable;
use std::sync::Arc;
/// Module for pre-generated values and tag sets that can be used when generating samples from
/// agents.
use std::{collections::BTreeMap, sync::Mutex};

/// Errors that may happen while reading a TOML specification.
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("{} specifies a has_one member {} that isn't defined", value, has_one))]
    HasOneDependencyNotDefined { value: String, has_one: String },

    /// Error that may happen when compiling a template from the values specification
    #[snafu(display("Could not compile template `{}`, caused by:\n{}", template, source))]
    CantCompileTemplate {
        /// Underlying Handlebars error that caused this problem
        source: handlebars::TemplateError,
        /// Template that caused this problem
        template: String,
    },

    /// Error that may happen when rendering a template with passed in data
    #[snafu(display("Could not render template `{}`, caused by:\n{}", template, source))]
    CantRenderTemplate {
        /// Underlying Handlebars error that caused this problem
        source: handlebars::RenderError,
        /// Template that caused this problem
        template: String,
    },

    #[snafu(display(
        "has_one {} must be accessed through its parent (e.g. parent foo with has_one bar: foo.bar",
        has_one
    ))]
    HasOneWithoutParent { has_one: String },

    #[snafu(display("no has_one found values for {}", has_one))]
    HasOneNotFound { has_one: String },

    #[snafu(display("has_one {} not found for parent id {}", has_one, parent_id))]
    HasOneNotFoundForParent { has_one: String, parent_id: usize },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct GeneratedValueCollection {
    name: String,
    values: Vec<GeneratedValue>,
}

#[derive(Debug)]
pub struct GeneratedValue {
    value: Arc<String>,
    id: usize,
}

#[derive(Debug, Default)]
pub struct GeneratedTagSets {
    values: BTreeMap<String, Vec<Arc<GeneratedValue>>>,
    // each parent-child will have its children stored in this map. The children map
    // the id of the parent to the collection of its children values
    child_values: BTreeMap<String, BTreeMap<usize, Vec<Arc<GeneratedValue>>>>,
    // each parent-has_one will have its has_ones stored in this map
    has_one_values: BTreeMap<String, ParentToHasOnes>,
    tag_sets: BTreeMap<String, Vec<TagSet>>,
}

#[derive(Debug, Default)]
pub struct ParentToHasOnes {
    // each parent id will have its has_ones stored in this map. The map within
    // maps the has_one name to its generated value
    id_to_has_ones: BTreeMap<usize, BTreeMap<Arc<String>, Arc<GeneratedValue>>>,
}

#[derive(Debug)]
pub struct TagSetKey {
    value: String,
    tag_key: Arc<String>,
}

impl GeneratedTagSets {
    #[allow(dead_code)]
    pub fn from_spec(spec: &DataSpec) -> Result<Self> {
        let mut generated_tag_sets = Self::default();

        let rng: RandomNumberGenerator<rand::rngs::SmallRng> = RandomNumberGenerator::new(
            spec.base_seed
                .to_owned()
                .unwrap_or_else(|| "default seed".to_string()),
        );
        let random_helper = RandomHelper::new(Mutex::new(rng));
        let mut template = Handlebars::new();
        template.register_helper("format-time", Box::new(FormatNowHelper));
        template.register_helper("random", Box::new(random_helper));

        let mut leftover_specs = -1;

        loop {
            if leftover_specs == 0 {
                break;
            }

            let new_leftover = generated_tag_sets.generate_values(&mut template, spec)? as i64;
            if new_leftover == leftover_specs {
                panic!("unresolvable loop in values generation");
            }
            leftover_specs = new_leftover;
        }

        generated_tag_sets.generate_tag_sets(spec)?;

        Ok(generated_tag_sets)
    }

    #[allow(dead_code)]
    pub fn sets_for(&self, name: &str) -> Option<&Vec<TagSet>> {
        self.tag_sets.get(name)
    }

    fn generate_values(
        &mut self,
        registry: &mut Handlebars<'static>,
        data_spec: &DataSpec,
    ) -> Result<usize> {
        let mut leftover_count = 0;

        for spec in &data_spec.values {
            if self.values.contains_key(&spec.name) {
                continue;
            } else if !self.can_generate(spec) {
                leftover_count += 1;
                continue;
            }

            self.generate_values_spec(registry, spec)?;
        }

        Ok(leftover_count)
    }

    fn generate_tag_sets(&mut self, data_spec: &DataSpec) -> Result<()> {
        for set_spec in &data_spec.tag_sets {
            self.generate_tag_set_spec(&set_spec.name, &set_spec.for_each)?;
        }

        Ok(())
    }

    fn generate_tag_set_spec(
        &mut self,
        set_name: &str,
        for_each: &[ForEachValueTag],
    ) -> Result<()> {
        let tag_set_keys: Vec<_> = for_each
            .iter()
            .map(|v| TagSetKey {
                value: v.value.to_string(),
                tag_key: Arc::new(v.tag_key.to_string()),
            })
            .collect();
        let mut keys = tag_set_keys.iter().peekable();

        let tag_sets = self.for_each_tag_set(None, &mut keys, &[])?;
        self.tag_sets.insert(set_name.to_string(), tag_sets);

        Ok(())
    }

    fn for_each_tag_set(
        &self,
        parent_id: Option<usize>,
        keys: &mut Peekable<core::slice::Iter<'_, TagSetKey>>,
        tag_pairs: &[Arc<TagPair>],
    ) -> Result<Vec<TagSet>> {
        let key = keys
            .next()
            .expect("for_each_tag_set should never be called without a next key");

        match self.get_generated_values(parent_id, &key.value) {
            Some(values) => {
                if keys.peek().is_none() {
                    let mut tag_sets = Vec::with_capacity(values.len());

                    for v in values {
                        let mut tag_pairs = tag_pairs.to_vec();
                        tag_pairs.push(Arc::new(TagPair {
                            key: Arc::clone(&key.tag_key),
                            value: Arc::clone(&v.value),
                        }));
                        tag_sets.push(TagSet::new(tag_pairs));
                    }

                    return Ok(tag_sets);
                }

                let mut tag_sets = vec![];

                for v in values {
                    let pair = TagPair {
                        key: Arc::clone(&key.tag_key),
                        value: Arc::clone(&v.value),
                    };
                    let mut pairs = tag_pairs.to_vec();
                    pairs.push(Arc::new(pair));
                    let mut sets = self.for_each_tag_set(Some(v.id), &mut keys.clone(), &pairs)?;
                    tag_sets.append(&mut sets);
                }

                Ok(tag_sets)
            }
            None => {
                let parent_id = parent_id.expect("for_each_tag_set should never be called without a parent id if in has_one evaluation");
                let one_key = key
                    .value
                    .split('.')
                    .last()
                    .context(HasOneWithoutParent {
                        has_one: &key.value,
                    })?
                    .to_string();
                let one = self
                    .has_one_values
                    .get(&key.value)
                    .context(HasOneNotFound {
                        has_one: &key.value,
                    })?
                    .id_to_has_ones
                    .get(&parent_id)
                    .context(HasOneNotFoundForParent {
                        has_one: &key.value,
                        parent_id,
                    })?
                    .get(&one_key)
                    .expect("bug in generating values for has_one");
                let tag = Arc::new(TagPair {
                    key: Arc::clone(&key.tag_key),
                    value: Arc::clone(&one.value),
                });
                let mut tags = tag_pairs.to_vec();
                tags.push(tag);

                match keys.peek() {
                    Some(_) => self.for_each_tag_set(Some(parent_id), &mut keys.clone(), &tags),
                    None => Ok(vec![TagSet::new(tags)]),
                }
            }
        }
    }

    fn get_generated_values(
        &self,
        parent_id: Option<usize>,
        key: &str,
    ) -> Option<&Vec<Arc<GeneratedValue>>> {
        match self.child_values.get(key) {
            Some(child_values) => child_values.get(&parent_id.expect(
                "should never get_get_generated_values for child values without a parent_id",
            )),
            None => self.values.get(key),
        }
    }

    fn can_generate(&self, spec: &ValuesSpec) -> bool {
        match (&spec.has_one, &spec.belongs_to) {
            (None, None) => true,
            (None, Some(b)) => self.values.contains_key(b),
            (Some(has_ones), None) => {
                for name in has_ones {
                    if !self.values.contains_key(name) {
                        return false;
                    }
                }

                true
            }
            (Some(has_ones), Some(b)) => {
                for name in has_ones {
                    if !self.values.contains_key(name) {
                        return false;
                    }
                }

                self.values.contains_key(b)
            }
        }
    }

    fn generate_values_spec(
        &mut self,
        template: &mut Handlebars<'static>,
        spec: &ValuesSpec,
    ) -> Result<()> {
        template
            .register_template_string(&spec.name, &spec.value)
            .context(CantCompileTemplate {
                template: &spec.name,
            })?;

        match &spec.belongs_to {
            Some(belongs_to) => self.generate_belongs_to(template, belongs_to.as_str(), spec)?,
            None => {
                let mut vals = Vec::with_capacity(spec.cardinality);
                let mut id_map = BTreeMap::new();
                for i in 1..(spec.cardinality + 1) {
                    id_map.insert("id", i);
                    let rendered_value =
                        template
                            .render(&spec.name, &id_map)
                            .context(CantRenderTemplate {
                                template: &spec.name,
                            })?;

                    vals.push(Arc::new(GeneratedValue {
                        id: i,
                        value: Arc::new(rendered_value),
                    }));
                }
                self.values.insert(spec.name.to_string(), vals);
            }
        }

        if let Some(has_ones) = spec.has_one.as_ref() {
            self.add_has_ones(&spec.name, has_ones)?;
        }

        Ok(())
    }

    fn add_has_ones(&mut self, parent: &str, has_ones: &[String]) -> Result<()> {
        let parent_values = self
            .values
            .get(parent)
            .expect("add_has_ones should never be called before the parent values are inserted");

        for has_one in has_ones {
            let parent_has_one_key = has_one_values_key(parent, has_one);
            let parent_has_ones = self
                .has_one_values
                .entry(parent_has_one_key)
                .or_insert_with(ParentToHasOnes::default);

            let has_one = Arc::new(has_one.to_string());

            let has_one_values = self.values.get(has_one.as_ref()).expect(
                "add_has_ones should never be called before the values collection is created",
            );

            let mut ones_iter = has_one_values.iter();
            for parent in parent_values {
                let one_val = ones_iter.next().unwrap_or_else(|| {
                    ones_iter = has_one_values.iter();
                    ones_iter.next().unwrap()
                });

                let has_one_map = parent_has_ones
                    .id_to_has_ones
                    .entry(parent.id)
                    .or_insert_with(BTreeMap::new);
                has_one_map.insert(Arc::clone(&has_one), Arc::clone(one_val));
            }
        }

        Ok(())
    }

    fn generate_belongs_to(
        &mut self,
        template: &mut Handlebars<'static>,
        belongs_to: &str,
        spec: &ValuesSpec,
    ) -> Result<()> {
        let parent_values = self.values.get(belongs_to).expect(
            "generate_belongs_to should never be called before the parent values are inserted",
        );

        let mut all_children = Vec::with_capacity(parent_values.len() * spec.cardinality);

        for parent in parent_values {
            let mut parent_owned = Vec::with_capacity(spec.cardinality);

            for _ in 0..spec.cardinality {
                let child_value_id = all_children.len() + 1;
                let data = json!({
                    belongs_to: {
                        "id": parent.id,
                        "value": &parent.value,
                    },
                    "id": child_value_id,
                });

                let rendered_value =
                    template
                        .render(&spec.name, &data)
                        .context(CantRenderTemplate {
                            template: &spec.name,
                        })?;
                let child_value = Arc::new(GeneratedValue {
                    id: child_value_id,
                    value: Arc::new(rendered_value),
                });

                parent_owned.push(Arc::clone(&child_value));
                all_children.push(child_value);
            }

            let child_vals = self
                .child_values
                .entry(child_values_key(belongs_to, &spec.name))
                .or_insert_with(BTreeMap::new);
            child_vals.insert(parent.id, parent_owned);
        }
        self.values.insert(spec.name.to_string(), all_children);

        Ok(())
    }
}

fn child_values_key(parent: &str, child: &str) -> String {
    format!("{}.{}", parent, child)
}

fn has_one_values_key(parent: &str, child: &str) -> String {
    format!("{}.{}", parent, child)
}

#[derive(Debug)]
pub struct TagSet {
    pub tags: Vec<Arc<TagPair>>,
}

impl TagSet {
    fn new(tags: Vec<Arc<TagPair>>) -> Self {
        let mut tags = tags;
        tags.sort_unstable_by(|a, b| a.key.partial_cmp(&b.key).unwrap());
        Self { tags }
    }
}

impl std::fmt::Display for TagSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.tags.iter().map(|t| t.to_string()).join(",");
        write!(f, "{}", s)
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct TagPair {
    key: Arc<String>,
    value: Arc<String>,
}

impl std::fmt::Display for TagPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn generate_tag_sets_basic() {
        let toml = r#"
name = "demo"
base_seed = "foo"

[[values]]
name = "foo"
value = "{{id}}#foo"
cardinality = 3

[[tag_sets]]
name = "testage"
for_each = [
    {value = "foo", tag_key = "keyfoo"},
]

[[agents]]
name = "basic"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "f1"
i64_range = [0, 23]
"#;

        let spec = DataSpec::from_str(toml).unwrap();
        let tag_sets = GeneratedTagSets::from_spec(&spec).unwrap();
        let testage = tag_sets.sets_for("testage").unwrap();
        let sets = testage.iter().map(|t| t.to_string()).join("\n");
        let expected = r#"
keyfoo=1#foo
keyfoo=2#foo
keyfoo=3#foo"#;
        assert_eq!(expected[1..], sets);
    }

    #[test]
    fn generate_tag_sets_belongs_to() {
        let toml = r#"
name = "demo"
base_seed = "foo"

[[values]]
name = "foo"
value = "{{id}}#foo"
cardinality = 2

[[values]]
name = "bar"
value = "{{id}}-{{foo.id}}-{{foo.value}}"
cardinality = 2
belongs_to = "foo"

[[tag_sets]]
name = "testage"
for_each = [
    {value = "foo", tag_key = "foo"},
    {value = "foo.bar", tag_key = "bar"},
]

[[agents]]
name = "basic"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "f1"
i64_range = [0, 23]
"#;

        let spec = DataSpec::from_str(toml).unwrap();
        let tag_sets = GeneratedTagSets::from_spec(&spec).unwrap();
        let testage = tag_sets.sets_for("testage").unwrap();
        let sets = testage.iter().map(|t| t.to_string()).join("\n");
        let expected = r#"
bar=1-1-1#foo,foo=1#foo
bar=2-1-1#foo,foo=1#foo
bar=3-2-2#foo,foo=2#foo
bar=4-2-2#foo,foo=2#foo"#;
        assert_eq!(expected[1..], sets);
    }

    #[test]
    fn generate_tag_sets_test() {
        let toml = r#"
name = "demo"
base_seed = "foo"

[[values]]
name = "foo"
value = "{{id}}-foo"
cardinality = 3
has_one = ["bar"]

[[values]]
name = "bar"
value = "{{id}}-bar"
cardinality = 2

[[values]]
name = "asdf"
value = "{{id}}-asdf"
cardinality = 2
belongs_to = "foo"
has_one = ["qwer"]

[[values]]
name = "jkl"
value = "{{id}}-jkl"
cardinality = 2

[[values]]
name = "qwer"
value = "{{id}}-qwer"
cardinality = 6

[[tag_sets]]
name = "testage"
for_each = [
    {value = "foo", tag_key = "foo"},
    {value = "foo.bar", tag_key = "bar"},
    {value = "foo.asdf", tag_key = "asfd"},
    {value = "asdf.qwer", tag_key = "qwer"},
    {value = "jkl", tag_key = "jkl"}
]

[[agents]]
name = "basic"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "f1"
i64_range = [0, 23]
"#;

        let spec = DataSpec::from_str(toml).unwrap();
        let tag_sets = GeneratedTagSets::from_spec(&spec).unwrap();
        let testage = tag_sets.sets_for("testage").unwrap();
        let sets = testage.iter().map(|t| t.to_string()).join("\n");
        let expected = r#"
asfd=1-asdf,bar=1-bar,foo=1-foo,jkl=1-jkl,qwer=1-qwer
asfd=1-asdf,bar=1-bar,foo=1-foo,jkl=2-jkl,qwer=1-qwer
asfd=2-asdf,bar=1-bar,foo=1-foo,jkl=1-jkl,qwer=2-qwer
asfd=2-asdf,bar=1-bar,foo=1-foo,jkl=2-jkl,qwer=2-qwer
asfd=3-asdf,bar=2-bar,foo=2-foo,jkl=1-jkl,qwer=3-qwer
asfd=3-asdf,bar=2-bar,foo=2-foo,jkl=2-jkl,qwer=3-qwer
asfd=4-asdf,bar=2-bar,foo=2-foo,jkl=1-jkl,qwer=4-qwer
asfd=4-asdf,bar=2-bar,foo=2-foo,jkl=2-jkl,qwer=4-qwer
asfd=5-asdf,bar=1-bar,foo=3-foo,jkl=1-jkl,qwer=5-qwer
asfd=5-asdf,bar=1-bar,foo=3-foo,jkl=2-jkl,qwer=5-qwer
asfd=6-asdf,bar=1-bar,foo=3-foo,jkl=1-jkl,qwer=6-qwer
asfd=6-asdf,bar=1-bar,foo=3-foo,jkl=2-jkl,qwer=6-qwer"#;
        assert_eq!(expected[1..], sets);
    }
}
