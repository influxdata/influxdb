use http::request::Parts;
use kube_core::ApiResource;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default, Clone)]
pub struct Request {
    pub verb: String,
    pub group: String,
    pub version: String,
    pub plural: String,
    pub ns: Option<String>,
    pub name: Option<String>,
    pub subresource: Option<String>,
}

impl Request {
    pub(crate) fn parse(parts: &Parts) -> Self {
        let verb = parts.method.as_str().to_lowercase();
        let (group, version, plural, ns, name, subresource) = match parts
            .uri
            .path()
            .split('/')
            .skip(1)
            .collect::<Vec<&str>>()
            .as_slice()
        {
            ["api", "v1", plural] => ("", "v1", *plural, "", "", ""),
            ["api", "v1", plural, name] => ("", "v1", *plural, "", *name, ""),
            ["api", "v1", "namespaces", ns, plural] => ("", "v1", *plural, *ns, "", ""),
            ["api", "v1", "namespaces", ns, plural, name] => ("", "v1", *plural, *ns, *name, ""),
            ["api", "v1", "namespaces", ns, plural, name, subresource] => {
                ("", "v1", *plural, *ns, *name, *subresource)
            }
            ["api", "v1", plural, name, subresource] => {
                ("", "v1", *plural, "", *name, *subresource)
            }
            ["apis", group, version, "namespaces", ns, plural] => {
                (*group, *version, *plural, *ns, "", "")
            }
            ["apis", group, version, "namespaces", ns, plural, name] => {
                (*group, *version, *plural, *ns, *name, "")
            }
            ["apis", group, version, "namespaces", ns, plural, name, subresource] => {
                (*group, *version, *plural, *ns, *name, *subresource)
            }
            ["apis", group, version, plural] => (*group, *version, *plural, "", "", ""),
            ["apis", group, version, plural, name] => (*group, *version, *plural, "", *name, ""),
            ["apis", group, version, plural, name, subresource] => {
                (*group, *version, *plural, "", *name, *subresource)
            }
            _ => ("", "", "", "", "", ""),
        };

        let verb = match (verb.as_str(), name.len()) {
            ("get", 0) => String::from("list"),
            ("delete", 0) => String::from("deletecollection"),
            ("post", _) => String::from("create"),
            ("put", _) => String::from("update"),
            _ => verb,
        };

        Self {
            verb,
            group: String::from(group),
            version: String::from(version),
            plural: String::from(plural),
            ns: if ns.is_empty() {
                None
            } else {
                Some(String::from(ns))
            },
            name: if name.is_empty() {
                None
            } else {
                Some(String::from(name))
            },
            subresource: if subresource.is_empty() {
                None
            } else {
                Some(String::from(subresource))
            },
        }
    }

    pub fn api_plural(&self) -> ApiPlural {
        ApiPlural::new(self.group.clone(), self.plural.clone())
    }
}
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApiPlural {
    group: String,
    plural: String,
}

impl ApiPlural {
    pub fn new(group: String, plural: String) -> Self {
        Self { group, plural }
    }
}

impl Display for ApiPlural {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.group.is_empty() {
            self.plural.fmt(f)
        } else {
            write!(f, "{}/{}", self.group, self.plural)
        }
    }
}

impl From<ApiResource> for ApiPlural {
    fn from(value: ApiResource) -> Self {
        Self::new(value.group, value.plural)
    }
}
