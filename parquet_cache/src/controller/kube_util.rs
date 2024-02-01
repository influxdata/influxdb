use fnv::FnvHasher;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::{Api, Error, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hasher;

/// The set of characters kubernetes considers safe for generated strings.
const SAFE_CHARS: [char; 27] = [
    'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x',
    'z', '2', '4', '5', '6', '7', '8', '9',
];

/// Encode a string using a small character set that is considered safe. This
/// minimizes the chances of accidental vulgarity.
pub fn safe_string(s: &str) -> String {
    s.chars()
        .map(|c| SAFE_CHARS[c as usize % SAFE_CHARS.len()])
        .collect()
}

/// Get a hash value for the provided object. The hashed value has no guaranteed properties
/// other than the same input will have the same resulting hash. There is no attempt made to
/// hash the value in the same way that kubernetes controllers will.
pub fn hash_object<T>(obj: &T) -> Result<String, serde_json::Error>
where
    T: ?Sized + Serialize,
{
    let bytes = serde_json::to_vec(obj)?;
    let mut hasher = FnvHasher::with_key(0);
    hasher.write(&bytes);
    Ok(safe_string(&format!(
        "{}",
        (hasher.finish() & 0xFFFFFFFF) as u32
    )))
}

/// Format label selectors so they can be used with ListParams.
pub fn selectors(selector: &LabelSelector) -> Option<String> {
    let mut clauses = vec![];
    if let Some(expressions) = &selector.match_expressions {
        clauses.extend(expressions.iter().filter_map(|requirement| {
            match requirement.operator.as_ref() {
                "In" => requirement
                    .values
                    .as_ref()
                    .map(|values| format!("{} in ({})", requirement.key, values.join(","))),
                "NotIn" => requirement
                    .values
                    .as_ref()
                    .map(|values| format!("{} notin ({})", requirement.key, values.join(","))),
                "Exists" => Some(requirement.key.clone()),
                "DoesNotExist" => Some(format!("!{}", requirement.key)),
                _ => None, // Skip unknown operator.
            }
        }));
    }
    if let Some(labels) = &selector.match_labels {
        clauses.extend(labels.iter().map(|(k, v)| format!("{k}={v}")))
    }
    match clauses.len() {
        0 => None,
        _ => Some(clauses.join(",")),
    }
}

pub async fn list_owned<K>(api: &Api<K>, owner_uid: &String) -> Result<Vec<K>, Error>
where
    K: Debug + Clone + Resource + DeserializeOwned + Send + Sync + 'static,
{
    let object_list = api.list(&Default::default()).await?;
    Ok(object_list
        .items
        .into_iter()
        .filter(|obj| obj.owner_references().iter().any(|or| &or.uid == owner_uid))
        .collect())
}

pub fn owner_reference<R>(obj: &R) -> OwnerReference
where
    R: Resource<DynamicType = ()>,
{
    let meta = obj.meta();
    OwnerReference {
        api_version: R::api_version(&()).into(),
        block_owner_deletion: Some(true),
        controller: Some(true),
        kind: R::kind(&()).into(),
        name: meta.name.clone().unwrap_or_default(),
        uid: meta.uid.clone().unwrap_or_default(),
    }
}
