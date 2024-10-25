use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use hashbrown::HashMap;
use serde::{
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Deserializer, Serialize, Serializer,
};

/// A new-type around a `HashMap` that provides special serialization and deserialization behaviour.
///
/// Specifically, it will be serialized as a vector of tuples, each tuple containing a key-value
/// pair from the map. Deserialization assumes said serialization, and deserializes from the vector
/// of tuples back into the map. Traits like `Deref`, `From`, etc. are implemented on this type such
/// that it can be used as a `HashMap`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SerdeVecHashMap<K: Eq + std::hash::Hash, V>(HashMap<K, V>);

impl<K, V, T> From<T> for SerdeVecHashMap<K, V>
where
    K: Eq + std::hash::Hash,
    T: Into<HashMap<K, V>>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl<K, V> Deref for SerdeVecHashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for SerdeVecHashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Serialize for SerdeVecHashMap<K, V>
where
    K: Eq + std::hash::Hash + Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for ele in self.iter() {
            seq.serialize_element(&ele)?;
        }
        seq.end()
    }
}

impl<'de, K, V> Deserialize<'de> for SerdeVecHashMap<K, V>
where
    K: Eq + std::hash::Hash + Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = deserializer.deserialize_seq(VecVisitor::new())?;
        Ok(Self(v.into_iter().collect()))
    }
}

type Output<K, V> = fn() -> Vec<(K, V)>;

struct VecVisitor<K, V> {
    marker: PhantomData<Output<K, V>>,
}

impl<K, V> VecVisitor<K, V> {
    fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<'de, K, V> Visitor<'de> for VecVisitor<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    type Value = Vec<(K, V)>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a vector of key value pairs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut v = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(ele) = seq.next_element()? {
            v.push(ele);
        }
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use super::SerdeVecHashMap;

    #[test]
    fn serde_vec_map_with_json() {
        let map = HashMap::<u32, &str>::from_iter([(0, "foo"), (1, "bar"), (2, "baz")]);
        let serde_vec_map = SerdeVecHashMap::from(map);
        // test round-trip to JSON:
        let s = serde_json::to_string(&serde_vec_map).unwrap();
        // with using a hashmap the order changes so asserting on the JSON itself is flaky, so if
        // you want to see it working use --nocapture on the test...
        println!("{s}");
        let d: SerdeVecHashMap<u32, &str> = serde_json::from_str(&s).unwrap();
        assert_eq!(d, serde_vec_map);
    }
}
