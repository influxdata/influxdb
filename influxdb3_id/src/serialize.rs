use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use indexmap::{
    IndexMap, IndexSet,
    map::{IntoIter, Iter, IterMut},
    set::{IntoIter as SetIntoIter, Iter as SetIter},
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
    ser::SerializeSeq,
};

/// A new-type around a [`IndexMap`] that provides special serialization and deserialization behaviour.
///
/// Specifically, it will be serialized as a vector of tuples, each tuple containing a key-value
/// pair from the map. Deserialization assumes said serialization, and deserializes from the vector
/// of tuples back into the map. Traits like `Deref`, `From`, etc. are implemented on this type such
/// that it can be used as a `IndexMap`.
///
/// During deserialization, there are no duplicate keys allowed. If duplicates are found, an error
/// will be thrown.
///
/// The `IndexMap` type is used to preserve insertion, and thereby iteration order. This ensures
/// consistent ordering of entities when this map is iterated over, for e.g., column ordering in
/// queries, or entity ordering during serialization. Since `IndexMap` stores key/value pairs in a
/// contiguous vector, iterating over its members is faster than a `HashMap`. This is beneficial for
/// WAL serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerdeVecMap<K: Eq + std::hash::Hash, V>(IndexMap<K, V>);

impl<K, V> SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(size: usize) -> Self {
        Self(IndexMap::with_capacity(size))
    }
}

impl<K, V> Default for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K, V, T> From<T> for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
    T: Into<IndexMap<K, V>>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl<K, V> IntoIterator for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    type Item = (K, V);

    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    type Item = (&'a K, &'a V);

    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, K, V> IntoIterator for &'a mut SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    type Item = (&'a K, &'a mut V);

    type IntoIter = IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<K, V> FromIterator<(K, V)> for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<K, V> Deref for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    type Target = IndexMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Serialize for SerdeVecMap<K, V>
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

impl<'de, K, V> Deserialize<'de> for SerdeVecMap<K, V>
where
    K: Eq + std::hash::Hash + Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = deserializer.deserialize_seq(VecVisitor::new())?;
        let mut map = IndexMap::with_capacity(v.len());
        for (k, v) in v.into_iter() {
            if map.insert(k, v).is_some() {
                return Err(de::Error::custom("duplicate key found"));
            }
        }
        Ok(Self(map))
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

/// A new-type around a [`IndexSet`] that provides special serialization and deserialization behaviour.
///
/// Specifically, it will be serialized as a vector, preserving the insertion order of the set.
/// Deserialization assumes said serialization, and deserializes from the vector back into the set.
/// Traits like `Deref`, `From`, etc. are implemented on this type such that it can be used as an
/// `IndexSet`.
///
/// During deserialization, there are no duplicate values allowed. If duplicates are found, they will
/// be silently ignored (only the first occurrence is kept).
///
/// The `IndexSet` type is used to preserve insertion order and provide O(1) contains() queries.
/// Since `IndexSet` stores values in a contiguous vector, iterating over its members is faster than
/// a `HashSet`. This is beneficial for WAL serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerdeVecSet<T: Eq + std::hash::Hash>(IndexSet<T>);

impl<T> SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(size: usize) -> Self {
        Self(IndexSet::with_capacity(size))
    }
}

impl<T> Default for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T, U> From<U> for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
    U: Into<IndexSet<T>>,
{
    fn from(value: U) -> Self {
        Self(value.into())
    }
}

impl<T> IntoIterator for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    type Item = T;

    type IntoIter = SetIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    type Item = &'a T;

    type IntoIter = SetIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> FromIterator<T> for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<T> Deref for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    type Target = IndexSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Serialize for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash + Serialize,
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

impl<'de, T> Deserialize<'de> for SerdeVecSet<T>
where
    T: Eq + std::hash::Hash + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = deserializer.deserialize_seq(SetVecVisitor::new())?;
        let set = IndexSet::from_iter(v);
        Ok(Self(set))
    }
}

type SetOutput<T> = fn() -> Vec<T>;

struct SetVecVisitor<T> {
    marker: PhantomData<SetOutput<T>>,
}

impl<T> SetVecVisitor<T> {
    fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<'de, T> Visitor<'de> for SetVecVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a vector of values")
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
    use indexmap::{IndexMap, IndexSet};

    use super::{SerdeVecMap, SerdeVecSet};

    #[test]
    fn serde_vec_map_with_json() {
        let map = IndexMap::<u32, &str>::from_iter([(0, "foo"), (1, "bar"), (2, "baz")]);
        let serde_vec_map = SerdeVecMap::from(map);
        // test round-trip to JSON:
        let s = serde_json::to_string(&serde_vec_map).unwrap();
        assert_eq!(r#"[[0,"foo"],[1,"bar"],[2,"baz"]]"#, s);
        let d: SerdeVecMap<u32, &str> = serde_json::from_str(&s).unwrap();
        assert_eq!(d, serde_vec_map);
    }

    #[test]
    fn test_no_duplicates() {
        let json_str = r#"[[0, "foo"], [0, "bar"]]"#;
        let err = serde_json::from_str::<SerdeVecMap<u8, &str>>(json_str).unwrap_err();
        assert!(err.to_string().contains("duplicate key found"));
    }

    #[test]
    fn serde_vec_set_with_json() {
        let set = IndexSet::<u32>::from_iter([0, 1, 2]);
        let serde_vec_set = SerdeVecSet::from(set);
        // test round-trip to JSON:
        let s = serde_json::to_string(&serde_vec_set).unwrap();
        assert_eq!(r#"[0,1,2]"#, s);
        let d: SerdeVecSet<u32> = serde_json::from_str(&s).unwrap();
        assert_eq!(d, serde_vec_set);
    }

    #[test]
    fn test_set_duplicates_ignored() {
        let json_str = r#"[0, 1, 0, 2, 1]"#;
        let set: SerdeVecSet<u8> = serde_json::from_str(json_str).unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&0));
        assert!(set.contains(&1));
        assert!(set.contains(&2));
    }
}
