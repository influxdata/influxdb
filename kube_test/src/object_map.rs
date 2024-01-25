use super::status;
use kube_core::{ApiResource, DynamicObject, Status};
use std::collections::{hash_map, HashMap};
use std::mem;

#[derive(Debug)]
pub struct ObjectMap {
    api_resource: ApiResource,
    objects: HashMap<Key, DynamicObject>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct Key {
    ns: Option<String>,
    name: String,
}

impl ObjectMap {
    pub fn new(api_resource: ApiResource) -> Self {
        Self {
            api_resource,
            objects: HashMap::new(),
        }
    }

    pub fn entry(&mut self, ns: Option<String>, name: String) -> Entry<'_> {
        let key = Key { ns, name };
        let inner = self.objects.entry(key);
        Entry {
            api_resource: &self.api_resource,
            inner,
        }
    }

    pub fn values(&self, ns: Option<String>) -> Values<'_> {
        Values {
            ns,
            inner: self.objects.values(),
        }
    }
}

#[derive(Debug)]
pub struct Entry<'a> {
    api_resource: &'a ApiResource,
    inner: hash_map::Entry<'a, Key, DynamicObject>,
}

impl<'a> Entry<'a> {
    pub fn create(self, obj: DynamicObject) -> Result<&'a DynamicObject, Box<Status>> {
        match self.inner {
            hash_map::Entry::Occupied(entry) => Err(Box::new(status::already_exists(
                self.api_resource,
                Some(entry.key().name.as_str()),
            ))),
            hash_map::Entry::Vacant(entry) => {
                let Key { ns, name } = entry.key().clone();
                let obj = entry.insert(obj);
                obj.metadata.namespace = ns;
                obj.metadata.name = Some(name);
                if obj.metadata.uid.is_none() {
                    obj.metadata.uid = Some(format!("{}", rand::random::<u64>()));
                }
                Ok(obj)
            }
        }
    }

    pub fn get(&mut self) -> Result<&DynamicObject, Box<Status>> {
        match &self.inner {
            hash_map::Entry::Occupied(entry) => Ok(entry.get()),
            hash_map::Entry::Vacant(entry) => {
                let name = entry.key().name.as_str();
                Err(Box::new(status::not_found(self.api_resource, Some(name))))
            }
        }
    }

    pub fn delete(self) -> Result<DynamicObject, Box<Status>> {
        match self.inner {
            hash_map::Entry::Occupied(entry) => {
                let obj = entry.remove();
                Ok(obj)
            }
            hash_map::Entry::Vacant(entry) => {
                let name = entry.key().name.as_str();
                Err(Box::new(status::not_found(self.api_resource, Some(name))))
            }
        }
    }

    pub fn update(self, mut obj: DynamicObject) -> Result<(bool, DynamicObject), Box<Status>> {
        match self.inner {
            hash_map::Entry::Occupied(mut entry) => {
                let Key { ns, name } = entry.key().clone();
                obj.metadata.namespace = ns;
                obj.metadata.name = Some(name);
                let _ = entry.insert(obj.clone());
                Ok((false, obj))
            }
            hash_map::Entry::Vacant(entry) => {
                let Key { ns, name } = entry.key().clone();
                let obj = entry.insert(obj);
                obj.metadata.namespace = ns;
                obj.metadata.name = Some(name);
                if obj.metadata.uid.is_none() {
                    obj.metadata.uid = Some(format!("{}", rand::random::<u64>()));
                }
                Ok((true, obj.clone()))
            }
        }
    }

    pub fn apply(self, patch: DynamicObject) -> Result<DynamicObject, Box<Status>> {
        let Key { ns, name } = self.inner.key().clone();

        let obj = self.inner.or_insert_with(|| {
            let obj = DynamicObject::new(name.as_str(), self.api_resource);
            if let Some(ns) = ns {
                obj.within(ns.as_str())
            } else {
                obj
            }
        });
        let _ = mem::replace(&mut obj.data, patch.data);
        Ok(obj.clone())
    }

    pub fn update_subresource(
        self,
        subresource: String,
        obj: DynamicObject,
    ) -> Result<(bool, DynamicObject), Box<Status>> {
        match self.inner {
            hash_map::Entry::Occupied(mut entry) => {
                if let Some(value) = obj.data.as_object().and_then(|v| v.get(&subresource)) {
                    if let Some(data) = entry.get_mut().data.as_object_mut() {
                        data.insert(subresource, value.clone());
                    }
                }
                Ok((false, entry.get().clone()))
            }
            hash_map::Entry::Vacant(entry) => {
                let Key { ns, name } = entry.key().clone();
                let obj = entry.insert(obj);
                obj.metadata.namespace = ns;
                obj.metadata.name = Some(name);
                if obj.metadata.uid.is_none() {
                    obj.metadata.uid = Some(format!("{}", rand::random::<u64>()));
                }
                Ok((true, obj.clone()))
            }
        }
    }
}

pub struct Values<'a> {
    ns: Option<String>,
    inner: hash_map::Values<'a, Key, DynamicObject>,
}

impl<'a> Iterator for Values<'a> {
    type Item = &'a DynamicObject;
    fn next(&mut self) -> Option<Self::Item> {
        match &self.ns {
            None => self.inner.next(),
            Some(ns) => loop {
                match self.inner.next() {
                    None => return None,
                    Some(v) => match &v.metadata.namespace {
                        Some(ns2) if ns2 == ns => return Some(v),
                        _ => continue,
                    },
                };
            },
        }
    }
}
