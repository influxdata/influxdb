use super::{object_map::ObjectMap, request::Request, status, Handler, Result};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use hyper::Body;
use kube_core::{ApiResource, DynamicObject, ObjectList, ObjectMeta, Resource};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI16, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct ResourceHandler<R> {
    api_resource: ApiResource,
    objects: Arc<Mutex<ObjectMap>>,
    gen_id: AtomicI16,
    phantom: PhantomData<R>,
}

impl<R> ResourceHandler<R>
where
    R: Resource<DynamicType = ()> + DeserializeOwned + Serialize,
{
    /// Create a new handler for a kubernetes resource type.
    pub fn new() -> Self {
        let api_resource = ApiResource::erase::<R>(&());
        Self {
            api_resource: api_resource.clone(),
            objects: Arc::new(Mutex::new(ObjectMap::new(api_resource))),
            gen_id: AtomicI16::new(0),
            phantom: Default::default(),
        }
    }

    /// Retrieve a stored kubernetes resource, if available.
    pub fn get(&self, ns: impl Into<String>, name: impl Into<String>) -> Option<R> {
        let ns = ns.into();
        let ns = if ns.is_empty() { None } else { Some(ns) };
        let name = name.into();
        match Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .get()
        {
            Ok(obj) => obj.clone().try_parse::<R>().ok(),
            _ => None,
        }
    }

    /// Store, or overwrite, the resource with the given name.
    pub fn set(&self, ns: impl Into<String>, name: impl Into<String>, resource: R) -> R {
        let ns = ns.into();
        let ns = if ns.is_empty() { None } else { Some(ns) };
        let name = name.into();
        let obj = serde_json::from_value::<DynamicObject>(serde_json::to_value(resource).unwrap())
            .unwrap();
        let (_, obj) = Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .update(obj)
            .unwrap();
        obj.try_parse::<R>().unwrap()
    }

    /// Retrieve all the stored resources. if the resource is namespaced and ns is not None then
    /// only resources in that namespace will be returned.
    pub fn all(&self, ns: impl Into<String>) -> Vec<R> {
        let ns = ns.into();
        let ns = if ns.is_empty() { None } else { Some(ns) };
        Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .values(ns)
            .cloned()
            .filter_map(|v| v.try_parse::<R>().ok())
            .collect::<Vec<_>>()
    }
}

impl<R> Default for ResourceHandler<R>
where
    R: Resource<DynamicType = ()> + DeserializeOwned + Serialize,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R> ResourceHandler<R> {
    fn maybe_generate_name(&self, meta: &mut ObjectMeta) {
        if meta.name.is_none() {
            if let Some(prefix) = &meta.generate_name {
                meta.name = Some(format!(
                    "{prefix}{:05}",
                    self.gen_id.fetch_add(1, Ordering::SeqCst)
                ));
            }
        }
    }

    fn create(&self, body: Vec<u8>) -> Result<Response<Body>> {
        let mut obj = serde_json::from_reader::<&[u8], DynamicObject>(body.as_ref())?;
        self.maybe_generate_name(&mut obj.metadata);
        let ns = obj.metadata.namespace.clone();
        let name = obj.metadata.name.clone().unwrap();
        match Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .create(obj)
        {
            Ok(obj) => response(StatusCode::CREATED, obj),
            Err(status) => response(StatusCode::from_u16(status.code).unwrap(), &status),
        }
    }

    fn retrieve(&self, ns: Option<String>, name: String) -> Result<Response<Body>> {
        match Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .get()
        {
            Ok(obj) => response(StatusCode::OK, obj),
            Err(status) => response(StatusCode::from_u16(status.code).unwrap(), &status),
        }
    }

    fn list(&self, ns: Option<String>) -> Result<Response<Body>> {
        let list = ObjectList {
            metadata: Default::default(),
            items: Arc::clone(&self.objects)
                .lock()
                .unwrap()
                .values(ns)
                .cloned()
                .collect(),
        };
        response(StatusCode::OK, &list)
    }

    fn update(&self, ns: Option<String>, name: String, body: Vec<u8>) -> Result<Response<Body>> {
        let obj = serde_json::from_reader::<&[u8], DynamicObject>(body.as_ref())?;
        match Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .update(obj)
        {
            Ok((true, obj)) => response(StatusCode::CREATED, &obj),
            Ok((false, obj)) => response(StatusCode::OK, &obj),
            Err(status) => response(StatusCode::from_u16(status.code).unwrap(), &status),
        }
    }

    fn update_subresource(
        &self,
        ns: Option<String>,
        name: String,
        subresource: String,
        body: Vec<u8>,
    ) -> Result<Response<Body>> {
        let obj = serde_json::from_reader::<&[u8], DynamicObject>(body.as_ref())?;
        match Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .update_subresource(subresource, obj)
        {
            Ok((true, obj)) => response(StatusCode::CREATED, &obj),
            Ok((false, obj)) => response(StatusCode::OK, &obj),
            Err(status) => response(StatusCode::from_u16(status.code).unwrap(), &status),
        }
    }

    fn delete(&self, ns: Option<String>, name: String) -> Result<Response<Body>> {
        match Arc::clone(&self.objects)
            .lock()
            .unwrap()
            .entry(ns, name)
            .delete()
        {
            Ok(obj) => response(StatusCode::OK, &obj),
            Err(status) => response(StatusCode::from_u16(status.code).unwrap(), &status),
        }
    }

    fn patch(
        &self,
        ns: Option<String>,
        name: String,
        header: HeaderMap,
        body: Vec<u8>,
    ) -> Result<Response<Body>> {
        let content_type = match header.get("Content-Type") {
            Some(v) => v.to_str().unwrap(),
            None => "",
        };
        match content_type {
            "application/apply-patch+yaml" => {
                let obj = serde_yaml::from_reader::<&[u8], DynamicObject>(body.as_ref())?;
                match Arc::clone(&self.objects)
                    .lock()
                    .unwrap()
                    .entry(ns, name)
                    .apply(obj)
                {
                    Ok(obj) => response(StatusCode::OK, &obj),
                    Err(status) => response(StatusCode::from_u16(status.code).unwrap(), &status),
                }
            }
            ct => {
                let status = status::invalid(&format!("unsupported patch type \"{ct}\""));
                response(StatusCode::from_u16(status.code).unwrap(), &status)
            }
        }
    }
}

fn response<T: Serialize>(status: StatusCode, data: &T) -> Result<Response<Body>> {
    let buf = serde_json::to_vec(data)?;
    Ok(Response::builder().status(status).body(buf.into())?)
}

impl<R> Handler for ResourceHandler<R>
where
    R: Debug,
{
    fn api_resource(&self) -> ApiResource {
        self.api_resource.clone()
    }

    fn handle(
        &self,
        req: Request,
        header: HeaderMap<HeaderValue>,
        body: Vec<u8>,
    ) -> Result<Response<Body>> {
        let Request {
            verb,
            ns,
            name,
            subresource,
            ..
        } = req;
        match verb.as_str() {
            "create" => self.create(body),
            "delete" => self.delete(ns, name.unwrap()),
            "get" => self.retrieve(ns, name.unwrap()),
            "list" => self.list(ns),
            "patch" => self.patch(ns, name.unwrap(), header, body),
            "update" => {
                if let Some(subresource) = subresource {
                    self.update_subresource(ns, name.unwrap(), subresource, body)
                } else {
                    self.update(ns, name.unwrap(), body)
                }
            }
            v => {
                let api_resource = self.api_resource();
                super::status::method_not_allowed(&api_resource, name, v)
            }
        }
    }
}
