//! This module is a gnarly hack around the fact opentelemetry doesn't currently let you
//! register an observer multiple times with the same name
//!
//! See https://github.com/open-telemetry/opentelemetry-rust/issues/541

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use observability_deps::opentelemetry::metrics::{Meter, ObserverResult};

type CallbackFunc<T> = Box<dyn Fn(&ObserverResult<T>) + Send + Sync + 'static>;

#[derive(Clone)]
struct CallbackCollection<T>(Arc<Mutex<Vec<CallbackFunc<T>>>>);

impl<T> CallbackCollection<T> {
    fn new<F>(f: F) -> Self
    where
        F: Fn(&ObserverResult<T>) + Send + Sync + 'static,
    {
        Self(Arc::new(Mutex::new(vec![Box::new(f)])))
    }

    fn push<F>(&self, f: F)
    where
        F: Fn(&ObserverResult<T>) + Send + Sync + 'static,
    {
        self.0.lock().push(Box::new(f))
    }

    fn invoke(&self, observer: ObserverResult<T>) {
        let callbacks = self.0.lock();
        for callback in callbacks.iter() {
            callback(&observer)
        }
    }
}

enum Callbacks {
    U64Value(CallbackCollection<u64>),
    U64Sum(CallbackCollection<u64>),
    F64Value(CallbackCollection<f64>),
    F64Sum(CallbackCollection<f64>),
}

impl std::fmt::Debug for Callbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::U64Value(_) => write!(f, "U64Value"),
            Self::U64Sum(_) => write!(f, "U64Sum"),
            Self::F64Value(_) => write!(f, "F64Value"),
            Self::F64Sum(_) => write!(f, "F64Sum"),
        }
    }
}

#[derive(Debug)]
pub struct ObserverCollection {
    callbacks: Mutex<HashMap<String, Callbacks>>,
    meter: Meter,
}

impl ObserverCollection {
    pub fn new(meter: Meter) -> Self {
        Self {
            callbacks: Default::default(),
            meter,
        }
    }

    pub fn u64_value_observer<F>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(&ObserverResult<u64>) + Send + Sync + 'static,
    {
        match self.callbacks.lock().entry(name.into()) {
            Entry::Occupied(occupied) => match occupied.get() {
                Callbacks::U64Value(callbacks) => callbacks.push(Box::new(callback)),
                c => panic!("metric type mismatch, expected U64Value got {:?}", c),
            },
            Entry::Vacant(vacant) => {
                let name = vacant.key().clone();
                let callbacks = CallbackCollection::new(callback);
                vacant.insert(Callbacks::U64Value(callbacks.clone()));
                self.meter
                    .u64_value_observer(name, move |observer| callbacks.invoke(observer))
                    .with_description(description)
                    .init();
            }
        }
    }

    pub fn u64_sum_observer<F>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(&ObserverResult<u64>) + Send + Sync + 'static,
    {
        match self.callbacks.lock().entry(name.into()) {
            Entry::Occupied(occupied) => match occupied.get() {
                Callbacks::U64Sum(callbacks) => callbacks.push(Box::new(callback)),
                c => panic!("metric type mismatch, expected U64Sum got {:?}", c),
            },
            Entry::Vacant(vacant) => {
                let name = vacant.key().clone();
                let callbacks = CallbackCollection::new(callback);
                vacant.insert(Callbacks::U64Sum(callbacks.clone()));
                self.meter
                    .u64_sum_observer(name, move |observer| callbacks.invoke(observer))
                    .with_description(description)
                    .init();
            }
        }
    }

    pub fn f64_value_observer<F>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(&ObserverResult<f64>) + Send + Sync + 'static,
    {
        match self.callbacks.lock().entry(name.into()) {
            Entry::Occupied(occupied) => match occupied.get() {
                Callbacks::F64Value(callbacks) => callbacks.push(Box::new(callback)),
                c => panic!("metric type mismatch, expected F64Value got {:?}", c),
            },
            Entry::Vacant(vacant) => {
                let name = vacant.key().clone();
                let callbacks = CallbackCollection::new(callback);
                vacant.insert(Callbacks::F64Value(callbacks.clone()));
                self.meter
                    .f64_value_observer(name, move |observer| callbacks.invoke(observer))
                    .with_description(description)
                    .init();
            }
        }
    }

    pub fn f64_sum_observer<F>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        callback: F,
    ) where
        F: Fn(&ObserverResult<f64>) + Send + Sync + 'static,
    {
        match self.callbacks.lock().entry(name.into()) {
            Entry::Occupied(occupied) => match occupied.get() {
                Callbacks::F64Sum(callbacks) => callbacks.push(Box::new(callback)),
                c => panic!("metric type mismatch, expected F64Sum got {:?}", c),
            },
            Entry::Vacant(vacant) => {
                let name = vacant.key().clone();
                let callbacks = CallbackCollection::new(callback);
                vacant.insert(Callbacks::F64Sum(callbacks.clone()));
                self.meter
                    .f64_sum_observer(name, move |observer| callbacks.invoke(observer))
                    .with_description(description)
                    .init();
            }
        }
    }
}
