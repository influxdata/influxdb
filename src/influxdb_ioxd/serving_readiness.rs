use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Debug, Clone)]
pub enum ServingReadinessState {
    Unavailable,
    Serving,
}

impl std::str::FromStr for ServingReadinessState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "unavailable" => Ok(Self::Unavailable),
            "serving" => Ok(Self::Serving),
            _ => Err(format!(
                "Invalid serving readiness format '{}'. Valid options: unavailable, serving",
                s
            )),
        }
    }
}

impl From<bool> for ServingReadinessState {
    fn from(v: bool) -> Self {
        match v {
            true => Self::Serving,
            false => Self::Unavailable,
        }
    }
}

impl From<ServingReadinessState> for bool {
    fn from(state: ServingReadinessState) -> Self {
        match state {
            ServingReadinessState::Unavailable => false,
            ServingReadinessState::Serving => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServingReadiness(Arc<AtomicBool>);

impl ServingReadiness {
    pub fn new(value: Arc<AtomicBool>) -> Self {
        Self(value)
    }

    pub fn get(&self) -> ServingReadinessState {
        self.0.load(Ordering::SeqCst).into()
    }

    pub fn set(&self, state: ServingReadinessState) {
        self.0.store(state.into(), Ordering::SeqCst)
    }
}

impl From<Arc<AtomicBool>> for ServingReadiness {
    fn from(value: Arc<AtomicBool>) -> Self {
        Self::new(value)
    }
}

impl From<AtomicBool> for ServingReadiness {
    fn from(value: AtomicBool) -> Self {
        Arc::new(value).into()
    }
}

impl From<ServingReadinessState> for ServingReadiness {
    fn from(value: ServingReadinessState) -> Self {
        AtomicBool::new(value.into()).into()
    }
}
