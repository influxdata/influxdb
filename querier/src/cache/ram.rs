use std::ops::{Add, Sub};

use cache_system::resource_consumption::Resource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RamSize(pub usize);

impl Resource for RamSize {
    fn zero() -> Self {
        Self(0)
    }

    fn unit() -> &'static str {
        "bytes"
    }
}

impl From<RamSize> for u64 {
    fn from(s: RamSize) -> Self {
        s.0 as Self
    }
}

impl Add for RamSize {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.checked_add(rhs.0).expect("overflow"))
    }
}

impl Sub for RamSize {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0.checked_sub(rhs.0).expect("underflow"))
    }
}

#[cfg(test)]
pub mod test_util {
    use super::*;
    use std::sync::Arc;

    use cache_system::backend::policy::lru::ResourcePool;
    use tokio::runtime::Handle;

    pub fn test_ram_pool() -> Arc<ResourcePool<RamSize>> {
        Arc::new(ResourcePool::new(
            "pool",
            RamSize(usize::MAX),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ))
    }
}
