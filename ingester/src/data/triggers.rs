//! Testing utilities for orchestrating tests

/// Production verson is a NoOp
#[cfg(not(test))]
mod prod;

#[cfg(not(test))]
pub(crate) use self::prod::TestTriggers;

/// Testing version
#[cfg(test)]
mod test;

#[cfg(test)]
pub(crate) use self::test::TestTriggers;
