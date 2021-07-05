/// Simple representation of the state a database can be in.
///
/// The state machine is a simple linear state machine:
///
/// ```text
/// Known -> RulesLoaded -> Initialized
/// ```
#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseStateCode {
    /// Database is known but nothing is loaded.
    Known,

    /// Rules are loaded
    RulesLoaded,

    /// Fully initialized database.
    Initialized,
}
