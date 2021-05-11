/// A "dictionary ID" (DID) is a compact numeric representation of an interned
/// string in the dictionary. The same string always maps the same DID.
///
/// DIDs can be compared, hashed and cheaply copied around, just like small integers.
///
/// An i32 is used to match the default for Arrow dictionaries
#[allow(clippy::upper_case_acronyms)]
pub type DID = i32;
pub const INVALID_DID: DID = -1;

pub type Dictionary = arrow_util::dictionary::StringDictionary<DID>;
