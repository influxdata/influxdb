//! Contains a structure to map from strings to u32 symbols based on
//! string interning.
use snafu::{OptionExt, Snafu};
use string_interner::{
    backend::StringBackend, DefaultHashBuilder, DefaultSymbol, StringInterner, Symbol,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Dictionary lookup error on id {}", id))]
    DictionaryIdLookupError { id: DID },

    #[snafu(display("Dictionary lookup error for value {}", value))]
    DictionaryValueLookupError { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A "dictionary ID" (DID) is a compact numeric representation of an interned
/// string in the dictionary. The same string always maps the same DID. DIDs can
/// be compared, hashed and cheaply copied around, just like small integers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DID(DefaultSymbol);

impl DID {
    fn new(s: DefaultSymbol) -> Self {
        Self(s)
    }
}

impl From<DID> for DefaultSymbol {
    fn from(id: DID) -> Self {
        id.0
    }
}

impl std::fmt::Display for DID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_usize())
    }
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    interner: StringInterner<DefaultSymbol, StringBackend<DefaultSymbol>, DefaultHashBuilder>,
    /// the approximate memory size of the dictionary
    pub size: usize,
}

impl Default for Dictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl Dictionary {
    pub fn new() -> Self {
        Self {
            interner: StringInterner::new(),
            size: 0,
        }
    }

    /// Returns the id corresponding to value, adding an entry for the
    /// id if it is not yet present in the dictionary.
    pub fn lookup_value_or_insert(&mut self, value: &str) -> DID {
        self.id(value).unwrap_or_else(|| {
            self.size += value.len();
            self.size += std::mem::size_of::<u32>();
            DID::new(self.interner.get_or_intern(value))
        })
    }

    /// Returns the ID in self.dictionary that corresponds to `value`, if any.
    /// Returns an error if no such value is found. Does not add the value
    /// to the dictionary.
    pub fn lookup_value(&self, value: &str) -> Result<DID> {
        self.id(value).context(DictionaryValueLookupError { value })
    }

    /// Returns the ID in self.dictionary that corresponds to `value`,
    /// if any. No error is returned to avoid an allocation when no value is
    /// present
    pub fn id(&self, value: &str) -> Option<DID> {
        self.interner.get(value).map(DID::new)
    }

    /// Returns the str in self.dictionary that corresponds to `id`,
    /// if any. Returns an error if no such id is found
    pub fn lookup_id(&self, id: DID) -> Result<&str> {
        self.interner
            .resolve(id.into())
            .context(DictionaryIdLookupError { id })
    }
}

#[cfg(test)]
mod test {
    use crate::dictionary::Dictionary;

    #[test]
    fn tracks_size() {
        let mut d = Dictionary::new();
        d.lookup_value_or_insert("foo");
        assert_eq!(7, d.size);
        d.lookup_value_or_insert("this is a much longer string");
        assert_eq!(39, d.size);
        d.lookup_value_or_insert("foo");
        assert_eq!(39, d.size);
    }
}
