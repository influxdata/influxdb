//! Contains a structure to map from strings to u32 symbols based on
//! string interning.
use snafu::{OptionExt, Snafu};
use string_interner::{
    backend::StringBackend, DefaultHashBuilder, DefaultSymbol, StringInterner, Symbol,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Dictionary lookup error on id {}", id))]
    DictionaryIdLookupError { id: u32 },

    #[snafu(display("Dictionary lookup error for value {}", value))]
    DictionaryValueLookupError { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Dictionary(
    StringInterner<DefaultSymbol, StringBackend<DefaultSymbol>, DefaultHashBuilder>,
);

impl Default for Dictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl Dictionary {
    pub fn new() -> Self {
        Self(StringInterner::new())
    }

    /// Returns the id corresponding to value, adding an entry for the
    /// id if it is not yet present in the dictionary.
    pub fn lookup_value_or_insert(&mut self, value: &str) -> u32 {
        symbol_to_u32(self.0.get_or_intern(value))
    }

    /// Returns the ID in self.dictionary that corresponds to `value`, if any.
    /// Returns an error if no such value is found. Does not add the value
    /// to the dictionary.
    pub fn lookup_value(&self, value: &str) -> Result<u32> {
        self.id(value).context(DictionaryValueLookupError { value })
    }

    /// Returns the ID in self.dictionary that corresponds to `value`,
    /// if any. No error is returned to avoid an allocation when no value is
    /// present
    pub fn id(&self, value: &str) -> Option<u32> {
        self.0.get(value).map(symbol_to_u32)
    }

    /// Returns the str in self.dictionary that corresponds to `id`,
    /// if any. Returns an error if no such id is found
    pub fn lookup_id(&self, id: u32) -> Result<&str> {
        let symbol =
            Symbol::try_from_usize(id as usize).expect("to be able to convert u32 to symbol");
        self.0
            .resolve(symbol)
            .context(DictionaryIdLookupError { id })
    }
}

fn symbol_to_u32(sym: DefaultSymbol) -> u32 {
    sym.to_usize() as u32
}
