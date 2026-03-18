use crate::ValueCollection;
use generated_types::influxdata::pbdata::v1 as proto;

/// Newtype for strings that can be pushed to or read from [`proto::PackedStrings`]
#[derive(Debug, Clone)]
pub struct PackedStr<'a>(pub &'a str);

impl ValueCollection for proto::PackedStrings {
    type Item<'a>
        = PackedStr<'a>
    where
        Self: 'a;

    fn get(&self, idx: usize) -> Option<Self::Item<'_>> {
        let (start, end) = (self.offsets.get(idx)?, self.offsets.get(idx + 1)?);
        let s = self
            .values
            .split_at(*start as usize)
            .1
            .split_at((*end - *start) as usize)
            .0;
        Some(PackedStr(s))
    }

    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
}

/// Newtype for strings that can be pushed to or read from [`proto::InternedStrings`]
#[derive(Debug, PartialEq, Clone)]
pub struct InternedStr<'a>(pub &'a str);

impl ValueCollection for proto::InternedStrings {
    type Item<'a>
        = InternedStr<'a>
    where
        Self: 'a;

    fn get(&self, idx: usize) -> Option<Self::Item<'_>> {
        let dict = self.dictionary.as_ref()?;
        let idx = self.values.get(idx)?;
        let s = dict.get(*idx as usize)?;
        Some(InternedStr(s.0))
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}
