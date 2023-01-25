use std::fmt::Display;

use data_types::ParquetFile;

use super::RoundSplit;

#[derive(Debug, Default)]
pub struct AllNowRoundSplit;

impl AllNowRoundSplit {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for AllNowRoundSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "now")
    }
}

impl RoundSplit for AllNowRoundSplit {
    fn split(&self, files: Vec<ParquetFile>) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
        (files, vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(AllNowRoundSplit::new().to_string(), "now");
    }

    #[test]
    fn test_split() {
        let split = AllNowRoundSplit::new();

        // empty input
        assert_eq!(split.split(vec![]), (vec![], vec![]));

        // not empty
        let f1 = ParquetFileBuilder::new(1).build();
        let f2 = ParquetFileBuilder::new(2).build();
        assert_eq!(
            split.split(vec![f1.clone(), f2.clone()]),
            (vec![f1, f2], vec![])
        );
    }
}
