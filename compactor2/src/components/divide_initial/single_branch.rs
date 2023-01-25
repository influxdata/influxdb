use std::fmt::Display;

use data_types::ParquetFile;

use super::DivideInitial;

#[derive(Debug, Default)]
pub struct SingleBranchDivideInitial;

impl SingleBranchDivideInitial {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for SingleBranchDivideInitial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "single_branch")
    }
}

impl DivideInitial for SingleBranchDivideInitial {
    fn divide(&self, files: Vec<ParquetFile>) -> Vec<Vec<ParquetFile>> {
        if files.is_empty() {
            vec![]
        } else {
            vec![files]
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            SingleBranchDivideInitial::new().to_string(),
            "single_branch"
        );
    }

    #[test]
    fn test_divide() {
        let divide = SingleBranchDivideInitial::new();

        // empty input
        assert_eq!(divide.divide(vec![]), Vec::<Vec<_>>::new());

        // not empty
        let f1 = ParquetFileBuilder::new(1).build();
        let f2 = ParquetFileBuilder::new(2).build();
        assert_eq!(
            divide.divide(vec![f1.clone(), f2.clone()]),
            vec![vec![f1, f2]]
        );
    }
}
