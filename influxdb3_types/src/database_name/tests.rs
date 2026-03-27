use crate::database_name::{DatabaseName, DatabaseNameError, LENGTH_CONSTRAINT, TruncatedString};

#[test]
fn test_into_string() {
    let name = DatabaseName::new("bananas").unwrap();

    // From ref
    assert_eq!(Into::<String>::into(&name), "bananas");
    // Owned type string
    assert_eq!(Into::<String>::into(name), "bananas");
}

#[test]
fn test_deref() {
    let db = DatabaseName::new("my_example_name").unwrap();
    assert_eq!(&*db, "my_example_name");
}

#[test]
fn test_too_short() {
    let name = "".to_string();
    let got = DatabaseName::try_from(name).unwrap_err();

    assert!(matches!(
        got,
        DatabaseNameError::LengthConstraint { name: _n }
    ));
}

#[test]
fn test_too_long() {
    let name = "way_too_long_".repeat(1000);
    let got = DatabaseName::try_from(name).unwrap_err();
    assert!(matches!(got, DatabaseNameError::LengthConstraint { .. }));

    let max_length_name = "a".repeat(511);
    let ok = DatabaseName::try_from(max_length_name);
    assert!(ok.is_ok());

    let range_plus_one_name = "a".repeat(512);
    let got = DatabaseName::try_from(range_plus_one_name).unwrap_err();
    assert!(matches!(got, DatabaseNameError::LengthConstraint { .. }));
}

#[test]
fn test_bad_chars_null() {
    let got = DatabaseName::new("example\x00").unwrap_err();
    assert_eq!(
        got.to_string(),
        "database name 'example\x00' contains invalid character, character number 7 is not allowed"
    );
}

#[test]
fn test_bad_chars_high_control() {
    let got = DatabaseName::new("\u{007f}example").unwrap_err();
    assert_eq!(
        got.to_string(),
        "database name '\u{007f}example' contains invalid character, character number 0 is not allowed"
    );
}

#[test]
fn test_bad_chars_tab() {
    let got = DatabaseName::new("example\tdb").unwrap_err();
    assert_eq!(
        got.to_string(),
        "database name 'example\tdb' contains invalid character, character number 7 is not allowed"
    );
}

#[test]
fn test_bad_chars_newline() {
    let got = DatabaseName::new("my_example\ndb").unwrap_err();
    assert_eq!(
        got.to_string(),
        "database name 'my_example\ndb' contains invalid character, character number 10 is not allowed"
    );
}

#[test]
fn test_bad_chars_whitespace() {
    let got = DatabaseName::new("my_example db").unwrap_err();
    assert_eq!(
        got.to_string(),
        "database name 'my_example db' contains invalid character, character number 10 is not allowed"
    );
}

#[test]
fn test_bad_chars_single_quote() {
    let got = DatabaseName::new("my_example'db").unwrap_err();
    assert_eq!(
        got.to_string(),
        "database name 'my_example\'db' contains invalid character, character number 10 is not allowed"
    );
}

#[test]
fn test_ok_chars() {
    let db =
        DatabaseName::new("my-example-db_with_underscores/and/fwd/slash/AndCaseSensitive").unwrap();
    assert_eq!(
        &*db,
        "my-example-db_with_underscores/and/fwd/slash/AndCaseSensitive"
    );

    let db = DatabaseName::new("a_ã_京").unwrap();
    assert_eq!(&*db, "a_ã_京");
}

#[test]
fn truncated_string() {
    let s = "hello world";
    assert_eq!(
        TruncatedString::new(s).inner(),
        "hello world",
        "No truncation should happen, this is not past the static length constraint"
    );

    let s = "a".repeat(1000);
    assert_eq!(
        TruncatedString::new(s).inner(),
        "a".repeat(*LENGTH_CONSTRAINT.end()),
        "The string should be truncated to the static constraint"
    );
}
