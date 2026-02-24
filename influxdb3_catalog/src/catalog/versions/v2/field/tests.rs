use super::*;

#[test]
fn test_parse_field_name() {
    assert_eq!(
        parse_field_name_aware("field_fam::field_name"),
        FieldName::Qualified("field_fam", "field_name")
    );
    assert_eq!(
        parse_field_name_auto("field_fam::field_name"),
        FieldName::Unqualified("field_fam::field_name")
    );

    assert_eq!(
        parse_field_name_aware("field_fam::sub::field_name"),
        FieldName::Qualified("field_fam", "sub::field_name")
    );

    assert_eq!(
        parse_field_name_aware("field::"),
        FieldName::Unqualified("field::")
    );

    assert_eq!(parse_field_name_aware("::"), FieldName::Unqualified("::"));

    assert_eq!(
        parse_field_name_aware("simple_field"),
        FieldName::Unqualified("simple_field")
    );

    assert_eq!(parse_field_name_aware(""), FieldName::Unqualified(""));
}
