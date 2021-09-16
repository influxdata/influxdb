include!(concat!(env!("OUT_DIR"), "/test.syntax3.rs"));
include!(concat!(env!("OUT_DIR"), "/test.syntax3.serde.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_empty() {
        let message = Empty {};

        let encoded = serde_json::to_string(&message).unwrap();
        let _decoded: Empty = serde_json::from_str(&encoded).unwrap();

        let err = serde_json::from_str::<Empty>("343").unwrap_err();
        assert_eq!(
            err.to_string().as_str(),
            "invalid type: integer `343`, expected struct test.syntax3.Empty at line 1 column 3"
        );

        let err = serde_json::from_str::<Empty>("{\"foo\": \"bar\"}").unwrap_err();
        assert_eq!(
            err.to_string().as_str(),
            "unknown field `foo`, there are no fields at line 1 column 6"
        );
    }

    #[test]
    fn test_kitchen_sink() {
        let mut decoded: KitchenSink = serde_json::from_str("{}").unwrap();

        let verify_encode = |decoded: &KitchenSink, expected: &str| {
            assert_eq!(serde_json::to_string(&decoded).unwrap().as_str(), expected);
        };

        let verify_decode = |decoded: &KitchenSink, expected: &str| {
            assert_eq!(decoded, &serde_json::from_str(expected).unwrap());
        };

        let verify = |decoded: &KitchenSink, expected: &str| {
            verify_encode(decoded, expected);
            verify_decode(decoded, expected);
        };

        verify(&decoded, "{}");
        decoded.i32 = 24;
        verify(&decoded, r#"{"i32":24}"#);
        decoded.i32 = 0;
        verify_decode(&decoded, "{}");

        // Explicit optional fields can distinguish between no value and default value
        decoded.optional_i32 = Some(2);
        verify(&decoded, r#"{"optionalI32":2}"#);

        decoded.optional_i32 = Some(0);
        verify(&decoded, r#"{"optionalI32":0}"#);

        // Can also decode from string
        verify_decode(&decoded, r#"{"optionalI32":"0"}"#);

        decoded.optional_i32 = None;
        verify_decode(&decoded, "{}");

        // 64-bit integers are encoded as strings
        decoded.i64 = 123125;
        verify(&decoded, r#"{"i64":"123125"}"#);

        decoded.i64 = 0;
        verify_decode(&decoded, "{}");

        decoded.optional_i64 = Some(532);
        verify(&decoded, r#"{"optionalI64":"532"}"#);

        decoded.optional_i64 = Some(0);
        verify(&decoded, r#"{"optionalI64":"0"}"#);

        // Can also decode from non-string
        verify_decode(&decoded, r#"{"optionalI64":0}"#);

        decoded.optional_i64 = None;
        verify_decode(&decoded, "{}");

        decoded.u64 = 34346;
        decoded.u32 = 567094456;
        decoded.optional_u32 = Some(0);
        decoded.optional_u64 = Some(3);
        verify(
            &decoded,
            r#"{"u32":567094456,"optionalU32":0,"u64":"34346","optionalU64":"3"}"#,
        );

        decoded.u64 = 0;
        decoded.u32 = 0;
        decoded.optional_u32 = None;
        decoded.optional_u64 = None;
        verify_decode(&decoded, "{}");

        decoded.repeated_i32 = vec![0, 23, 5, 6, 2, 34];
        verify(&decoded, r#"{"repeatedI32":[0,23,5,6,2,34]}"#);
        // Can also mix in some strings
        verify_decode(&decoded, r#"{"repeatedI32":[0,"23",5,6,"2",34]}"#);

        decoded.repeated_i32 = vec![];
        verify_decode(&decoded, "{}");

        decoded.repeated_u64 = vec![0, 532, 2];
        verify(&decoded, r#"{"repeatedU64":["0","532","2"]}"#);
        // Can also mix in some non-strings
        verify_decode(&decoded, r#"{"repeatedU64":["0",532,"2"]}"#);

        decoded.repeated_u64 = vec![];
        verify_decode(&decoded, "{}");

        // Enumerations should be encoded as strings
        decoded.value = kitchen_sink::Value::A as i32;
        verify(&decoded, r#"{"value":"VALUE_A"}"#);

        // Can also use variant number
        verify_decode(&decoded, r#"{"value":45}"#);

        decoded.value = kitchen_sink::Value::Unknown as i32;
        verify_decode(&decoded, "{}");

        decoded.optional_value = Some(kitchen_sink::Value::Unknown as i32);
        verify(&decoded, r#"{"optionalValue":"VALUE_UNKNOWN"}"#);

        // Can also use variant number
        verify_decode(&decoded, r#"{"optionalValue":0}"#);

        decoded.optional_value = None;
        verify_decode(&decoded, "{}");

        decoded
            .string_dict
            .insert("foo".to_string(), "bar".to_string());
        verify(&decoded, r#"{"stringDict":{"foo":"bar"}}"#);

        decoded.string_dict = Default::default();
        verify_decode(&decoded, "{}");

        decoded
            .int32_dict
            .insert(343, kitchen_sink::Prefix::A as i32);
        // Dictionary keys should always be strings
        // Enum dictionary values should be encoded as strings
        verify(&decoded, r#"{"int32Dict":{"343":"A"}}"#);
        // Enum dictionary values can be decoded from integers
        verify_decode(&decoded, r#"{"int32Dict":{"343":66}}"#);

        decoded.int32_dict = Default::default();
        verify_decode(&decoded, "{}");

        // 64-bit dictionary values should be encoded as strings
        decoded.integer_dict.insert(12, 13);
        verify(&decoded, r#"{"integerDict":{"12":"13"}}"#);
        // 64-bit dictionary values can be decoded from numeric types
        verify_decode(&decoded, r#"{"integerDict":{"12":13}}"#);

        decoded.integer_dict = Default::default();
        verify_decode(&decoded, "{}");

        decoded.one_of = Some(kitchen_sink::OneOf::OneOfI32(0));
        verify(&decoded, r#"{"oneOfI32":0}"#);
        // Can also specify string
        verify_decode(&decoded, r#"{"oneOfI32":"0"}"#);

        decoded.one_of = Some(kitchen_sink::OneOf::OneOfI32(12));
        verify(&decoded, r#"{"oneOfI32":12}"#);

        decoded.one_of = Some(kitchen_sink::OneOf::OneOfBool(false));
        verify(&decoded, r#"{"oneOfBool":false}"#);

        decoded.one_of = Some(kitchen_sink::OneOf::OneOfBool(true));
        verify(&decoded, r#"{"oneOfBool":true}"#);

        decoded.one_of = Some(kitchen_sink::OneOf::OneOfValue(
            kitchen_sink::Value::B as i32,
        ));
        verify(&decoded, r#"{"oneOfValue":"VALUE_B"}"#);
        // Can also specify enum variant
        verify_decode(&decoded, r#"{"oneOfValue":63}"#);

        decoded.one_of = None;
        verify_decode(&decoded, "{}");

        decoded.repeated_value = vec![
            kitchen_sink::Value::B as i32,
            kitchen_sink::Value::B as i32,
            kitchen_sink::Value::A as i32,
        ];
        verify(
            &decoded,
            r#"{"repeatedValue":["VALUE_B","VALUE_B","VALUE_A"]}"#,
        );
        verify_decode(&decoded, r#"{"repeatedValue":[63,"VALUE_B","VALUE_A"]}"#);

        decoded.repeated_value = Default::default();
        verify_decode(&decoded, "{}");

        decoded.bytes = prost::bytes::Bytes::from_static(b"kjkjkj");
        verify(&decoded, r#"{"bytes":"a2pramtq"}"#);

        decoded.bytes = Default::default();
        verify_decode(&decoded, "{}");

        decoded.optional_bytes = Some(prost::bytes::Bytes::from_static(b"kjkjkj"));
        verify(&decoded, r#"{"optionalBytes":"a2pramtq"}"#);

        decoded.optional_bytes = Some(Default::default());
        verify(&decoded, r#"{"optionalBytes":""}"#);

        decoded.optional_bytes = None;
        verify_decode(&decoded, "{}");

        decoded.repeated_bytes = vec![
            prost::bytes::Bytes::from_static(b"sdfsd"),
            prost::bytes::Bytes::from_static(b"fghfg"),
        ];
        verify(&decoded, r#"{"repeatedBytes":["c2Rmc2Q=","ZmdoZmc="]}"#);

        decoded.repeated_bytes = Default::default();
        verify_decode(&decoded, "{}");

        // decoded.bytes_dict.insert(
        //     "test".to_string(),
        //     prost::bytes::Bytes::from_static(b"asdf"),
        // );
        // verify(&decoded, r#"{"bytesDict":{"test":"YXNkZgo="}}"#);
        //
        // decoded.bytes_dict = Default::default();
        // verify_decode(&decoded, "{}");

        decoded.string = "test".to_string();
        verify(&decoded, r#"{"string":"test"}"#);

        decoded.string = Default::default();
        verify_decode(&decoded, "{}");

        decoded.optional_string = Some(String::new());
        verify(&decoded, r#"{"optionalString":""}"#);
    }
}
