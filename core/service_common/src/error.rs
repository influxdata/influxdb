//! Utilities for error handling in IOx services
use arrow::error::ArrowError;
use arrow_flight::error::FlightError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;

/// Converts a [`DataFusionError`] into the appropriate [`tonic::Code`]
///
/// Where possible, this function should try to ensure that the "user sees the error message rather
/// than an opaque message". Typically the messages of [`tonic::Code::Internal`] are not displayed
/// to the user as they result from bugs in the software and the user can't do anything about them.
///
/// On the other hand, [`tonic::Code::InvalidArgument`] can cause confusion with observability
/// because these errors are not counted in the "failed query" metrics. This can result in bugs
/// being hidden from metrics, which can make troubleshooting more difficult.
///
/// In an ideal world, it would be totally clear from a [`DataFusionError`] which errors belonged
/// in which mapping.
///
/// However, this is not always the case, so the code by default takes the "conservative UX"
/// approach to "show the user the message". This may be at odds with the conservative approach
/// from a security or observability perspective.
///
/// When possible, each error should be evaluated on a case-by-case basis. In the future we may
/// need to become more granular in how we categorize these errors.
pub fn datafusion_error_to_tonic_code(e: &DataFusionError) -> tonic::Code {
    let e = e.find_root();

    match e {
        DataFusionError::ResourcesExhausted(_) => tonic::Code::ResourceExhausted,
        // Map as many as possible back into user visible (non internal) errors
        DataFusionError::SQL(_, _)
        | DataFusionError::SchemaError(_, _)
        // Execution, ExecutableJoin, and ArrowError might be due to an internal error (e.g. some
        // sort of IO error or bug) or due to a user input error (e.g. you can get an Arrow error
        // if you try and divide by a column and it has zeros).
        //
        // Since we are not sure they are all internal errors, we classify them as InvalidArgument
        // so the user has a chance to see them.
        | DataFusionError::Execution(_)
        | DataFusionError::ExecutionJoin(_)
        // DataFusion most often returns "NotImplemented" when a
        // particular SQL feature is not implemented. This
        // information is useful to the user who may be able to
        // express their query using different syntax that is implemented.
        //
        // the grpc / tonic "NotImplemented" code typically means
        // that the client called an API endpoint that wasn't
        // implemented.
        //
        // See examples in:
        // https://github.com/apache/arrow-datafusion/search?q=NotImplemented
        | DataFusionError::NotImplemented(_)
        | DataFusionError::Plan(_) => tonic::Code::InvalidArgument,
        // cases handled by `find_root` / `Error::source`
        DataFusionError::Collection(_)
        | DataFusionError::Context(_,_)
        | DataFusionError::Diagnostic(_, _)
        | DataFusionError::Shared(_) => unreachable!("handled in chain traversal above"),
        // External errors are mostly traversed by the DataFusion already except for some IOx errors
        DataFusionError::ArrowError(e, _) => arrow_error_to_tonic_code(e),
        DataFusionError::External(e) => {
            dyn_error_to_tonic_code(e.as_ref())
        }
        DataFusionError::ObjectStore(e) => {
            object_store_error_to_tonic_code(e)
        }
        DataFusionError::ParquetError(e) => parquet_error_to_tonic_code(e),
        // Map as many as possible back into user visible
        // (non internal) errors and only treat the ones
        // the user likely can't do anything about as internal
        DataFusionError::Configuration(_)
        | DataFusionError::IoError(_)
        // Substrait errors come from internal code and are unused
        // with DataFusion at the moment
        | DataFusionError::Substrait(_)
        | DataFusionError::Internal(_) => tonic::Code::Internal,
        // explicitly don't have a catchall here so any
        // newly added DataFusion error will raise a compiler error for us to address
    }
}

/// Translate [`FlightError`] to [tonic code](tonic::Status).
///
/// This is done by traversing the error chain, similar to [`datafusion_error_to_tonic_code`].
pub fn flight_error_to_tonic_code(e: &FlightError) -> tonic::Code {
    match e {
        FlightError::Arrow(e) => arrow_error_to_tonic_code(e),
        FlightError::NotYetImplemented(_) => tonic::Code::Unimplemented,
        FlightError::Tonic(status) => status.code(),
        FlightError::ProtocolError(_) | FlightError::DecodeError(_) => tonic::Code::Internal,
        FlightError::ExternalError(e) => dyn_error_to_tonic_code(e.as_ref()),
    }
}

fn arrow_error_to_tonic_code(e: &ArrowError) -> tonic::Code {
    match e {
        ArrowError::NotYetImplemented(_) => tonic::Code::Unimplemented,
        ArrowError::ExternalError(e) => dyn_error_to_tonic_code(e.as_ref()),
        ArrowError::CastError(_)
        | ArrowError::MemoryError(_)
        | ArrowError::ParseError(_)
        | ArrowError::SchemaError(_)
        | ArrowError::ComputeError(_)
        | ArrowError::DivideByZero
        | ArrowError::ArithmeticOverflow(_)
        | ArrowError::OffsetOverflowError(_)
        | ArrowError::CsvError(_)
        | ArrowError::JsonError(_)
        | ArrowError::IoError(_, _)
        | ArrowError::IpcError(_)
        | ArrowError::InvalidArgumentError(_)
        | ArrowError::ParquetError(_)
        | ArrowError::CDataInterface(_)
        | ArrowError::DictionaryKeyOverflowError
        | ArrowError::RunEndIndexOverflowError => tonic::Code::InvalidArgument,
    }
}

/// Translate [`ParquetError`] to [tonic code](tonic::Status).
///
/// This is done by traversing the error chain, similar to [`datafusion_error_to_tonic_code`].
fn parquet_error_to_tonic_code(e: &ParquetError) -> tonic::Code {
    match e {
        ParquetError::External(e) => dyn_error_to_tonic_code(e.as_ref()),
        ParquetError::NYI(_) => tonic::Code::Unimplemented,
        ParquetError::IndexOutOfBound(_, _) => tonic::Code::OutOfRange,
        ParquetError::EOF(_)
        | ParquetError::ArrowError(_)
        | ParquetError::General(_)
        | ParquetError::NeedMoreData(_)
        | _ => tonic::Code::Internal,
    }
}

fn dyn_error_to_tonic_code(e: &(dyn std::error::Error + Send + Sync + 'static)) -> tonic::Code {
    if let Some(e) = e.downcast_ref::<ArrowError>() {
        arrow_error_to_tonic_code(e)
    } else if let Some(e) = e.downcast_ref::<DataFusionError>() {
        datafusion_error_to_tonic_code(e)
    } else if let Some(e) = e.downcast_ref::<executor::JobError>() {
        executor_error_to_tonic_code(e)
    } else if let Some(e) = e.downcast_ref::<FlightError>() {
        flight_error_to_tonic_code(e)
    } else if let Some(e) = e.downcast_ref::<object_store::Error>() {
        object_store_error_to_tonic_code(e)
    } else if let Some(e) = e.downcast_ref::<ParquetError>() {
        parquet_error_to_tonic_code(e)
    } else {
        // All other, unclassified cases are signalled as "internal error" to the user since they cannot do
        // anything about it (except for reporting a bug). Note that DataFusion "external" error is only from
        // DataFusion's PoV, not from a users PoV.
        tonic::Code::Internal
    }
}

fn executor_error_to_tonic_code(e: &executor::JobError) -> tonic::Code {
    use executor::JobError;

    match e {
        JobError::WorkerGone => tonic::Code::Unavailable,
        JobError::Panic { .. } => tonic::Code::Internal,
    }
}

fn object_store_error_to_tonic_code(e: &object_store::Error) -> tonic::Code {
    use object_store::Error;

    match e {
        Error::Generic { source, .. } => dyn_error_to_tonic_code(source.as_ref()),
        // these are all errors that the user should never see
        Error::NotFound { .. }
        | Error::InvalidPath { .. }
        | Error::JoinError { .. }
        | Error::NotSupported { .. }
        | Error::AlreadyExists { .. }
        | Error::Precondition { .. }
        | Error::NotModified { .. }
        | Error::NotImplemented
        | Error::PermissionDenied { .. }
        | Error::Unauthenticated { .. }
        | Error::UnknownConfigurationKey { .. } => tonic::Code::Internal,
        // enum is non exhaustive so it is not possible to match all variants here
        _ => tonic::Code::Internal,
    }
}

#[cfg(test)]
mod test {
    use datafusion::sql::sqlparser::parser::ParserError;
    use executor::JobError;

    use super::*;

    #[test]
    fn test_error_translation() {
        let s = "foo".to_string();

        // this is basically a second implementation of the translation table to help avoid mistakes
        do_transl_test(
            DataFusionError::ResourcesExhausted(s.clone()),
            tonic::Code::ResourceExhausted,
        );

        let e = ParserError::ParserError(s.clone());
        do_transl_test(
            DataFusionError::SQL(Box::new(e), None),
            tonic::Code::InvalidArgument,
        );

        do_transl_test(
            DataFusionError::NotImplemented(s.clone()),
            tonic::Code::InvalidArgument,
        );
        do_transl_test(
            DataFusionError::Plan(s.clone()),
            tonic::Code::InvalidArgument,
        );

        do_transl_test(DataFusionError::Internal(s.clone()), tonic::Code::Internal);

        // traversal
        do_transl_test(
            DataFusionError::Context(
                "it happened!".to_string(),
                Box::new(DataFusionError::ResourcesExhausted("foo".to_string())),
            ),
            tonic::Code::ResourceExhausted,
        );

        // arrow errors
        do_transl_test(
            DataFusionError::ArrowError(
                Box::new(ArrowError::NotYetImplemented("foo".to_string())),
                None,
            ),
            tonic::Code::Unimplemented,
        );

        // flight errors
        do_transl_test(
            FlightError::NotYetImplemented("foo".to_string()),
            tonic::Code::Unimplemented,
        );
        do_transl_test(
            FlightError::Tonic(Box::new(tonic::Status::new(tonic::Code::DataLoss, "foo"))),
            tonic::Code::DataLoss,
        );
        do_transl_test(
            FlightError::ExternalError(Box::new(ArrowError::NotYetImplemented("foo".to_string()))),
            tonic::Code::Unimplemented,
        );
        do_transl_test(
            FlightError::ExternalError(Box::new(DataFusionError::External(Box::new(
                DataFusionError::ResourcesExhausted(s.clone()),
            )))),
            tonic::Code::ResourceExhausted,
        );
        do_transl_test(
            FlightError::ExternalError(Box::new(DataFusionError::ObjectStore(Box::new(
                object_store::Error::Generic {
                    store: "foo",
                    source: Box::new(DataFusionError::Plan(s.clone())),
                },
            )))),
            tonic::Code::InvalidArgument,
        );

        // inspect "external" errors
        do_transl_test(
            DataFusionError::External(s.clone().into()),
            tonic::Code::Internal,
        );
        do_transl_test(
            DataFusionError::External(Box::new(executor::JobError::Panic { msg: s.clone() })),
            tonic::Code::Internal,
        );
        do_transl_test(
            DataFusionError::External(Box::new(executor::JobError::WorkerGone)),
            tonic::Code::Unavailable,
        );
        do_transl_test(
            DataFusionError::Context(
                "ctx".into(),
                Box::new(DataFusionError::External(Box::new(
                    executor::JobError::WorkerGone,
                ))),
            ),
            tonic::Code::Unavailable,
        );
        do_transl_test(
            FlightError::Arrow(ArrowError::ExternalError(Box::new(
                DataFusionError::ArrowError(
                    Box::new(ArrowError::ExternalError(Box::new(ParquetError::External(Box::new(
                        ArrowError::ComputeError(
                            "Error evaluating filter predicate: ArrowError(CastError(\"Cannot cast string 'val1' to value of Int64 type\"), None)".to_string(),
                        ),
                    ))))),
                    None,
                ),
            ))),
            tonic::Code::InvalidArgument,
        );

        // object store errors
        do_transl_test(
            DataFusionError::ObjectStore(Box::new(object_store::Error::Generic {
                store: "foo",
                source: Box::new(DataFusionError::Plan(s.clone())),
            })),
            tonic::Code::InvalidArgument,
        );
        do_transl_test(
            DataFusionError::External(Box::new(object_store::Error::Generic {
                store: "foo",
                source: Box::new(DataFusionError::Plan(s.clone())),
            })),
            tonic::Code::InvalidArgument,
        );

        // all dyn errors in a row
        do_transl_test(
            DataFusionError::External(Box::new(ArrowError::ExternalError(Box::new(
                FlightError::ExternalError(Box::new(JobError::WorkerGone)),
            )))),
            tonic::Code::Unavailable,
        );

        // make sure that the last s.clone() is OK
        drop(s);
    }

    fn do_transl_test<E>(e: E, code: tonic::Code)
    where
        E: Translate,
    {
        assert_eq!(e.to_code(), code);
    }

    trait Translate {
        fn to_code(&self) -> tonic::Code;
    }

    impl Translate for DataFusionError {
        fn to_code(&self) -> tonic::Code {
            datafusion_error_to_tonic_code(self)
        }
    }

    impl Translate for FlightError {
        fn to_code(&self) -> tonic::Code {
            flight_error_to_tonic_code(self)
        }
    }
}
