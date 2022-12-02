//! Routines for error handling
use datafusion::error::DataFusionError;

/// Converts a [`DataFusionError`] into the appropriate [`tonic::Code`]
///
/// Note: the goal of this function is that the "user sees the error
/// message rather than an opaque message". Typically the messages of
/// [`tonic::Code::Internal`] are not displayed to the user as they
/// result from bugs in the software rather and the user can't do
/// anything about them.
///
/// In an ideal world, it would be totally clear from a
/// [`DataFusionError`] which errors belonged in which mapping.
///
/// However, this is not always the case, so the code takes the
/// "conservative UX" approach to "show the user the message". This
/// may be at odds with the conservative approach from a security
/// perspective.
///
/// Basically because I wasn't sure they were all internal errors --
/// for example, you can get an Arrow error if you try and divide a
/// column by zero, depending on the data.
pub fn datafusion_error_to_tonic_code(e: &DataFusionError) -> tonic::Code {
    let e = e.find_root();

    match e {
        DataFusionError::ResourcesExhausted(_) => tonic::Code::ResourceExhausted,
        // Map as many as possible back into user visible (non internal) errors
        DataFusionError::SQL(_)
        | DataFusionError::SchemaError(_)
        // Execution, ArrowError and ParquetError might be due to an
        // internal error (e.g. some sort of IO error or bug) or due
        // to a user input error (e.g. you can get an Arrow error if
        // you try and divide by a column and it has zeros).
        //
        // Since we are not sure they are all internal errors we
        // classify them as InvalidArgument so the user has a chance
        // to see them
        | DataFusionError::Execution(_)
        | DataFusionError::ArrowError(_)
        | DataFusionError::ParquetError(_)
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
        DataFusionError::Context(_,_) => unreachable!("handled in chain traversal above"),
        // Map as many as possible back into user visible
        // (non internal) errors and only treat the ones
        // the user likely can't do anything about as internal
        DataFusionError::ObjectStore(_)
        | DataFusionError::IoError(_)
        // External originate from outside DataFusion’s core codebase.
        // As of 2022-10-17, these always come external object store
        // errors (e.g. misconfiguration or bad path) which would be
        // an internal error and thus we classify them as such.
        | DataFusionError::External(_)
        | DataFusionError::Internal(_) => tonic::Code::Internal,
        // explicitly don't have a catchall here so any
        // newly added DataFusion error will raise a compiler error for us to address
    }
}

#[cfg(test)]
mod test {
    use datafusion::sql::sqlparser::parser::ParserError;

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
        do_transl_test(DataFusionError::SQL(e), tonic::Code::InvalidArgument);

        do_transl_test(
            DataFusionError::NotImplemented(s.clone()),
            tonic::Code::InvalidArgument,
        );
        do_transl_test(
            DataFusionError::Plan(s.clone()),
            tonic::Code::InvalidArgument,
        );

        do_transl_test(DataFusionError::Internal(s), tonic::Code::Internal);

        // traversal
        do_transl_test(
            DataFusionError::Context(
                "it happened!".to_string(),
                Box::new(DataFusionError::ResourcesExhausted("foo".to_string())),
            ),
            tonic::Code::ResourceExhausted,
        );
    }

    fn do_transl_test(e: DataFusionError, code: tonic::Code) {
        assert_eq!(datafusion_error_to_tonic_code(&e), code);
    }
}
