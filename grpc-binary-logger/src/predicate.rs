use http_body::Body;

/// A [`Predicate`] allows filtering requests before they get processed by a [`crate::Sink`].
pub trait Predicate: Clone {
    /// If this method returns true, the logger layer will capture gRPC frames for this request
    /// and send them to a [`crate::Sink`].
    fn should_log<B>(&self, req: &hyper::Request<B>) -> bool
    where
        B: Body;
}

#[derive(Default, Clone, Debug)]
pub(crate) struct LogAll;

impl Predicate for LogAll {
    fn should_log<B>(&self, _req: &hyper::Request<B>) -> bool
    where
        B: Body,
    {
        true
    }
}

/// A [`Predicate`] that filters out all [gRPC server reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md)
#[derive(Default, Clone, Debug, Copy)]
pub struct NoReflection;

impl Predicate for NoReflection {
    fn should_log<B>(&self, req: &hyper::Request<B>) -> bool
    where
        B: Body,
    {
        let method = req.uri().path();
        !method.starts_with("/grpc.reflection.v1alpha.ServerReflection")
    }
}
