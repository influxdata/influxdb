use std::{
    fmt::Display,
    ops::Range,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures::{
    Stream,
    stream::{BoxStream, StreamExt},
};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use tokio::sync::Barrier;

/// [`GetOptions`] doesn't impl PartialEq or Clone.
#[derive(Debug, Default)]
pub struct WrappedGetOptions(pub(crate) GetOptions);

impl PartialEq for WrappedGetOptions {
    fn eq(&self, other: &Self) -> bool {
        let GetOptions {
            if_match: self_if_match,
            if_none_match: self_if_none_match,
            if_modified_since: self_if_modified_since,
            if_unmodified_since: self_if_unmodified_since,
            range: self_range,
            version: self_version,
            head: self_head,
            extensions: self_extensions,
        } = &self.0;
        let GetOptions {
            if_match: other_if_match,
            if_none_match: other_if_none_match,
            if_modified_since: other_if_modified_since,
            if_unmodified_since: other_if_unmodified_since,
            range: other_range,
            version: other_version,
            head: other_head,
            extensions: other_extensions,
        } = &other.0;

        (self_if_match == other_if_match)
            && (self_if_none_match == other_if_none_match)
            && (self_if_modified_since == other_if_modified_since)
            && (self_if_unmodified_since == other_if_unmodified_since)
            && (self_range == other_range)
            && (self_version == other_version)
            && (self_head == other_head)
            // `len` is the only thing we can check without knowing concrete types
            && (self_extensions.len() == other_extensions.len())
    }
}

impl From<GetOptions> for WrappedGetOptions {
    fn from(options: GetOptions) -> Self {
        Self(options)
    }
}

impl Clone for WrappedGetOptions {
    fn clone(&self) -> Self {
        Self(GetOptions {
            if_match: self.0.if_match.clone(),
            if_none_match: self.0.if_none_match.clone(),
            if_modified_since: self.0.if_modified_since,
            if_unmodified_since: self.0.if_unmodified_since,
            range: self.0.range.clone(),
            version: self.0.version.clone(),
            head: self.0.head,
            extensions: self.0.extensions.clone(),
        })
    }
}

/// Wrapper for PutPayload that implements PartialEQ
#[derive(Debug, Clone)]
pub enum PutPayloadWrapper {
    PutPayload(PutPayload),
    /// Ignore the contents when comparing parameters.
    Ignore,
}

impl PartialEq for PutPayloadWrapper {
    fn eq(&self, other: &Self) -> bool {
        if let PutPayloadWrapper::PutPayload(self_data) = self
            && let PutPayloadWrapper::PutPayload(other_data) = other
        {
            // for test, just copy the bytes
            let self_bytes: Bytes = self_data.clone().into();
            let other_bytes: Bytes = other_data.clone().into();
            self_bytes == other_bytes
        } else {
            // Always true if either are Ignore
            true
        }
    }
}

impl From<PutPayload> for PutPayloadWrapper {
    fn from(payload: PutPayload) -> Self {
        Self::PutPayload(payload)
    }
}

impl From<Vec<u8>> for PutPayloadWrapper {
    fn from(payload: Vec<u8>) -> Self {
        Self::PutPayload(PutPayload::from(payload))
    }
}

/// Wraps [`BoxStream`] to add `Debug`
pub struct BoxStreamWrapper<T>(BoxStream<'static, T>);

impl<T> From<BoxStream<'static, T>> for BoxStreamWrapper<T> {
    fn from(stream: BoxStream<'static, T>) -> Self {
        Self(stream)
    }
}

impl<T> From<BoxStreamWrapper<T>> for BoxStream<'static, T> {
    fn from(stream: BoxStreamWrapper<T>) -> Self {
        stream.0
    }
}

impl<T> std::fmt::Debug for BoxStreamWrapper<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoxStream")
    }
}

macro_rules! calls {
    (
        $((
            name = $name:ident,
            params = ($($param:ty),* $(,)?),
            res = $res:ty,
        )),*
        $(,)?
    ) => {
        #[derive(Debug)]
        #[expect(
            unused_parens,
            reason = "a single param will expand to ($param)"
        )]
        pub enum MockCall {
            $(
                $name {
                    /// The expected parameters.
                    ///
                    /// The mock call will panic if the parameters do NOT match.
                    params: ($($param),*),

                    /// Barriers that should be passed before returning.
                    ///
                    /// [`Barrier::wait`] will be called in order.
                    barriers: Vec<Arc<Barrier>>,

                    /// Mocked return call.
                    res: $res,
                },
            )*
        }

        impl MockCall {
            fn name(&self) -> &'static str {
                match self {
                    $(
                        Self::$name{..} => ::std::stringify!($name),
                    )*
                }
            }
        }
    };
}

calls!(
    (
        name = Put,
        params = (Path, PutPayloadWrapper),
        res = Result<PutResult>,
    ),
    (
        name = PutOpts,
        params = (Path, PutPayloadWrapper, PutOptions),
        res = Result<PutResult>,
    ),
    (
        name = PutMultipart,
        params = (Path),
        res = Result<Box<dyn MultipartUpload>>,
    ),
    (
        name = PutMultipartOptions,
        params = (Path, PutMultipartOptions),
        res = Result<Box<dyn MultipartUpload>>,
    ),
    (
        name = Get,
        params = (Path),
        res = Result<GetResult>,
    ),
    (
        name = GetOpts,
        params = (Path, WrappedGetOptions),
        res = Result<GetResult>,
    ),
    (
        name = GetRange,
        params = (Path, Range<u64>),
        res = Result<Bytes>,
    ),
    (
        name = GetRanges,
        params = (Path, Vec<Range<u64>>),
        res = Result<Vec<Bytes>>,
    ),
    (
        name = Head,
        params = (Path),
        res = Result<ObjectMeta>,
    ),
    (
        name = Delete,
        params = (Path),
        res = Result<()>,
    ),
    (
        name = DeleteStream,
        params = (),
        res = BoxStreamWrapper<Result<Path>>,
    ),
    (
        name = List,
        params = (Option<Path>),
        res = BoxStreamWrapper<Result<ObjectMeta>>,
    ),
    (
        name = ListWithOffset,
        params = (Option<Path>, Path),
        res = BoxStreamWrapper<Result<ObjectMeta>>,
    ),
    (
        name = ListWithDelimiter,
        params = (Option<Path>),
        res = Result<ListResult>,
    ),
    (
        name = Copy,
        params = (Path, Path),
        res = Result<()>,
    ),
    (
        name = Rename,
        params = (Path, Path),
        res = Result<()>,
    ),
    (
        name = CopyIfNotExists,
        params = (Path, Path),
        res = Result<()>,
    ),
    (
        name = RenameIfNotExists,
        params = (Path, Path),
        res = Result<()>,
    ),
);

#[derive(Debug, Default)]
struct MockStoreState {
    /// List of unused mocked calls and the index of when they where added.
    ///
    /// The index will be used for assertion messages so the user knows which of the mocked calls failed.
    calls: Vec<(usize, MockCall)>,

    /// Counter of mocked calls used to give every new call a new, increasing index.
    index_counter: usize,
}

#[derive(Debug, Default)]
pub struct MockStore {
    state: Mutex<MockStoreState>,
}

impl Drop for MockStore {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let state = self.state.lock().unwrap();
            if !state.calls.is_empty() {
                panic!("mocked calls left on drop: {:?}", state.calls.as_slice());
            }
        }
    }
}

impl Display for MockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mock")
    }
}

impl MockStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn mock_next(self: Arc<Self>, call: MockCall) -> Arc<Self> {
        self.mock_next_multi(vec![call])
    }

    pub fn mock_next_multi(self: Arc<Self>, calls: Vec<MockCall>) -> Arc<Self> {
        let mut state = self.state.lock().unwrap();

        for call in calls {
            let index = state.index_counter;
            state.index_counter += 1;

            state.calls.push((index, call));
        }

        drop(state);
        self
    }

    pub fn as_store(self: Arc<Self>) -> Arc<dyn ObjectStore> {
        self as Arc<dyn ObjectStore>
    }
}

macro_rules! barrier_wait {
    (stream, $barriers:ident, $res:ident) => {
        futures::stream::once(async move {
            for barrier in $barriers {
                barrier.wait().await;
            }
            $res
        })
        .flatten()
        .boxed()
    };
    (async, $barriers:ident, $res:ident) => {{
        for barrier in $barriers {
            barrier.wait().await;
        }
        $res
    }};
}

macro_rules! mock {
    (
        this = $self:ident,
        variant = $variant:ident,
        record = ($($param:ident),*),
        mode = $mode:tt,
    ) => {
        {
            // do NOT unwrap when guard is held to avoid lock poisoning
            let maybe = {
                let mut state = $self.state.lock().unwrap();
                (!state.calls.is_empty()).then(|| state.calls.remove(0))
            };

            let (idx, resp) = maybe.expect(concat!("no mocked call left but store is called with ", ::std::stringify!($variant)));

            let MockCall::$variant{params, barriers, res} = resp else {
                panic!(
                    "next response is not a {} but a {} at 0-based-index {}",
                    ::std::stringify!($variant),
                    resp.name(),
                    idx,
                );
            };

            let actual = ($($param.clone().into()),*);
            assert!(
                params == actual,
                "mocked parameters are different from the actual ones for {} at 0-based-index {}:\n\nActual:\n{:#?}\n\nExpected:\n{:#?}",
                ::std::stringify!($variant),
                idx,
                actual,
                params,
            );

            let res = res.into();

            if barriers.is_empty() {
                res
            } else {
                barrier_wait!($mode, barriers, res)
            }
        }
    };
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for MockStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        mock!(this = self, variant = Put, record = (location, payload), mode = async,)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        mock!(
            this = self,
            variant = PutOpts,
            record = (location, payload, opts),
            mode = async,
        )
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        mock!(this = self, variant = PutMultipart, record = (location), mode = async,)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        mock!(
            this = self,
            variant = PutMultipartOptions,
            record = (location, opts),
            mode = async,
        )
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        mock!(this = self, variant = Get, record = (location), mode = async,)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        mock!(this = self, variant = GetOpts, record = (location, options), mode = async,)
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        mock!(this = self, variant = GetRange, record = (location, range), mode = async,)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        mock!(
            this = self,
            variant = GetRanges,
            record = (location, ranges),
            mode = async,
        )
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        mock!(this = self, variant = Head, record = (location), mode = async,)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        mock!(this = self, variant = Delete, record = (location), mode = async,)
    }

    fn delete_stream<'a>(
        &'a self,
        _locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        mock!(
            this = self,
            variant = DeleteStream,
            record = (),
            mode = stream,
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        mock!(
            this = self,
            variant = List,
            record = (prefix),
            mode = stream,
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        mock!(
            this = self,
            variant = ListWithOffset,
            record = (prefix, offset),
            mode = stream,
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned();
        mock!(this = self, variant = ListWithDelimiter, record = (prefix), mode = async,)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        mock!(this = self, variant = Copy, record = (from, to), mode = async,)
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        mock!(this = self, variant = Rename, record = (from, to), mode = async,)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        mock!(this = self, variant = CopyIfNotExists, record = (from, to), mode = async,)
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        mock!(
            this = self,
            variant = RenameIfNotExists,
            record = (from, to),
            mode = async,
        )
    }
}

pub fn path() -> Path {
    Path::parse("path").unwrap()
}

pub fn path2() -> Path {
    Path::parse("path2").unwrap()
}

pub fn err() -> object_store::Error {
    object_store::Error::Generic {
        store: "test",
        source: "source".into(),
    }
}

pub static DATA: &[u8] = b"hello world";

pub fn object_meta() -> ObjectMeta {
    ObjectMeta {
        location: path(),
        last_modified: Default::default(),
        size: DATA.len() as u64,
        e_tag: None,
        version: None,
    }
}

pub fn not_found(path: impl Into<String>) -> object_store::Error {
    object_store::Error::NotFound {
        path: path.into(),
        source: "no source".into(),
    }
}

pub fn get_result(data: Bytes, path: &Path) -> GetResult {
    let len = data.len() as u64;
    GetResult {
        payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(data) }))),
        meta: ObjectMeta {
            location: path.clone(),
            last_modified: Default::default(),
            size: len,
            e_tag: None,
            version: None,
        },
        range: 0..len,
        attributes: Default::default(),
    }
}

pub fn panic_stream<T>() -> impl Stream<Item = T> {
    futures::stream::once(async { panic!("should not be called") })
}

pub fn multipart_upload_ok() -> Box<dyn MultipartUpload> {
    use async_trait::async_trait;
    use object_store::{Result, UploadPart};

    #[derive(Debug)]
    struct U;

    #[async_trait]
    impl MultipartUpload for U {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(futures::future::ready(Ok(())))
        }

        async fn complete(&mut self) -> Result<PutResult> {
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    Box::new(U)
}

pub fn multipart_upload_err() -> Box<dyn MultipartUpload> {
    use async_trait::async_trait;
    use object_store::{Result, UploadPart};

    #[derive(Debug)]
    struct U;

    #[async_trait]
    impl MultipartUpload for U {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(futures::future::ready(Err(err())))
        }

        async fn complete(&mut self) -> Result<PutResult> {
            Err(err())
        }

        async fn abort(&mut self) -> Result<()> {
            Err(err())
        }
    }

    Box::new(U)
}

pub fn get_result_stream() -> GetResult {
    let split_point = DATA.len() / 2;

    GetResult {
        payload: GetResultPayload::Stream(
            futures::stream::iter([&DATA[..split_point], &DATA[split_point..]])
                .map(Bytes::from_static)
                .map(Ok)
                .boxed(),
        ),
        meta: object_meta(),
        range: 0..(DATA.len() as u64),
        attributes: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug() {
        assert_eq!(
            format!("{:?}", MockStore::new()),
            "MockStore { state: Mutex { data: MockStoreState { calls: [], index_counter: 0 }, poisoned: false, .. } }",
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", MockStore::new()), "mock",);
    }

    #[tokio::test]
    async fn test_result_mocking() {
        let store = MockStore::new()
            .mock_next(MockCall::Copy {
                params: (path(), path()),
                barriers: vec![],
                res: Ok(()),
            })
            .mock_next(MockCall::Copy {
                params: (path(), path()),
                barriers: vec![],
                res: Err(err()),
            });

        store.copy(&path(), &path()).await.unwrap();
        store.copy(&path(), &path()).await.unwrap_err();
    }

    #[tokio::test]
    #[should_panic(expected = "no mocked call left but store is called with Copy")]
    async fn test_no_calls_left() {
        let store = MockStore::new().mock_next(MockCall::Copy {
            params: (path(), path()),
            barriers: vec![],
            res: Ok(()),
        });

        store.copy(&path(), &path()).await.unwrap();
        store.copy(&path(), &path()).await.ok();
    }

    #[tokio::test]
    #[should_panic(expected = "next response is not a Rename but a Copy at 0-based-index 0")]
    async fn test_wrong_calls_left() {
        let store = MockStore::new().mock_next(MockCall::Copy {
            params: (path(), path()),
            // Barrier is NOT checked.
            barriers: vec![Arc::new(Barrier::new(2))],
            res: Ok(()),
        });

        store.rename(&path(), &path()).await.ok();
    }

    #[tokio::test]
    #[should_panic(
        expected = "mocked parameters are different from the actual ones for Copy at 0-based-index 0:"
    )]
    async fn test_params_checked() {
        let path2 = Path::parse("other").unwrap();
        let store = MockStore::new().mock_next(MockCall::Copy {
            params: (path(), path2),
            // Barrier is used AFTER param checking.
            barriers: vec![Arc::new(Barrier::new(2))],
            res: Ok(()),
        });

        store.copy(&path(), &path()).await.ok();
    }

    #[test]
    #[should_panic(expected = "mocked calls left on drop")]
    fn test_calls_left_drop() {
        MockStore::new().mock_next(MockCall::Copy {
            params: (path(), path()),
            barriers: vec![],
            res: Ok(()),
        });
    }

    /// Do NOT double-panic due to the "mock calls left on drop" test because this would abort the process and
    /// potentially also hide the actual test failure -- at least the DX would be not great.
    #[test]
    #[should_panic(expected = "foo")]
    fn test_calls_left_no_double_panic() {
        let _store = MockStore::new().mock_next(MockCall::Copy {
            params: (path(), path()),
            barriers: vec![],
            res: Ok(()),
        });
        panic!("foo")
    }

    #[test]
    fn test_paths_different() {
        assert_ne!(path(), path2());
    }
}
