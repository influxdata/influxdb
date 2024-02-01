//! The remote API for the catalog cache

use crate::CacheKey;
use hyper::http::HeaderName;

pub mod client;

pub mod quorum;

pub mod server;

pub mod list;

/// The header used to encode the generation in a get response
static GENERATION: HeaderName = HeaderName::from_static("x-influx-generation");

/// Defines the mapping to HTTP paths for given request types
#[derive(Debug, Eq, PartialEq)]
enum RequestPath {
    /// A request addressing a resource identified by [`CacheKey`]
    Resource(CacheKey),
    /// A list request
    List,
}

impl RequestPath {
    fn parse(s: &str) -> Option<Self> {
        let s = s.strip_prefix('/').unwrap_or(s);
        if s == "v1/" {
            return Some(Self::List);
        }

        let (prefix, value) = s.rsplit_once('/')?;
        let value = u64::from_str_radix(value, 16).ok()?;
        match prefix {
            "v1/n" => Some(Self::Resource(CacheKey::Namespace(value as i64))),
            "v1/t" => Some(Self::Resource(CacheKey::Table(value as i64))),
            "v1/p" => Some(Self::Resource(CacheKey::Partition(value as i64))),
            _ => None,
        }
    }
}

impl std::fmt::Display for RequestPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List => write!(f, "v1/"),
            Self::Resource(CacheKey::Namespace(v)) => write!(f, "v1/n/{v:016x}"),
            Self::Resource(CacheKey::Table(v)) => write!(f, "v1/t/{v:016x}"),
            Self::Resource(CacheKey::Partition(v)) => write!(f, "v1/p/{v:016x}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::list::ListEntry;
    use crate::api::server::test_util::TestCacheServer;
    use crate::api::RequestPath;
    use crate::{CacheKey, CacheValue};
    use futures::TryStreamExt;
    use std::collections::HashSet;

    #[test]
    fn test_request_path() {
        let paths = [
            RequestPath::List,
            RequestPath::Resource(CacheKey::Partition(12)),
            RequestPath::Resource(CacheKey::Partition(i64::MAX)),
            RequestPath::Resource(CacheKey::Partition(i64::MIN)),
            RequestPath::Resource(CacheKey::Namespace(12)),
            RequestPath::Resource(CacheKey::Namespace(i64::MAX)),
            RequestPath::Resource(CacheKey::Namespace(i64::MIN)),
            RequestPath::Resource(CacheKey::Table(12)),
            RequestPath::Resource(CacheKey::Table(i64::MAX)),
            RequestPath::Resource(CacheKey::Table(i64::MIN)),
        ];

        let mut set = HashSet::with_capacity(paths.len());
        for path in paths {
            let s = path.to_string();
            let back = RequestPath::parse(&s).unwrap();
            assert_eq!(back, path);
            assert!(set.insert(s), "should be unique");
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let serve = TestCacheServer::bind_ephemeral();
        let client = serve.client();

        let key = CacheKey::Partition(1);

        let v1 = CacheValue::new("1".into(), 2);
        assert!(client.put(key, &v1).await.unwrap());

        let returned = client.get(key).await.unwrap().unwrap();
        assert_eq!(v1, returned);

        // Duplicate upsert ignored
        assert!(!client.put(key, &v1).await.unwrap());

        // Stale upsert ignored
        let v2 = CacheValue::new("2".into(), 1);
        assert!(!client.put(key, &v2).await.unwrap());

        let returned = client.get(key).await.unwrap().unwrap();
        assert_eq!(v1, returned);

        let v3 = CacheValue::new("3".into(), 3);
        assert!(client.put(key, &v3).await.unwrap());

        let returned = client.get(key).await.unwrap().unwrap();
        assert_eq!(v3, returned);

        let key2 = CacheKey::Partition(5);
        assert!(client.put(key2, &v1).await.unwrap());

        let mut result = client.list(None).try_collect::<Vec<_>>().await.unwrap();
        result.sort_unstable_by_key(|entry| entry.key());

        let expected = vec![ListEntry::new(key, v3), ListEntry::new(key2, v1)];
        assert_eq!(result, expected);

        serve.shutdown().await;
    }

    #[tokio::test]
    async fn test_list_size() {
        let serve = TestCacheServer::bind_ephemeral();
        let client = serve.client();

        let v1 = CacheValue::new("123".into(), 2);
        client.put(CacheKey::Table(1), &v1).await.unwrap();

        let v2 = CacheValue::new("13".into(), 2);
        client.put(CacheKey::Table(2), &v2).await.unwrap();

        let v3 = CacheValue::new("1".into(), 2);
        client.put(CacheKey::Table(3), &v3).await.unwrap();

        let mut res = client.list(Some(2)).try_collect::<Vec<_>>().await.unwrap();
        res.sort_unstable_by_key(|x| x.key());

        assert_eq!(res.len(), 3);

        assert_eq!(res[0].value(), None);
        assert_eq!(res[1].value(), Some(&v2.data));
        assert_eq!(res[2].value(), Some(&v3.data));

        let mut res = client.list(Some(3)).try_collect::<Vec<_>>().await.unwrap();
        res.sort_unstable_by_key(|x| x.key());

        assert_eq!(res[0].value(), Some(&v1.data));
        assert_eq!(res[1].value(), Some(&v2.data));
        assert_eq!(res[2].value(), Some(&v3.data));
    }
}
