use influxdb_iox_client::router::{
    generated_types::{Matcher, MatcherToShard, QuerySinks, Router, ShardConfig},
    UpdateRouterError,
};
use test_helpers::assert_error;

use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::rand_name,
};

#[tokio::test]
async fn test_router_crud() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Router).await;
    let mut client = server_fixture.router_client();

    let router_name_a = rand_name();
    let router_name_b = rand_name();

    let (router_name_a, router_name_b) = if router_name_a < router_name_b {
        (router_name_a, router_name_b)
    } else {
        (router_name_b, router_name_a)
    };
    let cfg_foo_1 = Router {
        name: router_name_b.clone(),
        write_sharder: Default::default(),
        write_sinks: Default::default(),
        query_sinks: Default::default(),
    };
    let cfg_foo_2 = Router {
        query_sinks: Some(QuerySinks {
            grpc_remotes: vec![1],
        }),
        ..cfg_foo_1.clone()
    };
    assert_ne!(cfg_foo_1, cfg_foo_2);

    let cfg_bar = Router {
        name: router_name_a,
        write_sharder: Default::default(),
        write_sinks: Default::default(),
        query_sinks: Default::default(),
    };

    // no routers
    assert_eq!(client.list_routers().await.unwrap().len(), 0);
    client.delete_router(&router_name_b).await.unwrap();

    // add routers
    client.update_router(cfg_foo_1.clone()).await.unwrap();
    client.update_router(cfg_bar.clone()).await.unwrap();
    let routers = client.list_routers().await.unwrap();
    assert_eq!(routers.len(), 2);
    assert_eq!(&routers[0], &cfg_bar);
    assert_eq!(&routers[1], &cfg_foo_1);

    // update router
    client.update_router(cfg_foo_2.clone()).await.unwrap();
    let routers = client.list_routers().await.unwrap();
    assert_eq!(routers.len(), 2);
    assert_eq!(&routers[0], &cfg_bar);
    assert_eq!(&routers[1], &cfg_foo_2);

    // delete routers
    client.delete_router(&router_name_b).await.unwrap();
    let routers = client.list_routers().await.unwrap();
    assert_eq!(routers.len(), 1);
    assert_eq!(&routers[0], &cfg_bar);
    client.delete_router(&router_name_b).await.unwrap();
}

#[tokio::test]
async fn test_router_update_invalid_argument() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Router).await;
    let mut client = server_fixture.router_client();

    let router_name = rand_name();

    let cfg_valid = Router {
        name: router_name.clone(),
        write_sharder: Default::default(),
        write_sinks: Default::default(),
        query_sinks: Default::default(),
    };
    let cfg_invalid = Router {
        write_sharder: Some(ShardConfig {
            specific_targets: vec![MatcherToShard {
                matcher: Some(Matcher {
                    table_name_regex: "*".to_owned(),
                }),
                shard: 1,
            }],
            hash_ring: None,
        }),
        ..cfg_valid.clone()
    };

    // invalid args don't create routers
    let res = client.update_router(cfg_invalid.clone()).await;
    assert_error!(res, UpdateRouterError::InvalidArgument(_));
    let routers = client.list_routers().await.unwrap();
    assert_eq!(routers.len(), 0);

    // invalid args don't update routesr
    client.update_router(cfg_valid.clone()).await.unwrap();
    let res = client.update_router(cfg_invalid).await;
    assert_error!(res, UpdateRouterError::InvalidArgument(_));
    let routers = client.list_routers().await.unwrap();
    assert_eq!(routers.len(), 1);
    assert_eq!(&routers[0], &cfg_valid);
}
