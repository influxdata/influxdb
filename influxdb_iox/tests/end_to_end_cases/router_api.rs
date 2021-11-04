use influxdb_iox_client::router::generated_types::{QuerySinks, Router};

use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::rand_name,
};

#[tokio::test]
async fn test_router_crud() {
    let server_fixture = ServerFixture::create_shared(ServerType::Router).await;
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
