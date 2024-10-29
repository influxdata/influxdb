use crate::server::TestServer;
use hyper::StatusCode;
use influxdb3_client::Error;
use influxdb3_client::Precision;

#[tokio::test]
async fn limits() -> Result<(), Error> {
    let server = TestServer::spawn().await;

    // Test that a server can't have more than 5 DBs
    for db in ["one", "two", "three", "four", "five"] {
        server
            .write_lp_to_db(
                db,
                "cpu,host=s1,region=us-east usage=0.9 1\n",
                Precision::Nanosecond,
            )
            .await?;
    }

    let Err(Error::ApiError { code, .. }) = server
        .write_lp_to_db(
            "six",
            "cpu,host=s1,region=us-east usage=0.9 1\n",
            Precision::Nanosecond,
        )
        .await
    else {
        panic!("Did not error when adding 6th db");
    };
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY);

    // Test that the server can't have more than 2000 tables
    // First create the other needed 1995 tables
    let table_lp = (0..1995).fold(String::new(), |mut acc, i| {
        acc.push_str("cpu");
        acc.push_str(&i.to_string());
        acc.push_str(",host=s1,region=us-east usage=0.9 1\n");
        acc
    });

    server
        .write_lp_to_db("one", &table_lp, Precision::Nanosecond)
        .await?;

    let Err(Error::ApiError { code, .. }) = server
        .write_lp_to_db(
            "six",
            "cpu2000,host=s1,region=us-east usage=0.9 1\n",
            Precision::Nanosecond,
        )
        .await
    else {
        panic!("Did not error when adding 2001st table");
    };
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY);

    // Test that we can't add a row 500 columns long
    let mut lp_500 = String::from("cpu,host=foo,region=bar usage=2");
    let mut lp_501 = String::from("cpu,host=foo,region=bar usage=2");
    for i in 5..=500 {
        let column = format!(",column{}=1", i);
        lp_500.push_str(&column);
        lp_501.push_str(&column);
    }
    lp_500.push_str(" 0\n");
    lp_501.push_str(",column501=1 0\n");

    server
        .write_lp_to_db("one", &lp_500, Precision::Nanosecond)
        .await?;

    let Err(Error::ApiError { code, .. }) = server
        .write_lp_to_db("one", &lp_501, Precision::Nanosecond)
        .await
    else {
        panic!("Did not error when adding 501st column");
    };
    assert_eq!(code, StatusCode::BAD_REQUEST);

    Ok(())
}
