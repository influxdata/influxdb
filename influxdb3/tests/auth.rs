use reqwest::StatusCode;
use std::env;
use std::process::Command;
use std::process::Stdio;

#[tokio::test]
async fn auth() {
    // The binary is made before testing so we have access to it
    let bin_path = {
        let mut bin_path = env::current_exe().unwrap();
        bin_path.pop();
        bin_path.pop();
        bin_path.join("influxdb3")
    };
    let mut server = Command::new(bin_path)
        .args([
            "serve",
            "--bearer-token",
            "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", // foo as a sha256
        ])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("Was able to spawn a server");
    let client = reqwest::Client::new();

    // Wait for the server to come up
    while client
        .get("http://127.0.0.1:8181/health")
        .bearer_auth("foo")
        .send()
        .await
        .is_err()
    {}

    match client
        .post("http://127.0.0.1:8181/api/v3/write_lp?db=foo")
        .body("cpu,host=a val=1i 123")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::UNAUTHORIZED {
                server.kill().unwrap();
                panic!("status code was not unauthorized: {:#?}", resp);
            }
        }
    }
    match client
        .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::UNAUTHORIZED {
                server.kill().unwrap();
                panic!("status code was not unauthorized: {:#?}", resp);
            }
        }
    }
    match client
        .post("http://127.0.0.1:8181/api/v3/write_lp?db=foo")
        .body("cpu,host=a val=1i 123")
        .bearer_auth("foo")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::OK {
                server.kill().unwrap();
                panic!("status code was not ok: {:#?}", resp);
            }
        }
    }

    match client
        .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
        .bearer_auth("foo")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::OK {
                server.kill().unwrap();
                panic!("status code was not ok: {:#?}", resp);
            }
        }
    }

    // Malformed Header Tests
    // Test that there is an extra string after the token foo
    match client
        .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
        .header("Authorization", "Bearer foo whee")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::BAD_REQUEST {
                server.kill().unwrap();
                panic!("status code was not bad request: {:#?}", resp);
            }
        }
    }
    // Test that Bearer is not capitalized properly
    match client
        .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
        .header("Authorization", "bearer foo")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::BAD_REQUEST {
                server.kill().unwrap();
                panic!("status code was not bad request: {:#?}", resp);
            }
        }
    }
    // Test that there is not a token after Bearer
    match client
        .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
        .header("Authorization", "Bearer")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::BAD_REQUEST {
                server.kill().unwrap();
                panic!("status code was not bad request: {:#?}", resp);
            }
        }
    }
    // Test that the request is denied with a bad Authorization header
    match client
        .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
        .header("Authorizon", "Bearer foo")
        .send()
        .await
    {
        Err(e) => {
            server.kill().unwrap();
            panic!("request errored out: {e}");
        }
        Ok(resp) => {
            if resp.status() != StatusCode::UNAUTHORIZED {
                server.kill().unwrap();
                panic!("status code was not unauthorized: {:#?}", resp);
            }
        }
    }

    server.kill().unwrap();
}
