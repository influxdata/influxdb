use parking_lot::Mutex;
use reqwest::StatusCode;
use std::env;
use std::mem;
use std::panic;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;

struct DropCommand {
    cmd: Option<Child>,
}

impl DropCommand {
    const fn new(cmd: Child) -> Self {
        Self { cmd: Some(cmd) }
    }

    fn kill(&mut self) {
        let mut cmd = self.cmd.take().unwrap();
        cmd.kill().unwrap();
        mem::drop(cmd);
    }
}

static COMMAND: Mutex<Option<DropCommand>> = parking_lot::const_mutex(None);

#[tokio::test]
async fn auth() {
    const HASHED_TOKEN: &str = "784e3a542d6e29ff4b74548b24a9adb31752e15010a430dc214f7acf1a701cb9ec58bbc4b1bc6620c070e778144c8c9129d4e51e1bb5ee29f0f460fa002a1694";
    const TOKEN: &str = "apiv3_1be71c8fd444ef4139348e551bb4beb343079b0e9f316bacccc8355ee27078abf362158f0c7fd24b840f5c6ecc38de7eadb995a7d41b312f253ac3f147ce4d6b";
    // The binary is made before testing so we have access to it
    let bin_path = {
        let mut bin_path = env::current_exe().unwrap();
        bin_path.pop();
        bin_path.pop();
        bin_path.join("influxdb3")
    };
    let server = DropCommand::new(
        Command::new(bin_path)
            .args([
                "serve",
                "--object-store",
                "memory",
                "--bearer-token",
                HASHED_TOKEN,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Was able to spawn a server"),
    );

    *COMMAND.lock() = Some(server);

    let current_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        COMMAND.lock().take().unwrap().kill();
        current_hook(info);
    }));

    let client = reqwest::Client::new();

    // Wait for the server to come up
    while client
        .get("http://127.0.0.1:8181/health")
        .bearer_auth(TOKEN)
        .send()
        .await
        .is_err()
    {}

    assert_eq!(
        client
            .post("http://127.0.0.1:8181/api/v3/write_lp?db=foo")
            .body("cpu,host=a val=1i 123")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post("http://127.0.0.1:8181/api/v3/write_lp?db=foo")
            .body("cpu,host=a val=1i 123")
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // Malformed Header Tests
    // Test that there is an extra string after the token foo
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .header("Authorization", format!("Bearer {TOKEN} whee"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .header("Authorization", format!("bearer {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .header("Authorization", "Bearer")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .header("Authorizon", format!("Bearer {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    COMMAND.lock().take().unwrap().kill();
}
