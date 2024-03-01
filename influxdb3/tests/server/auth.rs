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
                "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", // foo as a sha256
            ])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
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
        .bearer_auth("foo")
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
            .bearer_auth("foo")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .bearer_auth("foo")
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
            .header("Authorization", "Bearer foo whee")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get("http://127.0.0.1:8181/api/v3/query_sql?db=foo&q=select+*+from+cpu")
            .header("Authorization", "bearer foo")
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
            .header("Authorizon", "Bearer foo")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    COMMAND.lock().take().unwrap().kill();
}
