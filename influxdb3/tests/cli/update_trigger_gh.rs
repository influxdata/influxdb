//! `update trigger --path gh:...` re-fetches the plugin of a running trigger
//! (influxdb_pro#7063).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::Value;
use test_helpers::assert_contains;
use test_helpers::tempfile::TempDir;

use crate::server::{ConfigProvider, TestServer};

/// A plugin repository serving one plugin for every path: a request plugin that answers with a
/// marker. It answers 404 while it holds no plugin.
///
/// It serves from a thread of its own: the tests block on CLI commands whose server fetches from
/// it, so it cannot share their runtime.
struct PluginRepo {
    url: String,
    plugin: Arc<Mutex<Option<String>>>,
    fetches: Arc<AtomicUsize>,
}

impl PluginRepo {
    fn serving(marker: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let repo = Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            plugin: Arc::new(Mutex::new(None)),
            fetches: Arc::new(AtomicUsize::new(0)),
        };
        repo.serve(marker);
        let (plugin, fetches) = (Arc::clone(&repo.plugin), Arc::clone(&repo.fetches));
        std::thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let mut request = Vec::new();
                let mut chunk = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut chunk) {
                        Ok(n) if n > 0 => request.extend_from_slice(&chunk[..n]),
                        _ => break,
                    }
                }
                fetches.fetch_add(1, Ordering::SeqCst);
                let response = match plugin.lock().unwrap().clone() {
                    Some(plugin) => format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{plugin}",
                        plugin.len()
                    ),
                    None => {
                        "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            .to_string()
                    }
                };
                let _ = stream.write_all(response.as_bytes());
            }
        });
        repo
    }

    fn serve(&self, marker: &str) {
        *self.plugin.lock().unwrap() = Some(format!(
            "def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):\n    return {{\"marker\": \"{marker}\"}}\n"
        ));
    }

    fn remove(&self) {
        *self.plugin.lock().unwrap() = None;
    }

    fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

/// A server with database `foo` and the request trigger `gh_trigger`, which runs
/// `gh:test/plugin.py` from `repo` and answers at `/api/v3/engine/refresh`.
async fn server_with_gh_trigger(repo: &PluginRepo) -> (TestServer, TempDir) {
    let plugin_dir = TempDir::new().unwrap();
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_str().unwrap())
        .with_plugin_repo(&repo.url)
        .spawn()
        .await;
    server.create_database("foo").run().unwrap();
    server
        .create_trigger("foo", "gh_trigger", "gh:test/plugin.py", "request:refresh")
        .run()
        .unwrap();
    (server, plugin_dir)
}

/// Calls the request trigger and returns the marker its plugin answers with. A trigger that was
/// only just created may not be routed yet, so this waits for it.
async fn call_marker_plugin(server: &TestServer) -> String {
    let endpoint = format!("{}/api/v3/engine/refresh", server.client_addr());
    for _ in 0..100 {
        let response = server.http_client().post(&endpoint).send().await.unwrap();
        if response.status() == StatusCode::OK {
            let body: Value = response.json().await.unwrap();
            return body["marker"].as_str().unwrap().to_string();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the request trigger never answered");
}

fn update_trigger_path(
    server: &TestServer,
    trigger_name: &str,
    path: &str,
) -> anyhow::Result<String> {
    server.run(
        vec![
            "update",
            "trigger",
            "--tls-ca",
            "../testing-certs/rootCA.pem",
        ],
        &[
            "--database",
            "foo",
            "--trigger-name",
            trigger_name,
            "--path",
            path,
        ],
    )
}

#[test_log::test(tokio::test)]
async fn test_update_trigger_refetches_a_gh_plugin() {
    let repo = PluginRepo::serving("v1");
    let (server, _plugin_dir) = server_with_gh_trigger(&repo).await;
    assert_eq!(call_marker_plugin(&server).await, "v1");
    // Fetched when the trigger was created, and by its first run.
    assert_eq!(repo.fetches(), 2);

    // The repository changes. A running trigger does not follow by itself.
    repo.serve("v2");
    assert_eq!(call_marker_plugin(&server).await, "v1");
    assert_eq!(repo.fetches(), 2);

    let output = update_trigger_path(&server, "gh_trigger", "gh:test/plugin.py").unwrap();
    assert_contains!(output, "updated successfully");
    assert_eq!(call_marker_plugin(&server).await, "v2");
    assert_eq!(call_marker_plugin(&server).await, "v2");
    // Fetched to check the update, and by the one run after it.
    assert_eq!(repo.fetches(), 4);

    // Only the trigger's own plugin can be named, and nothing is fetched for another.
    let err = update_trigger_path(&server, "gh_trigger", "gh:test/other.py")
        .unwrap_err()
        .to_string();
    assert_contains!(&err, "400 Bad Request");
    assert_contains!(&err, "runs plugin 'gh:test/plugin.py'");
    let err = update_trigger_path(&server, "nope", "gh:test/plugin.py")
        .unwrap_err()
        .to_string();
    assert_contains!(&err, "404 Not Found");
    assert_eq!(repo.fetches(), 4);

    // A fetch that fails is an error, and the trigger stays on the code it has.
    repo.remove();
    let err = update_trigger_path(&server, "gh_trigger", "gh:test/plugin.py")
        .unwrap_err()
        .to_string();
    assert_contains!(&err, "error fetching plugin from repository");
    assert_eq!(repo.fetches(), 5);
    assert_eq!(call_marker_plugin(&server).await, "v2");
    assert_eq!(repo.fetches(), 5);
}
