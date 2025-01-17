use assert_cmd::cargo::CommandCargoExt;
use std::process::Command;
use std::process::Stdio;

pub struct CLI;

impl CLI {
    pub fn run(args: &[&str]) -> String {
        String::from_utf8(
            Command::cargo_bin("influxdb3")
                .unwrap()
                .args(args)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap()
                .wait_with_output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .into()
    }
}
