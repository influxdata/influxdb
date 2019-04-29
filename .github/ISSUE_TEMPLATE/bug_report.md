---
name: Bug report
about: Create a report to help us improve
---

<!--

Thank you for reporting a bug in InfluxDB. 

* Please ask usage questions on the Influx Community site.
    * https://community.influxdata.com/
* Please add a :+1: or comment on a similar existing bug report instead of opening a new one.
    * https://github.com/influxdata/influxdb/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+is%3Aclosed+sort%3Aupdated-desc+label%3Akind%2Fbug+
* Please check whether the bug can be reproduced with the latest release.
* The fastest way to fix a bug is to open a Pull Request.
    * https://github.com/influxdata/influxdb/pulls

-->

__Steps to reproduce:__
List the minimal actions needed to reproduce the behavior.

1. ...
2. ...
3. ...

__Expected behavior:__
Describe what you expected to happen.

__Actual behavior:__
Describe What actually happened.

__Environment info:__

* System info: Run `uname -srm` and copy the output here
* InfluxDB version: Run `influxd version` and copy the output here
* Other relevant environment details: Container runtime, disk info, etc

__Config:__
Copy any non-default config values here or attach the full config as a gist or file.

<!-- The following sections are only required if relevant. -->

__Logs:__
Include snippet of errors in log.

__Performance:__
Generate profiles with the following commands for bugs related to performance, locking, out of memory (OOM), etc.

```sh
# Commands should be run when the bug is actively.
# Note: This command will run for at least 30 seconds.
curl -o profiles.tar.gz "http://localhost:8086/debug/pprof/all?cpu=true"
curl -o vars.txt "http://localhost:8086/debug/vars"
iostat -xd 1 30 > iostat.txt
# Attach the `profiles.tar.gz`, `vars.txt`, and `iostat.txt` output files.
```
