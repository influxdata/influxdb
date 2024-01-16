---
name: Bug report
about: Create a report to help us improve
---

<!--

Thank you for reporting a bug in InfluxDB IOx.

Have you read the contributing section of the README? Please do if you haven't.
https://github.com/influxdata/influxdb/blob/main/README.md

* Please ask usage questions in the Influx Slack (there is an #influxdb-iox channel).
    * https://influxdata.com/slack
* Please don't open duplicate issues; use the search. If there is an existing issue please don't add "+1" or "me too" comments; only add comments with new information.
* Please check whether the bug can be reproduced with tip of main.
* The fastest way to fix a bug is to open a Pull Request.
    * https://github.com/influxdata/influxdb/pulls

-->

__Steps to reproduce:__
List the minimal actions needed to reproduce the behaviour.

1. ...
2. ...
3. ...

__Expected behaviour:__
Describe what you expected to happen.

__Actual behaviour:__
Describe What actually happened.

__Environment info:__

* Please provide the command you used to build the project, including any `RUSTFLAGS`.
* System info: Run `uname -srm` or similar and copy the output here (we want to know your OS, architecture etc).
* If you're running IOx in a containerised environment then details about that would be helpful.
* Other relevant environment details: disk info, hardware setup etc.

__Config:__
Copy any non-default config values here or attach the full config as a gist or file.

<!-- The following sections are only required if relevant. -->

__Logs:__
Include snippet of errors in logs or stack traces here.
Sometimes you can get useful information by running the program with the `RUST_BACKTRACE=full` environment variable.
Finally, the IOx server has a `-vv` for verbose logging.
