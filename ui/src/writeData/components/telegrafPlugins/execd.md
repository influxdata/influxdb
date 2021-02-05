# Execd Input Plugin

The `execd` plugin runs an external program as a long-running daemon. 
The programs must output metrics in any one of the accepted 
[Input Data Formats][] on the process's STDOUT, and is expected to
stay running. If you'd instead like the process to collect metrics and then exit,
check out the [inputs.exec][] plugin.

The `signal` can be configured to send a signal the running daemon on each
collection interval. This is used for when you want to have Telegraf notify the
plugin when it's time to run collection. STDIN is recommended, which writes a
new line to the process's STDIN.

STDERR from the process will be relayed to Telegraf as errors in the logs.

### Configuration:

```toml
[[inputs.execd]]
  ## One program to run as daemon.
  ## NOTE: process and each argument should each be their own string
  command = ["telegraf-smartctl", "-d", "/dev/sda"]

  ## Define how the process is signaled on each collection interval.
  ## Valid values are:
  ##   "none"    : Do not signal anything. (Recommended for service inputs)
  ##               The process must output metrics by itself.
  ##   "STDIN"   : Send a newline on STDIN. (Recommended for gather inputs)
  ##   "SIGHUP"  : Send a HUP signal. Not available on Windows. (not recommended)
  ##   "SIGUSR1" : Send a USR1 signal. Not available on Windows.
  ##   "SIGUSR2" : Send a USR2 signal. Not available on Windows.
  signal = "none"

  ## Delay before the process is restarted after an unexpected termination
  restart_delay = "10s"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
```

### Example

##### Daemon written in bash using STDIN signaling

```bash
#!/bin/bash

counter=0

while IFS= read -r LINE; do
    echo "counter_bash count=${counter}"
    let counter=counter+1
done
```

```toml
[[inputs.execd]]
  command = ["plugins/inputs/execd/examples/count.sh"]
  signal = "STDIN"
```

##### Go daemon using SIGHUP

```go
package main

import (
    "fmt"
    "os"
    "os/signal"
    "syscall"
)

func main() {
    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGHUP)

    counter := 0

    for {
        <-c

        fmt.Printf("counter_go count=%d\n", counter)
        counter++
    }
}

```

```toml
[[inputs.execd]]
  command = ["plugins/inputs/execd/examples/count.go.exe"]
  signal = "SIGHUP"
```

##### Ruby daemon running standalone

```ruby
#!/usr/bin/env ruby

counter = 0

loop do
  puts "counter_ruby count=#{counter}"
  STDOUT.flush

  counter += 1
  sleep 1
end
```

```toml
[[inputs.execd]]
  command = ["plugins/inputs/execd/examples/count.rb"]
  signal = "none"
```

[Input Data Formats]: https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
[inputs.exec]: https://github.com/influxdata/telegraf/blob/master/plugins/inputs/exec/README.md
