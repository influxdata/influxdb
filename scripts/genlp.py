#!/usr/bin/env python3
#
# Generate some simple line protocol. Usage:
#
# ```
# ./scripts/genlp.py | head -n 2000
# ```
#
# Please use https://github.com/influxdata/iox_data_generator for anything
# more complicated.
#

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

c=0
while True:
  for i in range(1, 1000):
    for j in range(1, 1000):
      c = c + 1
      print(f"cpu,hostname=host-{i},nodename=node-{j} cpu_seconds={c}")
