Siege
=====

Siege is an HTTP benchmarking tool and can be used against InfluxDB easily.
If you're on Mac you can install `siege` using `brew`. If you're on Linux
you can install using your package manager.


## Running

To run siege, first start one or more InfluxDB nodes. At least one of those
nodes should run on the default port of `8086`.

Next, choose a URL file to run. The URL files are named based on their series
cardinality. For example, `series.10.txt` has 10 unique series.

Now you can execute siege. There are several arguments available but only 
a few that we're concerned with:

```
-c NUM   the number of concurrent connections.
-d NUM   delay between each request, in seconds.
-b       benchmark mode. runs with a delay of 0.
-t DUR   duration of the benchmark. value should end in 's', 'm', or 'h'.
-f FILE  the path to the URL file.
```

These can be combined to simulate different load. For example, this command
will execute writes against 10 unique series using 100 concurrent connections:

```sh
$ siege -c 100 -f series.10.txt
```
