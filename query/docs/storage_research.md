The following benchmarks measure reading 10,000,000 points via TCP, yamux and gRPC. 
These were performed using a VM and host machine on the same physical hardware. 
`iperf` reported about 390 MB/s as the maximum throughput between the machines. 
`influxd` running on VM, client on host.

All protocols used the same protobuf message (to provide framing) with an embedded `[]byte` 
array to serialize batches of points – large arrays of structures for points are simply too slow.

The underlying storage engine cursor can read 10,000,000 points in about 230ms, therefore 
the overhead for each protocol is as follows

```
TCP   → 470ms
yamux → 620ms
gRPC  → 970ms
```

Maximum transfer rates are therefore:

```
TCP   → 340 MB/s or ~21e6 points / sec
yamux → 258 MB/s or ~16e6 points / sec
gRPC  → 164 MB/s or ~10e6 points / sec
```

It is worth noting that I have not tested Go's network libraries to determine maximum throughput, 
however I suspect it may be close to the TCP maximum. Whilst we will benchmark using independent
machines in AWS, these tests helped me understand relative performance of the various transports 
and the impact different serialization mechanisms have on our throughput. Protobuf is ok as long 
as we keep the graph small, meaning we customize the serialization of the points.

---

As a comparison, I also tested client and server on localhost, to compare the protocols without the 
network stack overhead. gRPC was very inconsistent, varying anywhere from 463ms to 793ms so the result 
represents the average of a number of runs.

Overhead

```
TCP   →  95ms
yamux → 108ms
gRPC  → 441ms
```

These numbers bring TCP and yamux within about 10% of each other. The majority of the difference 
between TCP and yamux is due to the additional frames sent by yamux to manage flow control, 
which add latency. If that overhead is a concern, we may need to tune the flow control algorithm.
