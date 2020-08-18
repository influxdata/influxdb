# ntpq Input Plugin

Get standard NTP query metrics, requires ntpq executable.

Below is the documentation of the various headers returned from the NTP query
command when running `ntpq -p`.

- remote – The remote peer or server being synced to. “LOCAL” is this local host
(included in case there are no remote peers or servers available);
- refid – Where or what the remote peer or server is itself synchronised to;
- st (stratum) – The remote peer or server Stratum
- t (type) – Type (u: unicast or manycast client, b: broadcast or multicast client,
l: local reference clock, s: symmetric peer, A: manycast server,
B: broadcast server, M: multicast server, see “Automatic Server Discovery“);
- when – When last polled (seconds ago, “h” hours ago, or “d” days ago);
- poll – Polling frequency: rfc5905 suggests this ranges in NTPv4 from 4 (16s)
to 17 (36h) (log2 seconds), however observation suggests the actual displayed
value is seconds for a much smaller range of 64 (26) to 1024 (210) seconds;
- reach – An 8-bit left-shift shift register value recording polls (bit set =
successful, bit reset = fail) displayed in octal;
- delay – Round trip communication delay to the remote peer or server (milliseconds);
- offset – Mean offset (phase) in the times reported between this local host and
the remote peer or server (RMS, milliseconds);
- jitter – Mean deviation (jitter) in the time reported for that remote peer or
server (RMS of difference of multiple time samples, milliseconds);

### Configuration:

```toml
# Get standard NTP query metrics, requires ntpq executable
[[inputs.ntpq]]
  ## If false, add -n for ntpq command. Can reduce metric gather times.
  dns_lookup = true
```

### Measurements & Fields:

- ntpq
    - delay (float, milliseconds)
    - jitter (float, milliseconds)
    - offset (float, milliseconds)
    - poll (int, seconds)
    - reach (int)
    - when (int, seconds)

### Tags:

- All measurements have the following tags:
    - refid
    - remote
    - type
    - stratum

### Example Output:

```
$ telegraf --config ~/ws/telegraf.conf --input-filter ntpq --test
* Plugin: ntpq, Collection 1
> ntpq,refid=.GPSs.,remote=*time.apple.com,stratum=1,type=u delay=91.797,jitter=3.735,offset=12.841,poll=64i,reach=377i,when=35i 1457960478909556134
```
