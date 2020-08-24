# chrony Input Plugin

Get standard chrony metrics, requires chronyc executable.

Below is the documentation of the various headers returned by `chronyc tracking`.

- Reference ID - This is the refid and name (or IP address) if available, of the
server to which the computer is currently synchronised. If this is 127.127.1.1
it means the computer is not synchronised to any external source and that you
have the ‘local’ mode operating (via the local command in chronyc (see section local),
or the local directive in the ‘/etc/chrony.conf’ file (see section local)).
- Stratum - The stratum indicates how many hops away from a computer with an attached
reference clock we are. Such a computer is a stratum-1 computer, so the computer in the
example is two hops away (i.e. a.b.c is a stratum-2 and is synchronised from a stratum-1).
- Ref time - This is the time (UTC) at which the last measurement from the reference
source was processed.
- System time - In normal operation, chronyd never steps the system clock, because any
jump in the timescale can have adverse consequences for certain application programs.
Instead, any error in the system clock is corrected by slightly speeding up or slowing
down the system clock until the error has been removed, and then returning to the system
clock’s normal speed. A consequence of this is that there will be a period when the
system clock (as read by other programs using the gettimeofday() system call, or by the
date command in the shell) will be different from chronyd's estimate of the current true
time (which it reports to NTP clients when it is operating in server mode). The value
reported on this line is the difference due to this effect.
- Last offset - This is the estimated local offset on the last clock update.
- RMS offset - This is a long-term average of the offset value.
- Frequency - The ‘frequency’ is the rate by which the system’s clock would be
wrong if chronyd was not correcting it. It is expressed in ppm (parts per million).
For example, a value of 1ppm would mean that when the system’s clock thinks it has
advanced 1 second, it has actually advanced by 1.000001 seconds relative to true time.
- Residual freq - This shows the ‘residual frequency’ for the currently selected
reference source. This reflects any difference between what the measurements from the
reference source indicate the frequency should be and the frequency currently being used.
The reason this is not always zero is that a smoothing procedure is applied to the
frequency. Each time a measurement from the reference source is obtained and a new
residual frequency computed, the estimated accuracy of this residual is compared with the
estimated accuracy (see ‘skew’ next) of the existing frequency value. A weighted average
is computed for the new frequency, with weights depending on these accuracies. If the
measurements from the reference source follow a consistent trend, the residual will be
driven to zero over time.
- Skew - This is the estimated error bound on the frequency.
- Root delay - This is the total of the network path delays to the stratum-1 computer
from which the computer is ultimately synchronised. In certain extreme situations, this
value can be negative. (This can arise in a symmetric peer arrangement where the computers’
frequencies are not tracking each other and the network delay is very short relative to the
turn-around time at each computer.)
- Root dispersion - This is the total dispersion accumulated through all the computers
back to the stratum-1 computer from which the computer is ultimately synchronised.
Dispersion is due to system clock resolution, statistical measurement variations etc.
- Leap status - This is the leap status, which can be Normal, Insert second,
Delete second or Not synchronised.

### Configuration:

```toml
# Get standard chrony metrics, requires chronyc executable.
[[inputs.chrony]]
  ## If true, chronyc tries to perform a DNS lookup for the time server.
  # dns_lookup = false
```

### Measurements & Fields:

- chrony
    - system_time (float, seconds)
    - last_offset (float, seconds)
    - rms_offset (float, seconds)
    - frequency (float, ppm)
    - residual_freq (float, ppm)
    - skew (float, ppm)
    - root_delay (float, seconds)
    - root_dispersion (float, seconds)
    - update_interval (float, seconds)

### Tags:

- All measurements have the following tags:
    - reference_id
    - stratum
    - leap_status

### Example Output:

```
$ telegraf --config telegraf.conf --input-filter chrony --test
* Plugin: chrony, Collection 1
> chrony,leap_status=normal,reference_id=192.168.1.1,stratum=3 frequency=-35.657,system_time=0.000027073,last_offset=-0.000013616,residual_freq=-0,rms_offset=0.000027073,root_delay=0.000644,root_dispersion=0.003444,skew=0.001,update_interval=1031.2 1463750789687639161
```




