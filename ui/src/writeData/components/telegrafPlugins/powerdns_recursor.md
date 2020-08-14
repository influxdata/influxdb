# PowerDNS Recursor Input Plugin

The `powerdns_recursor` plugin gathers metrics about PowerDNS Recursor using
the unix controlsocket.

### Configuration

```toml
[[inputs.powerdns_recursor]]
  ## Path to the Recursor control socket.
  unix_sockets = ["/var/run/pdns_recursor.controlsocket"]

  ## Directory to create receive socket.  This default is likely not writable,
  ## please reference the full plugin documentation for a recommended setup.
  # socket_dir = "/var/run/"
  ## Socket permissions for the receive socket.
  # socket_mode = "0666"
```

#### Permissions

Telegraf will need read/write access to the control socket and to the
`socket_dir`.  PowerDNS will need to be able to write to the `socket_dir`.

The setup described below was tested on a Debian Stretch system and may need
adapted for other systems.

First change permissions on the controlsocket in the PowerDNS recursor
configuration, usually in `/etc/powerdns/recursor.conf`:
```
socket-mode = 660
```

Then place the `telegraf` user into the `pdns` group:
```
usermod telegraf -a -G pdns
```

Since `telegraf` cannot write to to the default `/var/run` socket directory,
create a subdirectory and adjust permissions for this directory so that both
users can access it.
```sh
$ mkdir /var/run/pdns
$ chown root:pdns /var/run/pdns
$ chmod 770 /var/run/pdns
```

### Metrics

- powerdns_recursor
  - tags:
    - server
  - fields:
    - all-outqueries
    - answers-slow
    - answers0-1
    - answers1-10
    - answers10-100
    - answers100-1000
    - auth-zone-queries
    - auth4-answers-slow
    - auth4-answers0-1
    - auth4-answers1-10
    - auth4-answers10-100
    - auth4-answers100-1000
    - auth6-answers-slow
    - auth6-answers0-1
    - auth6-answers1-10
    - auth6-answers10-100
    - auth6-answers100-1000
    - cache-entries
    - cache-hits
    - cache-misses
    - case-mismatches
    - chain-resends
    - client-parse-errors
    - concurrent-queries
    - dlg-only-drops
    - dnssec-queries
    - dnssec-result-bogus
    - dnssec-result-indeterminate
    - dnssec-result-insecure
    - dnssec-result-nta
    - dnssec-result-secure
    - dnssec-validations
    - dont-outqueries
    - ecs-queries
    - ecs-responses
    - edns-ping-matches
    - edns-ping-mismatches
    - failed-host-entries
    - fd-usage
    - ignored-packets
    - ipv6-outqueries
    - ipv6-questions
    - malloc-bytes
    - max-cache-entries
    - max-mthread-stack
    - max-packetcache-entries
    - negcache-entries
    - no-packet-error
    - noedns-outqueries
    - noerror-answers
    - noping-outqueries
    - nsset-invalidations
    - nsspeeds-entries
    - nxdomain-answers
    - outgoing-timeouts
    - outgoing4-timeouts
    - outgoing6-timeouts
    - over-capacity-drops
    - packetcache-entries
    - packetcache-hits
    - packetcache-misses
    - policy-drops
    - policy-result-custom
    - policy-result-drop
    - policy-result-noaction
    - policy-result-nodata
    - policy-result-nxdomain
    - policy-result-truncate
    - qa-latency
    - query-pipe-full-drops
    - questions
    - real-memory-usage
    - resource-limits
    - security-status
    - server-parse-errors
    - servfail-answers
    - spoof-prevents
    - sys-msec
    - tcp-client-overflow
    - tcp-clients
    - tcp-outqueries
    - tcp-questions
    - throttle-entries
    - throttled-out
    - throttled-outqueries
    - too-old-drops
    - udp-in-errors
    - udp-noport-errors
    - udp-recvbuf-errors
    - udp-sndbuf-errors
    - unauthorized-tcp
    - unauthorized-udp
    - unexpected-packets
    - unreachables
    - uptime
    - user-msec
    - x-our-latency
    - x-ourtime-slow
    - x-ourtime0-1
    - x-ourtime1-2
    - x-ourtime16-32
    - x-ourtime2-4
    - x-ourtime4-8
    - x-ourtime8-16

### Example Output

```
powerdns_recursor,server=/var/run/pdns_recursor.controlsocket all-outqueries=3631810i,answers-slow=36863i,answers0-1=179612i,answers1-10=1223305i,answers10-100=1252199i,answers100-1000=408357i,auth-zone-queries=4i,auth4-answers-slow=44758i,auth4-answers0-1=59721i,auth4-answers1-10=1766787i,auth4-answers10-100=1329638i,auth4-answers100-1000=430372i,auth6-answers-slow=0i,auth6-answers0-1=0i,auth6-answers1-10=0i,auth6-answers10-100=0i,auth6-answers100-1000=0i,cache-entries=296689i,cache-hits=150654i,cache-misses=2949682i,case-mismatches=0i,chain-resends=420004i,client-parse-errors=0i,concurrent-queries=0i,dlg-only-drops=0i,dnssec-queries=152970i,dnssec-result-bogus=0i,dnssec-result-indeterminate=0i,dnssec-result-insecure=0i,dnssec-result-nta=0i,dnssec-result-secure=47i,dnssec-validations=47i,dont-outqueries=62i,ecs-queries=0i,ecs-responses=0i,edns-ping-matches=0i,edns-ping-mismatches=0i,failed-host-entries=21i,fd-usage=32i,ignored-packets=0i,ipv6-outqueries=0i,ipv6-questions=0i,malloc-bytes=0i,max-cache-entries=1000000i,max-mthread-stack=33747i,max-packetcache-entries=500000i,negcache-entries=100019i,no-packet-error=0i,noedns-outqueries=73341i,noerror-answers=25453808i,noping-outqueries=0i,nsset-invalidations=2398i,nsspeeds-entries=3966i,nxdomain-answers=3341302i,outgoing-timeouts=44384i,outgoing4-timeouts=44384i,outgoing6-timeouts=0i,over-capacity-drops=0i,packetcache-entries=78258i,packetcache-hits=25999027i,packetcache-misses=3100179i,policy-drops=0i,policy-result-custom=0i,policy-result-drop=0i,policy-result-noaction=3100336i,policy-result-nodata=0i,policy-result-nxdomain=0i,policy-result-truncate=0i,qa-latency=6553i,query-pipe-full-drops=0i,questions=29099363i,real-memory-usage=280494080i,resource-limits=0i,security-status=1i,server-parse-errors=0i,servfail-answers=304253i,spoof-prevents=0i,sys-msec=1312600i,tcp-client-overflow=0i,tcp-clients=0i,tcp-outqueries=116i,tcp-questions=133i,throttle-entries=21i,throttled-out=13296i,throttled-outqueries=13296i,too-old-drops=2i,udp-in-errors=4i,udp-noport-errors=2918i,udp-recvbuf-errors=0i,udp-sndbuf-errors=0i,unauthorized-tcp=0i,unauthorized-udp=0i,unexpected-packets=0i,unreachables=1708i,uptime=167482i,user-msec=1282640i,x-our-latency=19i,x-ourtime-slow=642i,x-ourtime0-1=3095566i,x-ourtime1-2=3401i,x-ourtime16-32=201i,x-ourtime2-4=304i,x-ourtime4-8=198i,x-ourtime8-16=24i 1533903879000000000
```
