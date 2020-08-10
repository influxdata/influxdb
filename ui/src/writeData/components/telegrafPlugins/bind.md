# BIND 9 Nameserver Statistics Input Plugin

This plugin decodes the JSON or XML statistics provided by BIND 9 nameservers.

### XML Statistics Channel

Version 2 statistics (BIND 9.6 - 9.9) and version 3 statistics (BIND 9.9+) are supported. Note that
for BIND 9.9 to support version 3 statistics, it must be built with the `--enable-newstats` compile
flag, and it must be specifically requested via the correct URL. Version 3 statistics are the
default (and only) XML format in BIND 9.10+.

### JSON Statistics Channel

JSON statistics schema version 1 (BIND 9.10+) is supported. As of writing, some distros still do
not enable support for JSON statistics in their BIND packages.

### Configuration:

- **urls** []string: List of BIND statistics channel URLs to collect from. Do not include a
  trailing slash in the URL. Default is "http://localhost:8053/xml/v3".
- **gather_memory_contexts** bool: Report per-context memory statistics.
- **gather_views** bool: Report per-view query statistics.

The following table summarizes the URL formats which should be used, depending on your BIND
version and configured statistics channel.

| BIND Version | Statistics Format | Example URL                   |
| ------------ | ----------------- | ----------------------------- |
| 9.6 - 9.8    | XML v2            | http://localhost:8053         |
| 9.9          | XML v2            | http://localhost:8053/xml/v2  |
| 9.9+         | XML v3            | http://localhost:8053/xml/v3  |
| 9.10+        | JSON v1           | http://localhost:8053/json/v1 |

#### Configuration of BIND Daemon

Add the following to your named.conf if running Telegraf on the same host as the BIND daemon:
```
statistics-channels {
    inet 127.0.0.1 port 8053;
};
```

Alternatively, specify a wildcard address (e.g., 0.0.0.0) or specific IP address of an interface to
configure the BIND daemon to listen on that address. Note that you should secure the statistics
channel with an ACL if it is publicly reachable. Consult the BIND Administrator Reference Manual
for more information.

### Measurements & Fields:

- bind_counter
  - name=value (multiple)
- bind_memory
  - total_use
  - in_use
  - block_size
  - context_size
  - lost
- bind_memory_context
  - total
  - in_use

### Tags:

- All measurements
  - url
  - source
  - port
- bind_counter
  - type
  - view (optional)
- bind_memory_context
  - id
  - name

### Sample Queries:

These are some useful queries (to generate dashboards or other) to run against data from this
plugin:

```
SELECT non_negative_derivative(mean(/^A$|^PTR$/), 5m) FROM bind_counter \
WHERE "url" = 'localhost:8053' AND "type" = 'qtype' AND time > now() - 1h \
GROUP BY time(5m), "type"
```

```
name: bind_counter
tags: type=qtype
time                non_negative_derivative_A non_negative_derivative_PTR
----                ------------------------- ---------------------------
1553862000000000000 254.99444444430992        1388.311111111194
1553862300000000000 354                       2135.716666666791
1553862600000000000 316.8666666666977         2130.133333333768
1553862900000000000 309.05000000004657        2126.75
1553863200000000000 315.64999999990687        2128.483333332464
1553863500000000000 308.9166666667443         2132.350000000559
1553863800000000000 302.64999999990687        2131.1833333335817
1553864100000000000 310.85000000009313        2132.449999999255
1553864400000000000 314.3666666666977         2136.216666666791
1553864700000000000 303.2333333331626         2133.8166666673496
1553865000000000000 304.93333333334886        2127.333333333023
1553865300000000000 317.93333333334886        2130.3166666664183
1553865600000000000 280.6666666667443         1807.9071428570896
```

### Example Output

Here is example output of this plugin:

```
bind_memory,host=LAP,port=8053,source=localhost,url=localhost:8053 block_size=12058624i,context_size=4575056i,in_use=4113717i,lost=0i,total_use=16663252i 1554276619000000000
bind_counter,host=LAP,port=8053,source=localhost,type=opcode,url=localhost:8053 IQUERY=0i,NOTIFY=0i,QUERY=9i,STATUS=0i,UPDATE=0i 1554276619000000000
bind_counter,host=LAP,port=8053,source=localhost,type=rcode,url=localhost:8053 17=0i,18=0i,19=0i,20=0i,21=0i,22=0i,BADCOOKIE=0i,BADVERS=0i,FORMERR=0i,NOERROR=7i,NOTAUTH=0i,NOTIMP=0i,NOTZONE=0i,NXDOMAIN=0i,NXRRSET=0i,REFUSED=0i,RESERVED11=0i,RESERVED12=0i,RESERVED13=0i,RESERVED14=0i,RESERVED15=0i,SERVFAIL=2i,YXDOMAIN=0i,YXRRSET=0i 1554276619000000000
bind_counter,host=LAP,port=8053,source=localhost,type=qtype,url=localhost:8053 A=1i,ANY=1i,NS=1i,PTR=5i,SOA=1i 1554276619000000000
bind_counter,host=LAP,port=8053,source=localhost,type=nsstat,url=localhost:8053 AuthQryRej=0i,CookieBadSize=0i,CookieBadTime=0i,CookieIn=9i,CookieMatch=0i,CookieNew=9i,CookieNoMatch=0i,DNS64=0i,ECSOpt=0i,ExpireOpt=0i,KeyTagOpt=0i,NSIDOpt=0i,OtherOpt=0i,QryAuthAns=7i,QryBADCOOKIE=0i,QryDropped=0i,QryDuplicate=0i,QryFORMERR=0i,QryFailure=0i,QryNXDOMAIN=0i,QryNXRedir=0i,QryNXRedirRLookup=0i,QryNoauthAns=0i,QryNxrrset=1i,QryRecursion=2i,QryReferral=0i,QrySERVFAIL=2i,QrySuccess=6i,QryTCP=1i,QryUDP=8i,RPZRewrites=0i,RateDropped=0i,RateSlipped=0i,RecQryRej=0i,RecursClients=0i,ReqBadEDNSVer=0i,ReqBadSIG=0i,ReqEdns0=9i,ReqSIG0=0i,ReqTCP=1i,ReqTSIG=0i,Requestv4=9i,Requestv6=0i,RespEDNS0=9i,RespSIG0=0i,RespTSIG=0i,Response=9i,TruncatedResp=0i,UpdateBadPrereq=0i,UpdateDone=0i,UpdateFail=0i,UpdateFwdFail=0i,UpdateRej=0i,UpdateReqFwd=0i,UpdateRespFwd=0i,XfrRej=0i,XfrReqDone=0i 1554276619000000000
bind_counter,host=LAP,port=8053,source=localhost,type=zonestat,url=localhost:8053 AXFRReqv4=0i,AXFRReqv6=0i,IXFRReqv4=0i,IXFRReqv6=0i,NotifyInv4=0i,NotifyInv6=0i,NotifyOutv4=0i,NotifyOutv6=0i,NotifyRej=0i,SOAOutv4=0i,SOAOutv6=0i,XfrFail=0i,XfrSuccess=0i 1554276619000000000
bind_counter,host=LAP,port=8053,source=localhost,type=sockstat,url=localhost:8053 FDWatchClose=0i,FDwatchConn=0i,FDwatchConnFail=0i,FDwatchRecvErr=0i,FDwatchSendErr=0i,FdwatchBindFail=0i,RawActive=1i,RawClose=0i,RawOpen=1i,RawOpenFail=0i,RawRecvErr=0i,TCP4Accept=6i,TCP4AcceptFail=0i,TCP4Active=9i,TCP4BindFail=0i,TCP4Close=5i,TCP4Conn=0i,TCP4ConnFail=0i,TCP4Open=8i,TCP4OpenFail=0i,TCP4RecvErr=0i,TCP4SendErr=0i,TCP6Accept=0i,TCP6AcceptFail=0i,TCP6Active=2i,TCP6BindFail=0i,TCP6Close=0i,TCP6Conn=0i,TCP6ConnFail=0i,TCP6Open=2i,TCP6OpenFail=0i,TCP6RecvErr=0i,TCP6SendErr=0i,UDP4Active=18i,UDP4BindFail=14i,UDP4Close=14i,UDP4Conn=0i,UDP4ConnFail=0i,UDP4Open=32i,UDP4OpenFail=0i,UDP4RecvErr=0i,UDP4SendErr=0i,UDP6Active=3i,UDP6BindFail=0i,UDP6Close=6i,UDP6Conn=0i,UDP6ConnFail=6i,UDP6Open=9i,UDP6OpenFail=0i,UDP6RecvErr=0i,UDP6SendErr=0i,UnixAccept=0i,UnixAcceptFail=0i,UnixActive=0i,UnixBindFail=0i,UnixClose=0i,UnixConn=0i,UnixConnFail=0i,UnixOpen=0i,UnixOpenFail=0i,UnixRecvErr=0i,UnixSendErr=0i 1554276619000000000
```
