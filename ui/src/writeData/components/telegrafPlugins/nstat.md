# Nstat Input Plugin

Plugin collects network metrics from `/proc/net/netstat`, `/proc/net/snmp` and `/proc/net/snmp6` files

### Configuration

The plugin firstly tries to read file paths from config values
if it is empty, then it reads from env variables.
* `PROC_NET_NETSTAT`
* `PROC_NET_SNMP`
* `PROC_NET_SNMP6`

If these variables are also not set,
then it tries to read the proc root from env - `PROC_ROOT`,
and sets `/proc` as a root path if `PROC_ROOT` is also empty.

Then appends default file paths:
* `/net/netstat`
* `/net/snmp`
* `/net/snmp6`

So if nothing is given, no paths in config and in env vars, the plugin takes the default paths.
* `/proc/net/netstat`
* `/proc/net/snmp`
* `/proc/net/snmp6`

The sample config file
```toml
[[inputs.nstat]]
  ## file paths
  ## e.g: /proc/net/netstat, /proc/net/snmp, /proc/net/snmp6
  # proc_net_netstat    = 	""
  # proc_net_snmp 		= 	""
  # proc_net_snmp6 		= 	""
  ## dump metrics with 0 values too
  # dump_zeros			= 	true
```

In case that `proc_net_snmp6` path doesn't exist (e.g. IPv6 is not enabled) no error would be raised.

### Measurements & Fields

- nstat
    - Icmp6InCsumErrors
    - Icmp6InDestUnreachs
    - Icmp6InEchoReplies
    - Icmp6InEchos
    - Icmp6InErrors
    - Icmp6InGroupMembQueries
    - Icmp6InGroupMembReductions
    - Icmp6InGroupMembResponses
    - Icmp6InMLDv2Reports
    - Icmp6InMsgs
    - Icmp6InNeighborAdvertisements
    - Icmp6InNeighborSolicits
    - Icmp6InParmProblems
    - Icmp6InPktTooBigs
    - Icmp6InRedirects
    - Icmp6InRouterAdvertisements
    - Icmp6InRouterSolicits
    - Icmp6InTimeExcds
    - Icmp6OutDestUnreachs
    - Icmp6OutEchoReplies
    - Icmp6OutEchos
    - Icmp6OutErrors
    - Icmp6OutGroupMembQueries
    - Icmp6OutGroupMembReductions
    - Icmp6OutGroupMembResponses
    - Icmp6OutMLDv2Reports
    - Icmp6OutMsgs
    - Icmp6OutNeighborAdvertisements
    - Icmp6OutNeighborSolicits
    - Icmp6OutParmProblems
    - Icmp6OutPktTooBigs
    - Icmp6OutRedirects
    - Icmp6OutRouterAdvertisements
    - Icmp6OutRouterSolicits
    - Icmp6OutTimeExcds
    - Icmp6OutType133
    - Icmp6OutType135
    - Icmp6OutType143
    - IcmpInAddrMaskReps
    - IcmpInAddrMasks
    - IcmpInCsumErrors
    - IcmpInDestUnreachs
    - IcmpInEchoReps
    - IcmpInEchos
    - IcmpInErrors
    - IcmpInMsgs
    - IcmpInParmProbs
    - IcmpInRedirects
    - IcmpInSrcQuenchs
    - IcmpInTimeExcds
    - IcmpInTimestampReps
    - IcmpInTimestamps
    - IcmpMsgInType3
    - IcmpMsgOutType3
    - IcmpOutAddrMaskReps
    - IcmpOutAddrMasks
    - IcmpOutDestUnreachs
    - IcmpOutEchoReps
    - IcmpOutEchos
    - IcmpOutErrors
    - IcmpOutMsgs
    - IcmpOutParmProbs
    - IcmpOutRedirects
    - IcmpOutSrcQuenchs
    - IcmpOutTimeExcds
    - IcmpOutTimestampReps
    - IcmpOutTimestamps
    - Ip6FragCreates
    - Ip6FragFails
    - Ip6FragOKs
    - Ip6InAddrErrors
    - Ip6InBcastOctets
    - Ip6InCEPkts
    - Ip6InDelivers
    - Ip6InDiscards
    - Ip6InECT0Pkts
    - Ip6InECT1Pkts
    - Ip6InHdrErrors
    - Ip6InMcastOctets
    - Ip6InMcastPkts
    - Ip6InNoECTPkts
    - Ip6InNoRoutes
    - Ip6InOctets
    - Ip6InReceives
    - Ip6InTooBigErrors
    - Ip6InTruncatedPkts
    - Ip6InUnknownProtos
    - Ip6OutBcastOctets
    - Ip6OutDiscards
    - Ip6OutForwDatagrams
    - Ip6OutMcastOctets
    - Ip6OutMcastPkts
    - Ip6OutNoRoutes
    - Ip6OutOctets
    - Ip6OutRequests
    - Ip6ReasmFails
    - Ip6ReasmOKs
    - Ip6ReasmReqds
    - Ip6ReasmTimeout
    - IpDefaultTTL
    - IpExtInBcastOctets
    - IpExtInBcastPkts
    - IpExtInCEPkts
    - IpExtInCsumErrors
    - IpExtInECT0Pkts
    - IpExtInECT1Pkts
    - IpExtInMcastOctets
    - IpExtInMcastPkts
    - IpExtInNoECTPkts
    - IpExtInNoRoutes
    - IpExtInOctets
    - IpExtInTruncatedPkts
    - IpExtOutBcastOctets
    - IpExtOutBcastPkts
    - IpExtOutMcastOctets
    - IpExtOutMcastPkts
    - IpExtOutOctets
    - IpForwDatagrams
    - IpForwarding
    - IpFragCreates
    - IpFragFails
    - IpFragOKs
    - IpInAddrErrors
    - IpInDelivers
    - IpInDiscards
    - IpInHdrErrors
    - IpInReceives
    - IpInUnknownProtos
    - IpOutDiscards
    - IpOutNoRoutes
    - IpOutRequests
    - IpReasmFails
    - IpReasmOKs
    - IpReasmReqds
    - IpReasmTimeout
    - TcpActiveOpens
    - TcpAttemptFails
    - TcpCurrEstab
    - TcpEstabResets
    - TcpExtArpFilter
    - TcpExtBusyPollRxPackets
    - TcpExtDelayedACKLocked
    - TcpExtDelayedACKLost
    - TcpExtDelayedACKs
    - TcpExtEmbryonicRsts
    - TcpExtIPReversePathFilter
    - TcpExtListenDrops
    - TcpExtListenOverflows
    - TcpExtLockDroppedIcmps
    - TcpExtOfoPruned
    - TcpExtOutOfWindowIcmps
    - TcpExtPAWSActive
    - TcpExtPAWSEstab
    - TcpExtPAWSPassive
    - TcpExtPruneCalled
    - TcpExtRcvPruned
    - TcpExtSyncookiesFailed
    - TcpExtSyncookiesRecv
    - TcpExtSyncookiesSent
    - TcpExtTCPACKSkippedChallenge
    - TcpExtTCPACKSkippedFinWait2
    - TcpExtTCPACKSkippedPAWS
    - TcpExtTCPACKSkippedSeq
    - TcpExtTCPACKSkippedSynRecv
    - TcpExtTCPACKSkippedTimeWait
    - TcpExtTCPAbortFailed
    - TcpExtTCPAbortOnClose
    - TcpExtTCPAbortOnData
    - TcpExtTCPAbortOnLinger
    - TcpExtTCPAbortOnMemory
    - TcpExtTCPAbortOnTimeout
    - TcpExtTCPAutoCorking
    - TcpExtTCPBacklogDrop
    - TcpExtTCPChallengeACK
    - TcpExtTCPDSACKIgnoredNoUndo
    - TcpExtTCPDSACKIgnoredOld
    - TcpExtTCPDSACKOfoRecv
    - TcpExtTCPDSACKOfoSent
    - TcpExtTCPDSACKOldSent
    - TcpExtTCPDSACKRecv
    - TcpExtTCPDSACKUndo
    - TcpExtTCPDeferAcceptDrop
    - TcpExtTCPDirectCopyFromBacklog
    - TcpExtTCPDirectCopyFromPrequeue
    - TcpExtTCPFACKReorder
    - TcpExtTCPFastOpenActive
    - TcpExtTCPFastOpenActiveFail
    - TcpExtTCPFastOpenCookieReqd
    - TcpExtTCPFastOpenListenOverflow
    - TcpExtTCPFastOpenPassive
    - TcpExtTCPFastOpenPassiveFail
    - TcpExtTCPFastRetrans
    - TcpExtTCPForwardRetrans
    - TcpExtTCPFromZeroWindowAdv
    - TcpExtTCPFullUndo
    - TcpExtTCPHPAcks
    - TcpExtTCPHPHits
    - TcpExtTCPHPHitsToUser
    - TcpExtTCPHystartDelayCwnd
    - TcpExtTCPHystartDelayDetect
    - TcpExtTCPHystartTrainCwnd
    - TcpExtTCPHystartTrainDetect
    - TcpExtTCPKeepAlive
    - TcpExtTCPLossFailures
    - TcpExtTCPLossProbeRecovery
    - TcpExtTCPLossProbes
    - TcpExtTCPLossUndo
    - TcpExtTCPLostRetransmit
    - TcpExtTCPMD5NotFound
    - TcpExtTCPMD5Unexpected
    - TcpExtTCPMTUPFail
    - TcpExtTCPMTUPSuccess
    - TcpExtTCPMemoryPressures
    - TcpExtTCPMinTTLDrop
    - TcpExtTCPOFODrop
    - TcpExtTCPOFOMerge
    - TcpExtTCPOFOQueue
    - TcpExtTCPOrigDataSent
    - TcpExtTCPPartialUndo
    - TcpExtTCPPrequeueDropped
    - TcpExtTCPPrequeued
    - TcpExtTCPPureAcks
    - TcpExtTCPRcvCoalesce
    - TcpExtTCPRcvCollapsed
    - TcpExtTCPRenoFailures
    - TcpExtTCPRenoRecovery
    - TcpExtTCPRenoRecoveryFail
    - TcpExtTCPRenoReorder
    - TcpExtTCPReqQFullDoCookies
    - TcpExtTCPReqQFullDrop
    - TcpExtTCPRetransFail
    - TcpExtTCPSACKDiscard
    - TcpExtTCPSACKReneging
    - TcpExtTCPSACKReorder
    - TcpExtTCPSYNChallenge
    - TcpExtTCPSackFailures
    - TcpExtTCPSackMerged
    - TcpExtTCPSackRecovery
    - TcpExtTCPSackRecoveryFail
    - TcpExtTCPSackShiftFallback
    - TcpExtTCPSackShifted
    - TcpExtTCPSchedulerFailed
    - TcpExtTCPSlowStartRetrans
    - TcpExtTCPSpuriousRTOs
    - TcpExtTCPSpuriousRtxHostQueues
    - TcpExtTCPSynRetrans
    - TcpExtTCPTSReorder
    - TcpExtTCPTimeWaitOverflow
    - TcpExtTCPTimeouts
    - TcpExtTCPToZeroWindowAdv
    - TcpExtTCPWantZeroWindowAdv
    - TcpExtTCPWinProbe
    - TcpExtTW
    - TcpExtTWKilled
    - TcpExtTWRecycled
    - TcpInCsumErrors
    - TcpInErrs
    - TcpInSegs
    - TcpMaxConn
    - TcpOutRsts
    - TcpOutSegs
    - TcpPassiveOpens
    - TcpRetransSegs
    - TcpRtoAlgorithm
    - TcpRtoMax
    - TcpRtoMin
    - Udp6IgnoredMulti
    - Udp6InCsumErrors
    - Udp6InDatagrams
    - Udp6InErrors
    - Udp6NoPorts
    - Udp6OutDatagrams
    - Udp6RcvbufErrors
    - Udp6SndbufErrors
    - UdpIgnoredMulti
    - UdpInCsumErrors
    - UdpInDatagrams
    - UdpInErrors
    - UdpLite6InCsumErrors
    - UdpLite6InDatagrams
    - UdpLite6InErrors
    - UdpLite6NoPorts
    - UdpLite6OutDatagrams
    - UdpLite6RcvbufErrors
    - UdpLite6SndbufErrors
    - UdpLiteIgnoredMulti
    - UdpLiteInCsumErrors
    - UdpLiteInDatagrams
    - UdpLiteInErrors
    - UdpLiteNoPorts
    - UdpLiteOutDatagrams
    - UdpLiteRcvbufErrors
    - UdpLiteSndbufErrors
    - UdpNoPorts
    - UdpOutDatagrams
    - UdpRcvbufErrors
    - UdpSndbufErrors

### Tags
- All measurements have the following tags
    - host (host of the system)
    - name (the type of the metric: snmp, snmp6 or netstat)
