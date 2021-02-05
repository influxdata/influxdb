# sysstat Input Plugin

Collect [sysstat](https://github.com/sysstat/sysstat) metrics - requires the sysstat
package installed.

This plugin collects system metrics with the sysstat collector utility `sadc` and parses
the created binary data file with the `sadf` utility.

### Configuration:

```toml
# Sysstat metrics collector
[[inputs.sysstat]]
  ## Path to the sadc command.
  #
  ## On Debian and Arch Linux the default path is /usr/lib/sa/sadc whereas
  ## on RHEL and CentOS the default path is /usr/lib64/sa/sadc
  sadc_path = "/usr/lib/sa/sadc" # required

  ## Path to the sadf command, if it is not in PATH
  # sadf_path = "/usr/bin/sadf"

  ## Activities is a list of activities, that are passed as argument to the
  ## sadc collector utility (e.g: DISK, SNMP etc...)
  ## The more activities that are added, the more data is collected.
  # activities = ["DISK"]

  ## Group metrics to measurements.
  ##
  ## If group is false each metric will be prefixed with a description
  ## and represents itself a measurement.
  ##
  ## If Group is true, corresponding metrics are grouped to a single measurement.
  # group = true

  ## Options for the sadf command. The values on the left represent the sadf options and
  ## the values on the right their description (wich are used for grouping and prefixing metrics).
  ##
  ## Run 'sar -h' or 'man sar' to find out the supported options for your sysstat version.
  [inputs.sysstat.options]
	-C = "cpu"
	-B = "paging"
	-b = "io"
	-d = "disk"             # requires DISK activity
	"-n ALL" = "network"
	"-P ALL" = "per_cpu"
	-q = "queue"
	-R = "mem"
	-r = "mem_util"
	-S = "swap_util"
	-u = "cpu_util"
	-v = "inode"
	-W = "swap"
	-w = "task"
  #	-H = "hugepages"        # only available for newer linux distributions
  #	"-I ALL" = "interrupts" # requires INT activity

  ## Device tags can be used to add additional tags for devices. For example the configuration below
  ## adds a tag vg with value rootvg for all metrics with sda devices.
  # [[inputs.sysstat.device_tags.sda]]
  #  vg = "rootvg"
```

### Measurements & Fields:
#### If group=true
- cpu
    - pct_idle (float)
    - pct_iowait (float)
    - pct_nice (float)
    - pct_steal (float)
    - pct_system (float)
    - pct_user (float)

- disk
    - avgqu-sz (float)
    - avgrq-sz (float)
    - await (float)
    - pct_util (float)
    - rd_sec_pers (float)
    - svctm (float)
    - tps (float)

And much more, depending on the options you configure.

#### If group=false
- cpu_pct_idle
    - value (float)
- cpu_pct_iowait
    - value (float)
- cpu_pct_nice
    - value (float)
- cpu_pct_steal
    - value (float)
- cpu_pct_system
    - value (float)
- cpu_pct_user
    - value (float)
- disk_avgqu-sz
    - value (float)
- disk_avgrq-sz
    - value (float)
- disk_await
    - value (float)
- disk_pct_util
    - value (float)
- disk_rd_sec_per_s
    - value (float)
- disk_svctm
    - value (float)
- disk_tps
    - value (float)

And much more, depending on the options you configure.

### Tags:

- All measurements have the following tags:
    - device

And more if you define some `device_tags`.
### Example Output:

With the configuration below:
```toml
[[inputs.sysstat]]
  sadc_path = "/usr/lib/sa/sadc" # required
  activities = ["DISK", "SNMP", "INT"]
  group = true
  [inputs.sysstat.options]
	-C = "cpu"
	-B = "paging"
	-b = "io"
	-d = "disk"             # requires DISK activity
	-H = "hugepages"
	"-I ALL" = "interrupts" # requires INT activity
	"-n ALL" = "network"
	"-P ALL" = "per_cpu"
	-q = "queue"
	-R = "mem"
	"-r ALL" = "mem_util"
	-S = "swap_util"
	-u = "cpu_util"
	-v = "inode"
	-W = "swap"
	-w = "task"
  [[inputs.sysstat.device_tags.sda]]
    vg = "rootvg"
```

you get the following output:
```
$ telegraf --config telegraf.conf --input-filter sysstat --test
* Plugin: sysstat, Collection 1
> cpu_util,device=all pct_idle=98.85,pct_iowait=0,pct_nice=0.38,pct_steal=0,pct_system=0.64,pct_user=0.13 1459255626657883725
> swap pswpin_per_s=0,pswpout_per_s=0 1459255626658387650
> per_cpu,device=cpu1 pct_idle=98.98,pct_iowait=0,pct_nice=0.26,pct_steal=0,pct_system=0.51,pct_user=0.26 1459255626659630437
> per_cpu,device=all pct_idle=98.85,pct_iowait=0,pct_nice=0.38,pct_steal=0,pct_system=0.64,pct_user=0.13 1459255626659670744
> per_cpu,device=cpu0 pct_idle=98.73,pct_iowait=0,pct_nice=0.76,pct_steal=0,pct_system=0.51,pct_user=0 1459255626659697515
> hugepages kbhugfree=0,kbhugused=0,pct_hugused=0 1459255626660057517
> network,device=lo coll_per_s=0,pct_ifutil=0,rxcmp_per_s=0,rxdrop_per_s=0,rxerr_per_s=0,rxfifo_per_s=0,rxfram_per_s=0,rxkB_per_s=0.81,rxmcst_per_s=0,rxpck_per_s=16,txcarr_per_s=0,txcmp_per_s=0,txdrop_per_s=0,txerr_per_s=0,txfifo_per_s=0,txkB_per_s=0.81,txpck_per_s=16 1459255626661197666
> network access_per_s=0,active_per_s=0,asmf_per_s=0,asmok_per_s=0,asmrq_per_s=0,atmptf_per_s=0,badcall_per_s=0,call_per_s=0,estres_per_s=0,fragcrt_per_s=0,fragf_per_s=0,fragok_per_s=0,fwddgm_per_s=0,getatt_per_s=0,hit_per_s=0,iadrerr_per_s=0,iadrmk_per_s=0,iadrmkr_per_s=0,idel_per_s=16,idgm_per_s=0,idgmerr_per_s=0,idisc_per_s=0,idstunr_per_s=0,iech_per_s=0,iechr_per_s=0,ierr_per_s=0,ihdrerr_per_s=0,imsg_per_s=0,ip-frag=0,iparmpb_per_s=0,irec_per_s=16,iredir_per_s=0,iseg_per_s=16,isegerr_per_s=0,isrcq_per_s=0,itm_per_s=0,itmex_per_s=0,itmr_per_s=0,iukwnpr_per_s=0,miss_per_s=0,noport_per_s=0,oadrmk_per_s=0,oadrmkr_per_s=0,odgm_per_s=0,odisc_per_s=0,odstunr_per_s=0,oech_per_s=0,oechr_per_s=0,oerr_per_s=0,omsg_per_s=0,onort_per_s=0,oparmpb_per_s=0,oredir_per_s=0,orq_per_s=16,orsts_per_s=0,oseg_per_s=16,osrcq_per_s=0,otm_per_s=0,otmex_per_s=0,otmr_per_s=0,packet_per_s=0,passive_per_s=0,rawsck=0,read_per_s=0,retrans_per_s=0,saccess_per_s=0,scall_per_s=0,sgetatt_per_s=0,sread_per_s=0,swrite_per_s=0,tcp-tw=7,tcp_per_s=0,tcpsck=1543,totsck=4052,udp_per_s=0,udpsck=2,write_per_s=0 1459255626661381788
> network,device=ens33 coll_per_s=0,pct_ifutil=0,rxcmp_per_s=0,rxdrop_per_s=0,rxerr_per_s=0,rxfifo_per_s=0,rxfram_per_s=0,rxkB_per_s=0,rxmcst_per_s=0,rxpck_per_s=0,txcarr_per_s=0,txcmp_per_s=0,txdrop_per_s=0,txerr_per_s=0,txfifo_per_s=0,txkB_per_s=0,txpck_per_s=0 1459255626661533072
> disk,device=sda,vg=rootvg avgqu-sz=0.01,avgrq-sz=8.5,await=3.31,pct_util=0.1,rd_sec_per_s=0,svctm=0.25,tps=4,wr_sec_per_s=34 1459255626663974389
> queue blocked=0,ldavg-1=1.61,ldavg-15=1.34,ldavg-5=1.67,plist-sz=1415,runq-sz=0 1459255626664159054
> paging fault_per_s=0.25,majflt_per_s=0,pct_vmeff=0,pgfree_per_s=19,pgpgin_per_s=0,pgpgout_per_s=17,pgscand_per_s=0,pgscank_per_s=0,pgsteal_per_s=0 1459255626664304249
> mem_util kbactive=2206568,kbanonpg=1472208,kbbuffers=118020,kbcached=1035252,kbcommit=8717200,kbdirty=156,kbinact=418912,kbkstack=24672,kbmemfree=1744868,kbmemused=3610272,kbpgtbl=87116,kbslab=233804,kbvmused=0,pct_commit=136.13,pct_memused=67.42 1459255626664554981
> io bread_per_s=0,bwrtn_per_s=34,rtps=0,tps=4,wtps=4 1459255626664596198
> inode dentunusd=235039,file-nr=17120,inode-nr=94505,pty-nr=14 1459255626664663693
> interrupts,device=i000 intr_per_s=0 1459255626664800109
> interrupts,device=i003 intr_per_s=0 1459255626665255145
> interrupts,device=i004 intr_per_s=0 1459255626665281776
> interrupts,device=i006 intr_per_s=0 1459255626665297416
> interrupts,device=i007 intr_per_s=0 1459255626665321008
> interrupts,device=i010 intr_per_s=0 1459255626665339413
> interrupts,device=i012 intr_per_s=0 1459255626665361510
> interrupts,device=i013 intr_per_s=0 1459255626665381327
> interrupts,device=i015 intr_per_s=1 1459255626665397313
> interrupts,device=i001 intr_per_s=0.25 1459255626665412985
> interrupts,device=i002 intr_per_s=0 1459255626665430475
> interrupts,device=i005 intr_per_s=0 1459255626665453944
> interrupts,device=i008 intr_per_s=0 1459255626665470650
> interrupts,device=i011 intr_per_s=0 1459255626665486069
> interrupts,device=i009 intr_per_s=0 1459255626665502913
> interrupts,device=i014 intr_per_s=0 1459255626665518152
> task cswch_per_s=722.25,proc_per_s=0 1459255626665849646
> cpu,device=all pct_idle=98.85,pct_iowait=0,pct_nice=0.38,pct_steal=0,pct_system=0.64,pct_user=0.13 1459255626666639715
> mem bufpg_per_s=0,campg_per_s=1.75,frmpg_per_s=-8.25 1459255626666770205
> swap_util kbswpcad=0,kbswpfree=1048572,kbswpused=0,pct_swpcad=0,pct_swpused=0 1459255626667313276
```

If you change the group value to false like below:
```toml
[[inputs.sysstat]]
  sadc_path = "/usr/lib/sa/sadc" # required
  activities = ["DISK", "SNMP", "INT"]
  group = false
  [inputs.sysstat.options]
	-C = "cpu"
	-B = "paging"
	-b = "io"
	-d = "disk"             # requires DISK activity
	-H = "hugepages"
	"-I ALL" = "interrupts" # requires INT activity
	"-n ALL" = "network"
	"-P ALL" = "per_cpu"
	-q = "queue"
	-R = "mem"
	"-r ALL" = "mem_util"
	-S = "swap_util"
	-u = "cpu_util"
	-v = "inode"
	-W = "swap"
	-w = "task"
  [[inputs.sysstat.device_tags.sda]]
    vg = "rootvg"
```

you get the following output:
```
$ telegraf -config telegraf.conf -input-filter sysstat -test
* Plugin: sysstat, Collection 1
> io_tps value=0.5 1459255780126025822
> io_rtps value=0 1459255780126025822
> io_wtps value=0.5 1459255780126025822
> io_bread_per_s value=0 1459255780126025822
> io_bwrtn_per_s value=38 1459255780126025822
> cpu_util_pct_user,device=all value=39.07 1459255780126025822
> cpu_util_pct_nice,device=all value=0 1459255780126025822
> cpu_util_pct_system,device=all value=47.94 1459255780126025822
> cpu_util_pct_iowait,device=all value=0 1459255780126025822
> cpu_util_pct_steal,device=all value=0 1459255780126025822
> cpu_util_pct_idle,device=all value=12.98 1459255780126025822
> swap_pswpin_per_s value=0 1459255780126025822
> cpu_pct_user,device=all value=39.07 1459255780126025822
> cpu_pct_nice,device=all value=0 1459255780126025822
> cpu_pct_system,device=all value=47.94 1459255780126025822
> cpu_pct_iowait,device=all value=0 1459255780126025822
> cpu_pct_steal,device=all value=0 1459255780126025822
> cpu_pct_idle,device=all value=12.98 1459255780126025822
> per_cpu_pct_user,device=all value=39.07 1459255780126025822
> per_cpu_pct_nice,device=all value=0 1459255780126025822
> per_cpu_pct_system,device=all value=47.94 1459255780126025822
> per_cpu_pct_iowait,device=all value=0 1459255780126025822
> per_cpu_pct_steal,device=all value=0 1459255780126025822
> per_cpu_pct_idle,device=all value=12.98 1459255780126025822
> per_cpu_pct_user,device=cpu0 value=33.5 1459255780126025822
> per_cpu_pct_nice,device=cpu0 value=0 1459255780126025822
> per_cpu_pct_system,device=cpu0 value=65.25 1459255780126025822
> per_cpu_pct_iowait,device=cpu0 value=0 1459255780126025822
> per_cpu_pct_steal,device=cpu0 value=0 1459255780126025822
> per_cpu_pct_idle,device=cpu0 value=1.25 1459255780126025822
> per_cpu_pct_user,device=cpu1 value=44.85 1459255780126025822
> per_cpu_pct_nice,device=cpu1 value=0 1459255780126025822
> per_cpu_pct_system,device=cpu1 value=29.55 1459255780126025822
> per_cpu_pct_iowait,device=cpu1 value=0 1459255780126025822
> per_cpu_pct_steal,device=cpu1 value=0 1459255780126025822
> per_cpu_pct_idle,device=cpu1 value=25.59 1459255780126025822
> hugepages_kbhugfree value=0 1459255780126025822
> hugepages_kbhugused value=0 1459255780126025822
> hugepages_pct_hugused value=0 1459255780126025822
> interrupts_intr_per_s,device=i000 value=0 1459255780126025822
> inode_dentunusd value=252876 1459255780126025822
> mem_util_kbmemfree value=1613612 1459255780126025822
> disk_tps,device=sda,vg=rootvg value=0.5 1459255780126025822
> swap_pswpout_per_s value=0 1459255780126025822
> network_rxpck_per_s,device=ens33 value=0 1459255780126025822
> queue_runq-sz value=4 1459255780126025822
> task_proc_per_s value=0 1459255780126025822
> task_cswch_per_s value=2019 1459255780126025822
> mem_frmpg_per_s value=0 1459255780126025822
> mem_bufpg_per_s value=0.5 1459255780126025822
> mem_campg_per_s value=1.25 1459255780126025822
> interrupts_intr_per_s,device=i001 value=0 1459255780126025822
> inode_file-nr value=19104 1459255780126025822
> mem_util_kbmemused value=3741528 1459255780126025822
> disk_rd_sec_per_s,device=sda,vg=rootvg value=0 1459255780126025822
> network_txpck_per_s,device=ens33 value=0 1459255780126025822
> queue_plist-sz value=1512 1459255780126025822
> paging_pgpgin_per_s value=0 1459255780126025822
> paging_pgpgout_per_s value=19 1459255780126025822
> paging_fault_per_s value=0.25 1459255780126025822
> paging_majflt_per_s value=0 1459255780126025822
> paging_pgfree_per_s value=34.25 1459255780126025822
> paging_pgscank_per_s value=0 1459255780126025822
> paging_pgscand_per_s value=0 1459255780126025822
> paging_pgsteal_per_s value=0 1459255780126025822
> paging_pct_vmeff value=0 1459255780126025822
> interrupts_intr_per_s,device=i002 value=0 1459255780126025822
> interrupts_intr_per_s,device=i003 value=0 1459255780126025822
> interrupts_intr_per_s,device=i004 value=0 1459255780126025822
> interrupts_intr_per_s,device=i005 value=0 1459255780126025822
> interrupts_intr_per_s,device=i006 value=0 1459255780126025822
> interrupts_intr_per_s,device=i007 value=0 1459255780126025822
> interrupts_intr_per_s,device=i008 value=0 1459255780126025822
> interrupts_intr_per_s,device=i009 value=0 1459255780126025822
> interrupts_intr_per_s,device=i010 value=0 1459255780126025822
> interrupts_intr_per_s,device=i011 value=0 1459255780126025822
> interrupts_intr_per_s,device=i012 value=0 1459255780126025822
> interrupts_intr_per_s,device=i013 value=0 1459255780126025822
> interrupts_intr_per_s,device=i014 value=0 1459255780126025822
> interrupts_intr_per_s,device=i015 value=1 1459255780126025822
> inode_inode-nr value=94709 1459255780126025822
> inode_pty-nr value=14 1459255780126025822
> mem_util_pct_memused value=69.87 1459255780126025822
> mem_util_kbbuffers value=118252 1459255780126025822
> mem_util_kbcached value=1045240 1459255780126025822
> mem_util_kbcommit value=9628152 1459255780126025822
> mem_util_pct_commit value=150.35 1459255780126025822
> mem_util_kbactive value=2303752 1459255780126025822
> mem_util_kbinact value=428340 1459255780126025822
> mem_util_kbdirty value=104 1459255780126025822
> mem_util_kbanonpg value=1568676 1459255780126025822
> mem_util_kbslab value=240032 1459255780126025822
> mem_util_kbkstack value=26224 1459255780126025822
> mem_util_kbpgtbl value=98056 1459255780126025822
> mem_util_kbvmused value=0 1459255780126025822
> disk_wr_sec_per_s,device=sda,vg=rootvg value=38 1459255780126025822
> disk_avgrq-sz,device=sda,vg=rootvg value=76 1459255780126025822
> disk_avgqu-sz,device=sda,vg=rootvg value=0 1459255780126025822
> disk_await,device=sda,vg=rootvg value=2 1459255780126025822
> disk_svctm,device=sda,vg=rootvg value=2 1459255780126025822
> disk_pct_util,device=sda,vg=rootvg value=0.1 1459255780126025822
> network_rxkB_per_s,device=ens33 value=0 1459255780126025822
> network_txkB_per_s,device=ens33 value=0 1459255780126025822
> network_rxcmp_per_s,device=ens33 value=0 1459255780126025822
> network_txcmp_per_s,device=ens33 value=0 1459255780126025822
> network_rxmcst_per_s,device=ens33 value=0 1459255780126025822
> network_pct_ifutil,device=ens33 value=0 1459255780126025822
> network_rxpck_per_s,device=lo value=10.75 1459255780126025822
> network_txpck_per_s,device=lo value=10.75 1459255780126025822
> network_rxkB_per_s,device=lo value=0.77 1459255780126025822
> network_txkB_per_s,device=lo value=0.77 1459255780126025822
> network_rxcmp_per_s,device=lo value=0 1459255780126025822
> network_txcmp_per_s,device=lo value=0 1459255780126025822
> network_rxmcst_per_s,device=lo value=0 1459255780126025822
> network_pct_ifutil,device=lo value=0 1459255780126025822
> network_rxerr_per_s,device=ens33 value=0 1459255780126025822
> network_txerr_per_s,device=ens33 value=0 1459255780126025822
> network_coll_per_s,device=ens33 value=0 1459255780126025822
> network_rxdrop_per_s,device=ens33 value=0 1459255780126025822
> network_txdrop_per_s,device=ens33 value=0 1459255780126025822
> network_txcarr_per_s,device=ens33 value=0 1459255780126025822
> network_rxfram_per_s,device=ens33 value=0 1459255780126025822
> network_rxfifo_per_s,device=ens33 value=0 1459255780126025822
> network_txfifo_per_s,device=ens33 value=0 1459255780126025822
> network_rxerr_per_s,device=lo value=0 1459255780126025822
> network_txerr_per_s,device=lo value=0 1459255780126025822
> network_coll_per_s,device=lo value=0 1459255780126025822
> network_rxdrop_per_s,device=lo value=0 1459255780126025822
> network_txdrop_per_s,device=lo value=0 1459255780126025822
> network_txcarr_per_s,device=lo value=0 1459255780126025822
> network_rxfram_per_s,device=lo value=0 1459255780126025822
> network_rxfifo_per_s,device=lo value=0 1459255780126025822
> network_txfifo_per_s,device=lo value=0 1459255780126025822
> network_call_per_s value=0 1459255780126025822
> network_retrans_per_s value=0 1459255780126025822
> network_read_per_s value=0 1459255780126025822
> network_write_per_s value=0 1459255780126025822
> network_access_per_s value=0 1459255780126025822
> network_getatt_per_s value=0 1459255780126025822
> network_scall_per_s value=0 1459255780126025822
> network_badcall_per_s value=0 1459255780126025822
> network_packet_per_s value=0 1459255780126025822
> network_udp_per_s value=0 1459255780126025822
> network_tcp_per_s value=0 1459255780126025822
> network_hit_per_s value=0 1459255780126025822
> network_miss_per_s value=0 1459255780126025822
> network_sread_per_s value=0 1459255780126025822
> network_swrite_per_s value=0 1459255780126025822
> network_saccess_per_s value=0 1459255780126025822
> network_sgetatt_per_s value=0 1459255780126025822
> network_totsck value=4234 1459255780126025822
> network_tcpsck value=1637 1459255780126025822
> network_udpsck value=2 1459255780126025822
> network_rawsck value=0 1459255780126025822
> network_ip-frag value=0 1459255780126025822
> network_tcp-tw value=4 1459255780126025822
> network_irec_per_s value=10.75 1459255780126025822
> network_fwddgm_per_s value=0 1459255780126025822
> network_idel_per_s value=10.75 1459255780126025822
> network_orq_per_s value=10.75 1459255780126025822
> network_asmrq_per_s value=0 1459255780126025822
> network_asmok_per_s value=0 1459255780126025822
> network_fragok_per_s value=0 1459255780126025822
> network_fragcrt_per_s value=0 1459255780126025822
> network_ihdrerr_per_s value=0 1459255780126025822
> network_iadrerr_per_s value=0 1459255780126025822
> network_iukwnpr_per_s value=0 1459255780126025822
> network_idisc_per_s value=0 1459255780126025822
> network_odisc_per_s value=0 1459255780126025822
> network_onort_per_s value=0 1459255780126025822
> network_asmf_per_s value=0 1459255780126025822
> network_fragf_per_s value=0 1459255780126025822
> network_imsg_per_s value=0 1459255780126025822
> network_omsg_per_s value=0 1459255780126025822
> network_iech_per_s value=0 1459255780126025822
> network_iechr_per_s value=0 1459255780126025822
> network_oech_per_s value=0 1459255780126025822
> network_oechr_per_s value=0 1459255780126025822
> network_itm_per_s value=0 1459255780126025822
> network_itmr_per_s value=0 1459255780126025822
> network_otm_per_s value=0 1459255780126025822
> network_otmr_per_s value=0 1459255780126025822
> network_iadrmk_per_s value=0 1459255780126025822
> network_iadrmkr_per_s value=0 1459255780126025822
> network_oadrmk_per_s value=0 1459255780126025822
> network_oadrmkr_per_s value=0 1459255780126025822
> network_ierr_per_s value=0 1459255780126025822
> network_oerr_per_s value=0 1459255780126025822
> network_idstunr_per_s value=0 1459255780126025822
> network_odstunr_per_s value=0 1459255780126025822
> network_itmex_per_s value=0 1459255780126025822
> network_otmex_per_s value=0 1459255780126025822
> network_iparmpb_per_s value=0 1459255780126025822
> network_oparmpb_per_s value=0 1459255780126025822
> network_isrcq_per_s value=0 1459255780126025822
> network_osrcq_per_s value=0 1459255780126025822
> network_iredir_per_s value=0 1459255780126025822
> network_oredir_per_s value=0 1459255780126025822
> network_active_per_s value=0 1459255780126025822
> network_passive_per_s value=0 1459255780126025822
> network_iseg_per_s value=10.75 1459255780126025822
> network_oseg_per_s value=9.5 1459255780126025822
> network_atmptf_per_s value=0 1459255780126025822
> network_estres_per_s value=0 1459255780126025822
> network_retrans_per_s value=1.5 1459255780126025822
> network_isegerr_per_s value=0.25 1459255780126025822
> network_orsts_per_s value=0 1459255780126025822
> network_idgm_per_s value=0 1459255780126025822
> network_odgm_per_s value=0 1459255780126025822
> network_noport_per_s value=0 1459255780126025822
> network_idgmerr_per_s value=0 1459255780126025822
> queue_ldavg-1 value=2.1 1459255780126025822
> queue_ldavg-5 value=1.82 1459255780126025822
> queue_ldavg-15 value=1.44 1459255780126025822
> queue_blocked value=0 1459255780126025822
> swap_util_kbswpfree value=1048572 1459255780126025822
> swap_util_kbswpused value=0 1459255780126025822
> swap_util_pct_swpused value=0 1459255780126025822
> swap_util_kbswpcad value=0 1459255780126025822
> swap_util_pct_swpcad value=0 1459255780126025822
```
