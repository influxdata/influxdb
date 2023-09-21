# Load into influxdb 1.8:
#
# curl localhost:8086/write\?db=top_bottom --data-binary "@influxdb_iox/tests/query_tests/data/top_bottom.lp"
#
# Float data, regular intervals, usage_system has gaps
#
cpu,cpu=cpu0,machine=machine1 usage_idle=89.5,usage_system=89.5  0000000060000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=79.5,usage_system=79.5  0000000060000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8,usage_system=99.8  0000000060000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8,usage_system=89.8  0000000060000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=88.6,usage_system=88.6  0000000070000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=78.6,usage_system=78.6  0000000070000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.7,usage_system=99.7  0000000070000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.7,usage_system=89.7  0000000070000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=88.6                    0000000080000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=78.6                    0000000080000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8                    0000000080000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8                    0000000080000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.7                    0000000090000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.7                    0000000090000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=83.4,usage_system=83.4  0000000090000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=73.4,usage_system=73.4  0000000090000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.7                    0000000100000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.7                    0000000100000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=87.7,usage_system=87.7  0000000100000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=77.7,usage_system=77.7  0000000100000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=88.7                    0000000110000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=78.7                    0000000110000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.3                    0000000110000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.3                    0000000110000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=86.9                    0000000120000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=76.9                    0000000120000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.9,usage_system=99.9  0000000120000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.9,usage_system=89.9  0000000120000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=89.8,usage_system=89.8  0000000130000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=79.8,usage_system=79.8  0000000130000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8,usage_system=99.8  0000000130000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8,usage_system=89.8  0000000130000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=89.0                    0000000140000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=79.0                    0000000140000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.9,usage_system=99.9  0000000140000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.9,usage_system=89.9  0000000140000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=90.4                    0000000150000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=80.4                    0000000150000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.9                    0000000150000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.9                    0000000150000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=90.2                    0000000160000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=80.2                    0000000160000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8                    0000000160000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8                    0000000160000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8                    0000000170000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8                    0000000170000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=89.8,usage_system=89.8  0000000170000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=79.8,usage_system=79.8  0000000170000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=90.0,usage_system=90.0  0000000180000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=80.0,usage_system=80.0  0000000180000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8,usage_system=99.8  0000000180000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8,usage_system=89.8  0000000180000000000
cpu,cpu=cpu0,machine=machine1 usage_idle=88.8                    0000000190000000000
cpu,cpu=cpu0,machine=machine2 usage_idle=78.8                    0000000190000000000
cpu,cpu=cpu1,machine=machine1 usage_idle=99.8,usage_system=99.8  0000000190000000000
cpu,cpu=cpu1,machine=machine2 usage_idle=89.8,usage_system=89.8  0000000190000000000

# integers at regular intervals
diskio,name=disk0 reads=2591520i,writes=5591520i  0000000060000000000
diskio,name=disk0                writes=5591620i  0000000070000000000
diskio,name=disk0                writes=5591729i  0000000080000000000
diskio,name=disk0                writes=5592114i  0000000090000000000
diskio,name=disk0                writes=5592210i  0000000100000000000
diskio,name=disk0 reads=2592366i,writes=5592366i  0000000110000000000
diskio,name=disk0 reads=2592576i,writes=5592576i  0000000120000000000
diskio,name=disk0 reads=2592646i,writes=5592646i  0000000130000000000
diskio,name=disk0                writes=5592810i  0000000140000000000
diskio,name=disk0 reads=2592997i,writes=5592997i  0000000150000000000
diskio,name=disk0                writes=5593109i  0000000160000000000
diskio,name=disk0 reads=2593219i,writes=5593219i  0000000170000000000
diskio,name=disk0 reads=2593438i,writes=5593438i  0000000180000000000
diskio,name=disk0                writes=5593513i  0000000190000000000
diskio,name=disk0 reads=2593589i,writes=5593589i  0000000200000000000
diskio,name=disk0 reads=2593735i,writes=5593735i  0000000210000000000
