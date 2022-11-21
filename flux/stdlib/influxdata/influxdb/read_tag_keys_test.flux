package universe_test

import "testing"
import "testing/expect"
import "planner"
import "csv"

option now = () => (2030-01-01T00:00:00Z)

testcase read_tag_keys {
    expect.planner(rules: ["PushDownReadTagKeysRule": 1])

    input = "
#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.72
,,0,2018-05-22T19:53:46Z,system,host.local,load1,1.74
,,0,2018-05-22T19:53:56Z,system,host.local,load1,1.63
,,0,2018-05-22T19:54:06Z,system,host.local,load1,1.91
,,0,2018-05-22T19:54:16Z,system,host.local,load1,1.84

,,1,2018-05-22T19:53:26Z,system,host.local,load3,1.98
,,1,2018-05-22T19:53:36Z,system,host.local,load3,1.97
,,1,2018-05-22T19:53:46Z,system,host.local,load3,1.97
,,1,2018-05-22T19:53:56Z,system,host.local,load3,1.96
,,1,2018-05-22T19:54:06Z,system,host.local,load3,1.98
,,1,2018-05-22T19:54:16Z,system,host.local,load3,1.97

,,2,2018-05-22T19:53:26Z,system,host.local,load5,1.95
,,2,2018-05-22T19:53:36Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:46Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:56Z,system,host.local,load5,1.89
,,2,2018-05-22T19:54:06Z,system,host.local,load5,1.94
,,2,2018-05-22T19:54:16Z,system,host.local,load5,1.93

#datatype,string,long,dateTime:RFC3339,string,string,string,string,double
#group,false,false,false,true,true,true,true,false
#default,_result,,,,,,,
,result,table,_time,_measurement,region,host,_field,_value
,,3,2018-05-22T19:53:26Z,system,us-east,host.local,load1,10
,,3,2018-05-22T19:53:36Z,system,us-east,host.local,load1,11
,,3,2018-05-22T19:53:46Z,system,us-east,host.local,load1,18
,,3,2018-05-22T19:53:56Z,system,us-east,host.local,load1,19
,,3,2018-05-22T19:54:06Z,system,us-east,host.local,load1,17
,,3,2018-05-22T19:54:16Z,system,us-east,host.local,load1,17

,,4,2018-05-22T19:53:26Z,system,us-east,host.local,load3,16
,,4,2018-05-22T19:53:36Z,system,us-east,host.local,load3,16
,,4,2018-05-22T19:53:46Z,system,us-east,host.local,load3,15
,,4,2018-05-22T19:53:56Z,system,us-east,host.local,load3,19
,,4,2018-05-22T19:54:06Z,system,us-east,host.local,load3,19
,,4,2018-05-22T19:54:16Z,system,us-east,host.local,load3,19

,,5,2018-05-22T19:53:26Z,system,us-west,host.local,load5,19
,,5,2018-05-22T19:53:36Z,system,us-west,host.local,load5,22
,,5,2018-05-22T19:53:46Z,system,us-west,host.local,load5,11
,,5,2018-05-22T19:53:56Z,system,us-west,host.local,load5,12
,,5,2018-05-22T19:54:06Z,system,us-west,host.local,load5,13
,,5,2018-05-22T19:54:16Z,system,us-west,host.local,load5,13
"

    want = csv.from(
        csv: "
#datatype,string,long,string
#group,false,false,false
#default,0,,
,result,table,_value
,,0,_field
,,0,_measurement
,,0,_start
,,0,_stop
,,0,host
,,0,region
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> filter(fn: (r) => true)
        |> keys()
        |> keep(columns: ["_value"])
        |> distinct()
        |> sort()
    testing.diff(want: want, got: result)
}

testcase read_tag_keys_with_predicate {
    expect.planner(rules: ["PushDownReadTagKeysRule": 1])

    input = "
#group,false,false,false,false,true,true,true,true,true,true,true
#datatype,string,long,dateTime:RFC3339,long,string,string,string,string,string,string,string
#default,_result,,,,,,,,,,
,result,table,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,0,2020-10-21T20:48:30Z,4881964326,inodes_free,disk,disk1s5,apfs,euterpe.local,ro,/
,,0,2020-10-21T20:48:40Z,4881964326,inodes_free,disk,disk1s5,apfs,euterpe.local,ro,/
,,0,2020-10-21T20:48:50Z,4881964326,inodes_free,disk,disk1s5,apfs,euterpe.local,ro,/
,,1,2020-10-21T20:48:30Z,4294963701,inodes_free,disk,disk2s1,hfs,euterpe.local,ro,/Volumes/IntelliJ IDEA CE
,,1,2020-10-21T20:48:40Z,4294963701,inodes_free,disk,disk2s1,hfs,euterpe.local,ro,/Volumes/IntelliJ IDEA CE
,,1,2020-10-21T20:48:50Z,4294963701,inodes_free,disk,disk2s1,hfs,euterpe.local,ro,/Volumes/IntelliJ IDEA CE
,,2,2020-10-21T20:48:30Z,488514,inodes_used,disk,disk1s5,apfs,euterpe.local,ro,/
,,2,2020-10-21T20:48:40Z,488514,inodes_used,disk,disk1s5,apfs,euterpe.local,ro,/
,,2,2020-10-21T20:48:50Z,488514,inodes_used,disk,disk1s5,apfs,euterpe.local,ro,/
,,3,2020-10-21T20:48:30Z,3578,inodes_used,disk,disk2s1,hfs,euterpe.local,ro,/Volumes/IntelliJ IDEA CE
,,3,2020-10-21T20:48:40Z,3578,inodes_used,disk,disk2s1,hfs,euterpe.local,ro,/Volumes/IntelliJ IDEA CE
,,3,2020-10-21T20:48:50Z,3578,inodes_used,disk,disk2s1,hfs,euterpe.local,ro,/Volumes/IntelliJ IDEA CE

#group,false,false,false,false,true,true,true,true,true
#datatype,string,long,dateTime:RFC3339,double,string,string,string,string,string
#default,_result,,,,,,,,
,result,table,_time,_value,_field,_measurement,cpu,host,arch
,,4,2020-10-21T20:48:30Z,69.30000000167638,usage_idle,cpu,cpu0,euterpe.local,amd64
,,4,2020-10-21T20:48:40Z,67.36736736724372,usage_idle,cpu,cpu0,euterpe.local,amd64
,,4,2020-10-21T20:48:50Z,69.23076923005354,usage_idle,cpu,cpu0,euterpe.local,amd64
,,5,2020-10-21T20:48:30Z,96.10000000102445,usage_idle,cpu,cpu1,euterpe.local,amd64
,,5,2020-10-21T20:48:40Z,95.70000000055181,usage_idle,cpu,cpu1,euterpe.local,amd64
,,5,2020-10-21T20:48:50Z,95.89999999860534,usage_idle,cpu,cpu1,euterpe.local,amd64

#group,false,false,true,true,false,false,true,true,true
#datatype,string,long,string,string,dateTime:RFC3339,double,string,string,string
#default,_result,,,,,,,,
,result,table,_field,_measurement,_time,_value,cpu,host,region
,,6,usage_user,cpu,2020-10-21T20:48:30Z,19.30000000007567,cpu0,euterpe.local,north
,,6,usage_user,cpu,2020-10-21T20:48:40Z,20.020020020038682,cpu0,euterpe.local,north
,,6,usage_user,cpu,2020-10-21T20:48:50Z,18.581418581407107,cpu0,euterpe.local,north
,,7,usage_user,cpu,2020-10-21T20:48:30Z,2.3000000000138243,cpu1,euterpe.local,north
,,7,usage_user,cpu,2020-10-21T20:48:40Z,2.4000000000536965,cpu1,euterpe.local,north
,,7,usage_user,cpu,2020-10-21T20:48:50Z,2.0999999999423746,cpu1,euterpe.local,north
"

    want = csv.from(
        csv: "
#datatype,string,long,string
#group,false,false,false
#default,0,,
,result,table,_value
,,0,_field
,,0,_measurement
,,0,_start
,,0,_stop
,,0,cpu
,,0,host
,,0,region
",
    )
    result = testing.loadStorage(csv: input)
        |> range(start: -100y)
        |> filter(fn: (r) => r._field == "usage_user")
        |> keys()
        |> keep(columns: ["_value"])
        |> distinct()
        |> sort()

    testing.diff(want: want, got: result)
}
