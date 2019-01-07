inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string
#group,false,false,false,false,false,false,true,true
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_value,_measurement,user
,,0,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:26Z,1,RAM,user1
,,0,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:36Z,2,RAM,user1
,,0,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:46Z,3,RAM,user1
,,0,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:56Z,5,RAM,user1
,,1,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:26Z,0,CPU,user1
,,1,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:36Z,1,CPU,user1
,,2,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:26Z,2,RAM,user2
,,2,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:36Z,4,RAM,user2
,,2,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:46Z,4,RAM,user2
,,2,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:56Z,0,RAM,user2
,,2,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:54:06Z,2,RAM,user2
,,2,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:54:16Z,10,RAM,user2
,,3,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:26Z,4,CPU,user2
,,3,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:36Z,20,CPU,user2
,,3,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:46Z,7,CPU,user2
,,3,2018-05-22T19:53:00Z,2018-05-22T19:55:00Z,2018-05-22T19:53:56Z,10,CPU,user2
"
outData = "
#datatype,string,long,string,double
#group,false,false,true,false
#default,_result,,,
,result,table,_measurement,_value
,,0,CPU,8
,,1,RAM,-1.83333333333333333
"

t_cov = () => {
    left = testLoadStorage(csv: inData)
        |> range(start:2018-05-22T19:53:00Z, stop:2018-05-22T19:55:00Z)
        |> drop(columns: ["_start", "_stop"])
        |> filter(fn: (r) => r.user == "user1")
        |> group(columns: ["_measurement"])

    right = testLoadStorage(csv: inData)
        |> range(start:2018-05-22T19:53:00Z, stop:2018-05-22T19:55:00Z)
        |> drop(columns: ["_start", "_stop"])
        |> filter(fn: (r) => r.user == "user2")
        |> group(columns: ["_measurement"])

    got = cov(x:left, y:right, on: ["_time", "_measurement"])
    want = testLoadStorage(csv: outData)
    return assertEquals(name: "cov", want: want, got: got)
}

t_cov()
