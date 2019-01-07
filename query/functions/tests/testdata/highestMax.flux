inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,false,false,false,false,true,true,false
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,1,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T00:00:00Z,10,A,BB,HostA
,,1,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T01:00:00Z,20,A,BB,HostA
,,1,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T02:00:00Z,30,A,BB,HostA
,,2,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T03:00:00Z,15,A,CC,HostA
,,2,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T04:00:00Z,21,A,CC,HostA
,,2,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T05:00:00Z,33,A,CC,HostB
,,3,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T06:00:00Z,18,A,DD,HostC
,,3,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T07:00:00Z,12,A,DD,HostC
,,3,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T08:00:00Z,15,A,DD,HostC
,,4,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T09:00:00Z,25,B,CC,HostD
,,4,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T10:00:00Z,75,B,CC,HostD
,,4,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T11:00:00Z,50,B,CC,HostD
,,5,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T12:00:00Z,10,B,DD,HostD
,,5,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T13:00:00Z,13,B,DD,HostE
,,5,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T14:00:00Z,27,B,DD,HostE
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,false,false,false,false,false,false,false
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T10:00:00Z,75,B,CC,HostD
,,0,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T05:00:00Z,33,A,CC,HostB
,,0,2018-11-07T00:00:00Z,2018-11-08T00:00:00Z,2018-11-07T02:00:00Z,30,A,BB,HostA
"

t_highestMax = (table=<-) =>
  table
    |> highestMax(n: 3, groupColumns: ["_measurement", "host"])

testingTest(name: "highestMax",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_highestMax)