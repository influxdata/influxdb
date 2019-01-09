inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,161,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:26Z,82.9833984375,used_percent,swap,host.local
,,161,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:36Z,82.598876953125,used_percent,swap,host.local
,,161,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:46Z,82.598876953125,used_percent,swap,host.local
,,161,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:56Z,82.598876953125,used_percent,swap,host.local
,,161,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:06Z,82.598876953125,used_percent,swap,host.local
,,161,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:16Z,82.6416015625,used_percent,swap,host.local
,,162,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:26Z,1.83,load1,system,host.local
,,162,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:36Z,1.7,load1,system,host.local
,,162,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:46Z,1.74,load1,system,host.local
,,162,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:56Z,1.63,load1,system,host.local
,,162,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:06Z,1.91,load1,system,host.local
,,162,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:16Z,1.84,load1,system,host.local
,,163,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:26Z,1.98,load15,system,host.local
,,163,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:36Z,1.97,load15,system,host.local
,,163,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:46Z,1.97,load15,system,host.local
,,163,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:56Z,1.96,load15,system,host.local
,,163,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:06Z,1.98,load15,system,host.local
,,163,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:16Z,1.97,load15,system,host.local
,,164,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:26Z,1.95,load5,system,host.local
,,164,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:36Z,1.92,load5,system,host.local
,,164,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:46Z,1.92,load5,system,host.local
,,164,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:56Z,1.89,load5,system,host.local
,,164,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:06Z,1.94,load5,system,host.local
,,164,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:16Z,1.93,load5,system,host.local
"
outData = "
#datatype,string,long,dateTime:RFC3339,string,double,double,double,double
#group,false,false,false,true,false,false,false,false
#default,0,,,,,,,
,result,table,_time,host,load1_system,load15_system,load5_system,used_percent_swap
,,0,2018-05-22T19:53:26Z,host.local,1.83,1.98,1.95,82.9833984375
,,0,2018-05-22T19:53:36Z,host.local,1.7,1.97,1.92,82.598876953125
,,0,2018-05-22T19:53:46Z,host.local,1.74,1.97,1.92,82.598876953125
,,0,2018-05-22T19:53:56Z,host.local,1.63,1.96,1.89,82.598876953125
,,0,2018-05-22T19:54:06Z,host.local,1.91,1.98,1.94,82.598876953125
,,0,2018-05-22T19:54:16Z,host.local,1.84,1.97,1.93,82.6416015625
"

t_pivot = (table=<-) =>
  table
  |> range(start: 2018-05-22T19:53:26Z)
  |> pivot(rowKey: ["_time"], columnKey: ["_measurement", "_field"], valueColumn: "_value")
  |> yield(name:"0")

testingTest(name: "pivot",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_pivot)