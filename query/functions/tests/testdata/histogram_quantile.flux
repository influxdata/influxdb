inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,double,double
#group,false,false,true,true,true,true,false,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_field,_value,le
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,1,0.1
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,2,0.2
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,2,0.3
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,2,0.4
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,2,0.5
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,2,0.6
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,2,0.7
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,8,0.8
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,10,0.9
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,10,+Inf
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,0,-Inf
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,10,0.2
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,15,0.4
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,25,0.6
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,35,0.8
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,45,1.0
,,2,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,45,+Inf
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,double
#group,false,false,true,true,true,true,false
#default,_result,,,,,,
,result,table,_start,_stop,_time,_field,_value
,,0,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,x_duration_seconds,0.8500000000000001
,,1,2018-05-22T19:53:00Z,2018-05-22T19:54:00Z,2018-05-22T19:53:00Z,y_duration_seconds,0.91
"

t_histogram_quantile = (table=<-) =>
  table
    |> range(start: 2018-05-22T19:53:00Z)
    |> histogramQuantile(quantile:0.90,upperBoundColumn:"le")

testingTest(name: "histogram_quantile",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_histogram_quantile)