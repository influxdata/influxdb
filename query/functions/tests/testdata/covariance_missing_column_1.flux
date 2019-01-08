inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string
#group,false,false,false,false,false,false,true
#default,_result,,,,,,
,result,table,_start,_stop,_time,x,_measurement
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,0,cpu
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,0,cpu
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,2,cpu
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,7,cpu
"
outData = "
#datatype,string,string
#group,true,true
#default,,
,error,reference
,specified column does not exist in table: y,
"

covariance_missing_column_1 = (table=<-) =>
  table
	|> range(start: 2018-05-22T19:53:26Z)
    |> covariance(columns: ["x", "r"])
	|> yield(name: "0")


testingTest(name: "covariance_missing_column_1",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: covariance_missing_column_1)
