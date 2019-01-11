inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,dateTime:RFC3339,double
#group,false,false,false,false,true,true,false,false
#default,,,,,,,,
,result,table,_start,_stop,_measurement,_field,_time,_value
,,0,2018-12-18T21:12:45Z,2018-12-18T21:13:35Z,SOYcRk,NC7N,2018-12-18T21:12:45Z,-61.68790887989735
,,0,2018-12-18T21:12:45Z,2018-12-18T21:13:35Z,SOYcRk,NC7N,2018-12-18T21:12:55Z,-6.3173755351186465
,,0,2018-12-18T21:12:45Z,2018-12-18T21:13:35Z,SOYcRk,NC7N,2018-12-18T21:13:05Z,-26.049728557657513
,,0,2018-12-18T21:12:45Z,2018-12-18T21:13:35Z,SOYcRk,NC7N,2018-12-18T21:13:15Z,114.285955884979
,,0,2018-12-18T21:12:45Z,2018-12-18T21:13:35Z,SOYcRk,NC7N,2018-12-18T21:13:25Z,16.140262630578995
,,0,2018-12-18T21:12:45Z,2018-12-18T21:13:35Z,SOYcRk,NC7N,2018-12-18T21:13:35Z,29.50336437998469
"
outData = "
#datatype,string,long,string,string,double
#group,false,false,true,true,false
#default,_result,,,,
,result,table,_measurement,_field,_value
,,0,SOYcRk,NC7N,26.162588942633263
"

option now = () => 2018-12-18T21:14:00Z

t_percentile = (table=<-) => table
    |> range(start: -2m)
    |> percentile(percentile: 0.75, method: "exact_mean")

testingTest(name: "percentile_aggregate",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_percentile)
