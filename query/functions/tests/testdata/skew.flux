inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,dateTime:RFC3339,unsignedLong
#group,false,false,false,false,true,true,false,false
#default,,,,,,,,
,result,table,_start,_stop,_measurement,_field,_time,_value
,,0,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,DlXwgrw,2018-12-18T22:11:05Z,70
,,0,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,DlXwgrw,2018-12-18T22:11:15Z,48
,,0,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,DlXwgrw,2018-12-18T22:11:25Z,33
,,0,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,DlXwgrw,2018-12-18T22:11:35Z,24
,,0,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,DlXwgrw,2018-12-18T22:11:45Z,38
,,0,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,DlXwgrw,2018-12-18T22:11:55Z,75

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,dateTime:RFC3339,long
#group,false,false,false,false,true,true,false,false
#default,,,,,,,,
,result,table,_start,_stop,_measurement,_field,_time,_value
,,1,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,GxUPYq1,2018-12-18T22:11:05Z,96
,,1,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,GxUPYq1,2018-12-18T22:11:15Z,-44
,,1,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,GxUPYq1,2018-12-18T22:11:25Z,-25
,,1,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,GxUPYq1,2018-12-18T22:11:35Z,46
,,1,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,GxUPYq1,2018-12-18T22:11:45Z,-2
,,1,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,GxUPYq1,2018-12-18T22:11:55Z,-14

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,dateTime:RFC3339,double
#group,false,false,false,false,true,true,false,false
#default,,,,,,,,
,result,table,_start,_stop,_measurement,_field,_time,_value
,,2,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,qaOnnQc,2018-12-18T22:11:05Z,-61.68790887989735
,,2,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,qaOnnQc,2018-12-18T22:11:15Z,-6.3173755351186465
,,2,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,qaOnnQc,2018-12-18T22:11:25Z,-26.049728557657513
,,2,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,qaOnnQc,2018-12-18T22:11:35Z,114.285955884979
,,2,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,qaOnnQc,2018-12-18T22:11:45Z,16.140262630578995
,,2,2018-12-18T22:11:05Z,2018-12-18T22:11:55Z,Sgf,qaOnnQc,2018-12-18T22:11:55Z,29.50336437998469
"
outData = "
#datatype,string,long,string,string,double
#group,false,false,true,true,false
#default,_result,,,,
,result,table,_measurement,_field,_value
,,0,Sgf,DlXwgrw,0.3057387969724791
,,1,Sgf,GxUPYq1,0.7564084754909545
,,2,Sgf,qaOnnQc,0.6793279546139146
"

option now = () => 2018-12-18T22:12:00Z

t_skew = (table=<-) => table
  |> range(start: -5m)
  |> skew()

testingTest(name: "skew",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_skew)
