inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true,true
#default,_result,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,taskID,orgID,status,_measurement
,,0,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01114Z,02bac3c908f37000,runID,02bac3c8f0f37000,02bac3c8d6c5b000,started,records
,,0,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.113222Z,02bac3c922737000,runID,02bac3c8f0f37000,02bac3c8d6c5b000,started,records
,,1,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:10.920529Z,02bac3c8f1737000,runID,02bac3c8f0f37000,02bac3c8d6c5b000,success,records
,,1,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01435Z,02bac3c908f37000,runID,02bac3c8f0f37000,02bac3c8d6c5b000,success,records
,,1,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.115415Z,02bac3c922737000,runID,02bac3c8f0f37000,02bac3c8d6c5b000,success,records
,,2,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01114Z,2018-10-03T17:55:12Z,scheduledFor,02bac3c8f0f37000,02bac3c8d6c5b000,started,records
,,2,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.113222Z,2018-10-03T17:55:13Z,scheduledFor,02bac3c8f0f37000,02bac3c8d6c5b000,started,records
,,3,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:10.920529Z,2018-10-03T17:55:11Z,scheduledFor,02bac3c8f0f37000,02bac3c8d6c5b000,success,records
,,3,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01435Z,2018-10-03T17:55:12Z,scheduledFor,02bac3c8f0f37000,02bac3c8d6c5b000,success,records
,,3,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.115415Z,2018-10-03T17:55:13Z,scheduledFor,02bac3c8f0f37000,02bac3c8d6c5b000,success,records
,,4,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01114Z,1970-01-01T00:00:01Z,requestedAt,02bac3c8f0f37000,02bac3c8d6c5b000,started,records
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string,string
#group,false,false,false,false,false,false,false,false,false,true,false
#default,r1,,,,,,,,,,
,result,table,_start,_stop,_time,taskID,orgID,status,_measurement,runID,scheduledFor
,,0,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:10.920529Z,02bac3c8f0f37000,02bac3c8d6c5b000,success,records,02bac3c8f1737000,2018-10-03T17:55:11Z

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string,string,string
#group,false,false,false,false,false,false,false,false,false,false,true,false
#default,r1,,,,,,,,,,,
,result,table,_start,_stop,_time,taskID,orgID,status,_measurement,requestedAt,runID,scheduledFor
,,1,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01114Z,02bac3c8f0f37000,02bac3c8d6c5b000,started,records,1970-01-01T00:00:01Z,02bac3c908f37000,2018-10-03T17:55:12Z
,,1,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01435Z,02bac3c8f0f37000,02bac3c8d6c5b000,success,records,,02bac3c908f37000,2018-10-03T17:55:12Z
,,2,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.113222Z,02bac3c8f0f37000,02bac3c8d6c5b000,started,records,,02bac3c922737000,2018-10-03T17:55:13Z
,,2,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.115415Z,02bac3c8f0f37000,02bac3c8d6c5b000,success,records,,02bac3c922737000,2018-10-03T17:55:13Z

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,dateTime:RFC3339,dateTime:RFC3339
#group,false,false,true,true,true,true,true,false,false,false
#default,r2,,,,,,,,,
,result,table,_start,_stop,taskID,orgID,_measurement,runID,started,success
,,0,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,02bac3c8f0f37000,02bac3c8d6c5b000,records,02bac3c908f37000,2018-10-03T17:55:11.01114Z,2018-10-03T17:55:11.01435Z
,,0,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,02bac3c8f0f37000,02bac3c8d6c5b000,records,02bac3c922737000,2018-10-03T17:55:11.113222Z,2018-10-03T17:55:11.115415Z
,,0,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,02bac3c8f0f37000,02bac3c8d6c5b000,records,02bac3c8f1737000,1970-01-01T00:00:00Z,2018-10-03T17:55:10.920529Z

#datatype,string,long,string,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,dateTime:RFC3339,string,dateTime:RFC3339,string
#group,false,false,true,true,true,false,true,true,false,false,false,false,true
#default,r3,,,,,,,,,,,,
,result,table,_measurement,_start,_stop,_time,orgID,runID,scheduledFor,started,status,success,taskID
,,0,records,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:10.920529Z,02bac3c8d6c5b000,02bac3c8f1737000,2018-10-03T17:55:11Z,1970-01-01T00:00:00Z,success,2018-10-03T17:55:10.920529Z,02bac3c8f0f37000
,,1,records,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01114Z,02bac3c8d6c5b000,02bac3c908f37000,2018-10-03T17:55:12Z,2018-10-03T17:55:11.01114Z,started,2018-10-03T17:55:11.01435Z,02bac3c8f0f37000
,,1,1970-01-01T00:00:01Z,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.01435Z,02bac3c8d6c5b000,02bac3c908f37000,2018-10-03T17:55:12Z,2018-10-03T17:55:11.01114Z,success,2018-10-03T17:55:11.01435Z,02bac3c8f0f37000
,,2,records,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.113222Z,02bac3c8d6c5b000,02bac3c922737000,2018-10-03T17:55:13Z,2018-10-03T17:55:11.113222Z,started,2018-10-03T17:55:11.115415Z,02bac3c8f0f37000
,,2,,2018-10-02T17:55:11.520461Z,2018-10-03T17:55:11.520461Z,2018-10-03T17:55:11.115415Z,02bac3c8d6c5b000,02bac3c922737000,2018-10-03T17:55:13Z,2018-10-03T17:55:11.113222Z,success,2018-10-03T17:55:11.115415Z,02bac3c8f0f37000
"

supl = testLoadStorage(csv: inData)
  |> range(start: 2018-10-02T17:55:11.520461Z)
  |> filter(fn: (r) => r._measurement == "records" and r.taskID == "02bac3c8f0f37000" )
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["runID"])



main = testLoadStorage(csv: inData)
  |> range(start: 2018-10-02T17:55:11.520461Z)
  |> filter(fn: (r) => r._measurement == "records" and r.taskID == "02bac3c8f0f37000" )
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> pivot(rowKey:["runID"], columnKey: ["status"], valueColumn: "_time")


got = join(tables: {main: main, supl: supl}, on: ["_start", "_stop", "orgID", "taskID", "runID", "_measurement"])
want = testLoadStorage(csv: outData)
assertEquals(name: "task_per_line", want: want, got: got)