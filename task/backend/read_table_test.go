package backend

import (
	"bytes"
	"io"
	"testing"

	"github.com/influxdata/flux/csv"
	"go.uber.org/zap/zaptest"
)

func TestReadTable(t *testing.T) {
	encoded := []byte(`group,false,false,true,true,false,true,false,false,false,false,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string,string,string
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_time,taskID,finishedAt,logs,runID,scheduledFor,startedAt,status
,,0,2019-07-23T20:06:24.369913228Z,2019-07-23T20:11:24.369913228Z,2019-07-23T20:06:30.232988837Z,0432e57782b51000,2019-07-23T20:06:30.300005674Z,"[{""runID"":""04341baa937a1000"",""time"":""2019-07-23T20:06:30.223615661Z"",""message"":""Started task from script: \""option v = {timeRangeStart: -1h, timeRangeStop: now(), windowPeriod: 15000ms}\\noption task = {name: \\\""howdy\\\"", every: 10s}\\n\\nfrom(bucket: \\\""goller+acc1's Bucket\\\"")\\n\\t|\u003e range(start: v.timeRangeStart, stop: v.timeRangeStop)\\n\\t|\u003e filter(fn: (r) =\u003e\\n\\t\\t(r._measurement == \\\""disk\\\""))\\n\\t|\u003e filter(fn: (r) =\u003e\\n\\t\\t(r._field == \\\""free\\\""))\\n\\t|\u003e filter(fn: (r) =\u003e\\n\\t\\t(r.device == \\\""disk1s1\\\""))\\n\\t|\u003e aggregateWindow(every: v.windowPeriod, fn: mean)\\n\\t|\u003e yield(name: \\\""mean\\\"")\\n\\t|\u003e to(bucket: \\\""goller+acc1's Bucket\\\"", org: \\\""goller+acc1@gmail.com\\\"")\""""},{""runID"":""04341baa937a1000"",""time"":""2019-07-23T20:06:30.291420936Z"",""message"":""Run failed to execute: panic: column _value:float is not of type int""},{""runID"":""04341baa937a1000"",""time"":""2019-07-23T20:06:30.295269151Z"",""message"":""Failed""}]",04341baa937a1000,2019-07-23T20:06:30Z,2019-07-23T20:06:30.232988837Z,failed
,,0,2019-07-23T20:06:24.369913228Z,2019-07-23T20:11:24.369913228Z,2019-07-23T20:06:40.215226536Z,0432e57782b51000,2019-07-23T20:06:40.284116882Z,"[{""runID"":""04341bb4543a1000"",""time"":""2019-07-23T20:06:40.210364486Z"",""message"":""Started task from script: \""option v = {timeRangeStart: -1h, timeRangeStop: now(), windowPeriod: 15000ms}\\noption task = {name: \\\""howdy\\\"", every: 10s}\\n\\nfrom(bucket: \\\""goller+acc1's Bucket\\\"")\\n\\t|\u003e range(start: v.timeRangeStart, stop: v.timeRangeStop)\\n\\t|\u003e filter(fn: (r) =\u003e\\n\\t\\t(r._measurement == \\\""disk\\\""))\\n\\t|\u003e filter(fn: (r) =\u003e\\n\\t\\t(r._field == \\\""free\\\""))\\n\\t|\u003e filter(fn: (r) =\u003e\\n\\t\\t(r.device == \\\""disk1s1\\\""))\\n\\t|\u003e aggregateWindow(every: v.windowPeriod, fn: mean)\\n\\t|\u003e yield(name: \\\""mean\\\"")\\n\\t|\u003e to(bucket: \\\""goller+acc1's Bucket\\\"", org: \\\""goller+acc1@gmail.com\\\"")\""""},{""runID"":""04341bb4543a1000"",""time"":""2019-07-23T20:06:40.277078243Z"",""message"":""Run failed to execute: panic: column _value:float is not of type int""},{""runID"":""04341bb4543a1000"",""time"":""2019-07-23T20:06:40.280158006Z"",""message"":""Failed""}]",04341bb4543a1000,2019-07-23T20:06:40Z,2019-07-23T20:06:40.215226536Z,failed`)

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	itr, err := decoder.Decode(io.NopCloser(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("got error decoding csv: %v", err)
	}

	defer itr.Release()
	re := &runReader{log: zaptest.NewLogger(t)}

	for itr.More() {
		err := itr.Next().Tables().Do(re.readTable)
		if err != nil {
			t.Fatalf("received error in runs table: %v", err)
		}
	}

	if itr.Err() != nil {
		t.Fatalf("got error from iterator %v", itr.Err())
	}
}
