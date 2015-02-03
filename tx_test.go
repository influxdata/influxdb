package influxdb_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
)

// Ensure a transaction can retrieve a list of iterators for a simple SELECT statement.
func TestTx_CreateIterators(t *testing.T) {
	t.Skip()
	s := OpenDefaultServer(NewMessagingClient())
	defer s.Close()

	// Write to us-east
	s.MustWriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": "serverA", "service": "redis"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Values: map[string]interface{}{"value": float64(100)}}})
	s.MustWriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": "serverB", "service": "redis"}, Timestamp: mustParseTime("2000-01-01T00:00:10Z"), Values: map[string]interface{}{"value": float64(90)}}})
	s.MustWriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": "serverC"}, Timestamp: mustParseTime("2000-01-01T00:00:20Z"), Values: map[string]interface{}{"value": float64(80)}}})
	s.MustWriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-east", "host": "serverA", "service": "redis"}, Timestamp: mustParseTime("2000-01-01T00:00:30Z"), Values: map[string]interface{}{"value": float64(70)}}})

	// Write to us-west
	s.MustWriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west", "host": "serverD"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Values: map[string]interface{}{"value": float64(1)}}})
	s.MustWriteSeries("db", "raw", []influxdb.Point{{Name: "cpu", Tags: map[string]string{"region": "us-west", "host": "serverE", "service": "redis"}, Timestamp: mustParseTime("2000-01-01T00:00:00Z"), Values: map[string]interface{}{"value": float64(2)}}})

	// Create a statement to iterate over.
	// stmt := MustParseSelectStatement(`
	// 	SELECT value
	// 	FROM "db"."raw"."cpu"
	// 	WHERE (((service = 'redis' AND region = 'us-west' AND value > 20) AND host = 'serverE') OR service='redis') AND (time >= '2000-01-01' AND time < '2000-01-02')
	// 	GROUP BY time(1h), region`)

	stmt := MustParseSelectStatement(`
		SELECT value
		FROM "db"."raw"."cpu"
		WHERE (service = 'redis' AND value < 100) AND (time >= '2000-01-01' AND time < '2000-01-02')
		GROUP BY time(1h), region`)

	// Retrieve iterators from server.
	tx, err := s.Begin()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tx.SetNow(mustParseTime("2000-01-01T00:00:00Z"))

	// Retrieve iterators from transaction.
	itrs, err := tx.CreateIterators(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	} else if n := len(itrs); n != 2 {
		t.Fatalf("iterator count: %d", n)
	}

	// Open transaction.
	if err := tx.Open(); err != nil {
		t.Fatalf("tx open error: %s", err)
	}
	defer tx.Close()

	// Iterate over each one.
	if data := slurp(itrs); !reflect.DeepEqual(data, []keyValue{
		{key: 946684800000000000, value: float64(2), tags: "\x00\aus-west"},
		{key: 946684810000000000, value: float64(90), tags: "\x00\aus-east"},
		{key: 946684830000000000, value: float64(70), tags: "\x00\aus-east"},
	}) {
		t.Fatalf("unexpected data: %#v", data)
	}
}

func slurp(itrs []influxql.Iterator) []keyValue {
	var rows []keyValue
	for _, itr := range itrs {
		for k, v := itr.Next(); k != 0; k, v = itr.Next() {
			rows = append(rows, keyValue{key: k, value: v, tags: itr.Tags()})
		}
	}
	sort.Sort(keyValueSlice(rows))
	return rows
}

type keyValue struct {
	key   int64
	value interface{}
	tags  string
}

type keyValueSlice []keyValue

func (p keyValueSlice) Len() int           { return len(p) }
func (p keyValueSlice) Less(i, j int) bool { return p[i].key < p[j].key || p[i].tags < p[j].tags }
func (p keyValueSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
