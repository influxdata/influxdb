package httpd_test

import (
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/httpd"
)

func TestResponseWriter_CSV(t *testing.T) {
	header := make(http.Header)
	header.Set("Accept", "text/csv")
	r := &http.Request{
		Header: header,
		URL:    &url.URL{},
	}
	w := httptest.NewRecorder()

	results := make(chan *query.ResultSet)
	go func() {
		defer close(results)
		result := &query.ResultSet{ID: 0}
		results <- result.Init()
		defer result.Close()

		result = result.WithColumns("time", "value")
		series, _ := result.CreateSeriesWithTags("cpu", query.NewTags(map[string]string{
			"host":   "server01",
			"region": "uswest",
		}))
		defer series.Close()

		for _, row := range [][]interface{}{
			{time.Unix(0, 10), float64(2.5)},
			{time.Unix(0, 20), int64(5)},
			{time.Unix(0, 30), nil},
			{time.Unix(0, 40), "foobar"},
			{time.Unix(0, 50), true},
			{time.Unix(0, 60), false},
			{time.Unix(0, 70), uint64(math.MaxInt64 + 1)},
		} {
			series.Emit(row)
		}
	}()

	config := httpd.NewConfig()
	enc := httpd.NewEncoder(r, &config)
	enc.Encode(w, results)

	if got, want := w.Body.String(), `name,tags,time,value
cpu,"host=server01,region=uswest",10,2.5
cpu,"host=server01,region=uswest",20,5
cpu,"host=server01,region=uswest",30,
cpu,"host=server01,region=uswest",40,foobar
cpu,"host=server01,region=uswest",50,true
cpu,"host=server01,region=uswest",60,false
cpu,"host=server01,region=uswest",70,9223372036854775808
`; got != want {
		t.Errorf("unexpected output:\n\ngot=%v\nwant=%s", got, want)
	}
}
