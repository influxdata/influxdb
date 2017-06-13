package httpd_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
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

	writer := httpd.NewResponseWriter(w, r)
	writer.WriteResponse(httpd.Response{
		Results: []*influxql.Result{
			{
				StatementID: 0,
				Series: []*models.Row{
					{
						Name: "cpu",
						Tags: map[string]string{
							"host":   "server01",
							"region": "uswest",
						},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							{time.Unix(0, 10), float64(2.5)},
							{time.Unix(0, 20), int64(5)},
							{time.Unix(0, 30), nil},
							{time.Unix(0, 40), "foobar"},
							{time.Unix(0, 50), true},
							{time.Unix(0, 60), false},
						},
					},
				},
			},
		},
	})

	if got, want := w.Body.String(), `name,tags,time,value
cpu,"host=server01,region=uswest",10,2.5
cpu,"host=server01,region=uswest",20,5
cpu,"host=server01,region=uswest",30,
cpu,"host=server01,region=uswest",40,foobar
cpu,"host=server01,region=uswest",50,true
cpu,"host=server01,region=uswest",60,false
`; got != want {
		t.Errorf("unexpected output:\n\ngot=%v\nwant=%s", got, want)
	}
}

// discard is an http.ResponseWriter that discards all output.
type discard struct{}

func (discard) Header() http.Header            { return http.Header{} }
func (discard) WriteHeader(int)                {}
func (discard) Write(data []byte) (int, error) { return len(data), nil }

func BenchmarkJSONResponseWriter_1K(b *testing.B) {
	benchmarkResponseWriter(b, "application/json", 10, 100)
}
func BenchmarkJSONResponseWriter_100K(b *testing.B) {
	benchmarkResponseWriter(b, "application/json", 1000, 100)
}
func BenchmarkJSONResponseWriter_1M(b *testing.B) {
	benchmarkResponseWriter(b, "application/json", 10000, 100)
}

func BenchmarkMsgpackResponseWriter_1K(b *testing.B) {
	benchmarkResponseWriter(b, "application/x-msgpack", 10, 100)
}
func BenchmarkMsgpackResponseWriter_100K(b *testing.B) {
	benchmarkResponseWriter(b, "application/x-msgpack", 1000, 100)
}
func BenchmarkMsgpackResponseWriter_1M(b *testing.B) {
	benchmarkResponseWriter(b, "application/x-msgpack", 10000, 100)
}

func BenchmarkCSVResponseWriter_1K(b *testing.B) {
	benchmarkResponseWriter(b, "text/csv", 10, 100)
}
func BenchmarkCSVResponseWriter_100K(b *testing.B) {
	benchmarkResponseWriter(b, "text/csv", 1000, 100)
}
func BenchmarkCSVResponseWriter_1M(b *testing.B) {
	benchmarkResponseWriter(b, "text/csv", 10000, 100)
}

func benchmarkResponseWriter(b *testing.B, contentType string, seriesN, pointsPerSeriesN int) {
	r, err := http.NewRequest("POST", "/query", nil)
	if err != nil {
		b.Fatal(err)
	}
	r.Header.Set("Accept", contentType)

	// Generate a sample result.
	rows := make(models.Rows, 0, seriesN)
	for i := 0; i < seriesN; i++ {
		row := &models.Row{
			Name: "cpu",
			Tags: map[string]string{
				"host": fmt.Sprintf("server-%d", i),
			},
			Columns: []string{"time", "value"},
			Values:  make([][]interface{}, 0, b.N),
		}

		for j := 0; j < pointsPerSeriesN; j++ {
			row.Values = append(row.Values, []interface{}{
				time.Unix(int64(10*j), 0),
				float64(100),
			})
		}
		rows = append(rows, row)
	}
	result := &influxql.Result{Series: rows}

	// Create new ResponseWriter with the underlying ResponseWriter
	// being the discard writer so we only benchmark the marshaling.
	w := httpd.NewResponseWriter(discard{}, r)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w.WriteResponse(httpd.Response{
			Results: []*influxql.Result{result},
		})
	}
}
