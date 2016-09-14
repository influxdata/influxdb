package influx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/influx"
	"golang.org/x/net/context"
)

func Test_MakesRequestsToQueryEndpoint(t *testing.T) {
	t.Parallel()
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(`{}`))
		called = true
		if path := r.URL.Path; path != "/query" {
			t.Error("Expected the path to contain `/query` but was", path)
		}
	}))
	defer ts.Close()

	var series mrfusion.TimeSeries
	series, err := influx.NewClient(ts.URL)
	if err != nil {
		t.Fatal("Unexpected error initializing client: err:", err)
	}

	_, err = series.Query(context.Background(), "show databases")
	if err != nil {
		t.Fatal("Expected no error but was", err)
	}

	if called == false {
		t.Error("Expected http request to Influx but there was none")
	}
}
