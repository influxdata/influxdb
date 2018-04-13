package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/chronograf"
)

func TestService_Influx(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		TimeSeries   chronograf.TimeSeries
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		ID              string
		wantStatus      int
		wantContentBody string
		wantBody        string
	}{}
}
