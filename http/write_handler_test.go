package http

import (
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	platform "github.com/influxdata/influxdb"
)

func TestWriteService_Write(t *testing.T) {
	type args struct {
		org    platform.ID
		bucket platform.ID
		r      io.Reader
	}
	tests := []struct {
		name    string
		args    args
		status  int
		want    string
		wantErr bool
	}{
		{
			args: args{
				org:    1,
				bucket: 2,
				r:      strings.NewReader("m,t1=v1 f1=2"),
			},
			status: http.StatusNoContent,
			want:   "m,t1=v1 f1=2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var org, bucket *platform.ID
			var lp []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				org, _ = platform.IDFromString(r.URL.Query().Get("org"))
				bucket, _ = platform.IDFromString(r.URL.Query().Get("bucket"))
				defer r.Body.Close()
				in, _ := gzip.NewReader(r.Body)
				defer in.Close()
				lp, _ = ioutil.ReadAll(in)
				w.WriteHeader(tt.status)
			}))
			s := &WriteService{
				Addr: ts.URL,
			}
			if err := s.Write(context.Background(), tt.args.org, tt.args.bucket, tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("WriteService.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got, want := *org, tt.args.org; got != want {
				t.Errorf("WriteService.Write() org = %v, want %v", got, want)
			}

			if got, want := *bucket, tt.args.bucket; got != want {
				t.Errorf("WriteService.Write() bucket = %v, want %v", got, want)
			}

			if got, want := string(lp), tt.want; got != want {
				t.Errorf("WriteService.Write() = %v, want %v", got, want)
			}
		})
	}
}
