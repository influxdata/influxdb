package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/models"
)

func TestWriteService_Write(t *testing.T) {
	type fields struct {
		Token              string
		InsecureSkipVerify bool
	}
	type args struct {
		ctx    context.Context
		org    platform.ID
		bucket platform.ID
		r      io.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		status  int
		want    []models.Point
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.URL.Query().Get(OrgID)
				w.WriteHeader(tt.status)
			}))
			s := &WriteService{
				Addr:               ts.URL,
				Token:              tt.fields.Token,
				InsecureSkipVerify: tt.fields.InsecureSkipVerify,
			}
			if err := s.Write(tt.args.ctx, tt.args.org, tt.args.bucket, tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("WriteService.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
