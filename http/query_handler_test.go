package http

import (
	"bytes"
	"context"
	"fmt"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/platform/query"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFluxService_Query(t *testing.T) {
	tests := []struct {
		name    string
		token   string
		ctx     context.Context
		r       *query.ProxyRequest
		status  int
		want    int64
		wantW   string
		wantErr bool
	}{
		{
			name:  "query",
			ctx:   context.Background(),
			token: "mytoken",
			r: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			status: http.StatusOK,
			want:   6,
			wantW:  "howdy\n",
		},
		{
			name:  "error status",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			status:  http.StatusUnauthorized,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				fmt.Fprintln(w, "howdy")
			}))
			defer ts.Close()
			s := &FluxService{
				URL:   ts.URL,
				Token: tt.token,
			}

			w := &bytes.Buffer{}
			got, err := s.Query(tt.ctx, w, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("FluxService.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FluxService.Query() = %v, want %v", got, tt.want)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("FluxService.Query() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}
