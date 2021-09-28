package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/protobuf/proto" //lint:ignore SA1019 this deprecated package will be removed by https://github.com/influxdata/influxdb/pull/22571
	"github.com/google/go-cmp/cmp"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func TestPusher_Push(t *testing.T) {
	type check struct {
		Method string
		Body   []byte
	}
	tests := []struct {
		name    string
		gather  prometheus.Gatherer
		timeout time.Duration
		status  int

		want    check
		wantErr bool
	}{
		{
			name: "no metrics no push",
			gather: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				return nil, nil
			}),
		},
		{
			name: "timeout while gathering data returns error",
			gather: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				time.Sleep(time.Hour)
				return nil, nil
			}),
			timeout: time.Millisecond,
			wantErr: true,
		},
		{
			name: "timeout server timeout data returns error",
			gather: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				mf := &dto.MetricFamily{}
				return []*dto.MetricFamily{mf}, nil
			}),
			timeout: time.Millisecond,
			wantErr: true,
		},
		{
			name: "error gathering metrics returns error",
			gather: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				return nil, fmt.Errorf("e1")
			}),
			wantErr: true,
		},
		{
			name: "status code that is not Accepted (202) is an error",
			gather: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				mf := &dto.MetricFamily{}
				return []*dto.MetricFamily{mf}, nil
			}),
			status: http.StatusInternalServerError,
			want: check{
				Method: http.MethodPost,
				Body:   []byte{0x00},
			},
			wantErr: true,
		},
		{
			name: "sending metric are marshalled into delimited protobufs",
			gather: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
				mf := &dto.MetricFamily{
					Name: proto.String("n1"),
					Help: proto.String("h1"),
				}
				return []*dto.MetricFamily{mf}, nil
			}),
			status: http.StatusAccepted,
			want: check{
				Method: http.MethodPost,
				Body: MustMarshal(&dto.MetricFamily{
					Name: proto.String("n1"),
					Help: proto.String("h1"),
				}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			var got check
			srv := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if tt.timeout > 0 { // testing server timeouts
						r = r.WithContext(ctx)
						_ = r
						<-ctx.Done()
						return
					}
					got.Method = r.Method
					got.Body, _ = ioutil.ReadAll(r.Body)
					w.WriteHeader(tt.status)
				}),
			)
			defer srv.Close()

			url := srv.URL
			client := srv.Client()
			p := &Pusher{
				URL:        url,
				Gather:     tt.gather,
				Client:     client,
				PushFormat: expfmt.FmtProtoDelim,
			}
			if err := p.Push(ctx); (err != nil) != tt.wantErr {
				t.Errorf("Pusher.Push() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("%q. Pusher.Push()  = -got/+want %s", tt.name, cmp.Diff(got, tt.want))
				t.Logf("%v\n%v", got.Body, tt.want.Body)
			}
		})
	}
}

func MustMarshal(mf *dto.MetricFamily) []byte {
	buf := &bytes.Buffer{}
	_, err := pbutil.WriteDelimited(buf, mf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}
