package httpd

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"mime"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/flux/client"
	"github.com/prometheus/client_golang/prometheus"
)

type Controller interface {
	Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
	PrometheusCollectors() []prometheus.Collector
}

// httpDialect is an encoding dialect that can write metadata to HTTP headers
type httpDialect interface {
	SetHeaders(w http.ResponseWriter)
}

func decodeQueryRequest(r *http.Request) (*client.QueryRequest, error) {
	ct := r.Header.Get("Content-Type")
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return nil, err
	}

	var req client.QueryRequest
	switch mt {
	case "application/vnd.flux":
		if d, err := ioutil.ReadAll(r.Body); err != nil {
			return nil, err
		} else {
			req.Query = string(d)
		}
	default:
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
	}

	req = req.WithDefaults()
	err = req.Validate()
	if err != nil {
		return nil, err
	}

	return &req, err
}
