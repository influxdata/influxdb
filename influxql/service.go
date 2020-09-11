package influxql

import (
	"context"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2/kit/check"
)

// ProxyQueryService performs InfluxQL queries and encodes the result into a writer.
// The results are opaque to a ProxyQueryService.
type ProxyQueryService interface {
	check.Checker
	Query(ctx context.Context, w io.Writer, req *QueryRequest) (Statistics, error)
}

// ProxyMode enumerates the possible ProxyQueryService operating modes used by a downstream client.
type ProxyMode byte

const (
	// ProxyModeHTTP specifies a ProxyQueryService that forwards InfluxQL requests via HTTP to influxqld.
	ProxyModeHTTP ProxyMode = iota

	// ProxyModeQueue specifies a ProxyQueryService that pushes InfluxQL requests to a queue and influxqld issues a callback request to the initiating service.
	ProxyModeQueue
)

var proxyModeString = [...]string{
	ProxyModeHTTP:  "http",
	ProxyModeQueue: "queue",
}

func (i ProxyMode) String() string {
	if int(i) > len(proxyModeString) {
		return "invalid"
	}
	return proxyModeString[i]
}

func (i *ProxyMode) Set(v string) (err error) {
	switch v {
	case "http":
		*i = ProxyModeHTTP
	case "queue":
		*i = ProxyModeQueue
	default:
		err = fmt.Errorf("unexpected %s type: %s", i.Type(), v)
	}
	return err
}

func (i *ProxyMode) Type() string { return "proxy-mode" }

// RequestMode is enumerates the possible influxqld operating modes for receiving InfluxQL requests.
type RequestMode byte

const (
	// RequestModeHTTP specifies the HTTP listener should be active.
	RequestModeHTTP RequestMode = iota

	// RequestModeQueue specifies the queue dispatcher should be active.
	RequestModeQueue

	// RequestModeAll specifies both the HTTP listener and queue dispatcher should be active.
	RequestModeAll
)

var requestModeString = [...]string{
	RequestModeHTTP:  "http",
	RequestModeQueue: "queue",
	RequestModeAll:   "all",
}

func (i RequestMode) String() string {
	if int(i) > len(requestModeString) {
		return "invalid"
	}
	return proxyModeString[i]
}

func (i *RequestMode) Set(v string) (err error) {
	switch v {
	case "http":
		*i = RequestModeHTTP
	case "queue":
		*i = RequestModeQueue
	case "all":
		*i = RequestModeAll
	default:
		err = fmt.Errorf("unexpected %s type: %s", i.Type(), v)
	}
	return err
}

func (i *RequestMode) Type() string { return "request-mode" }
