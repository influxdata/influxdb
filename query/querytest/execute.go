package querytest

import (
	"math"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/functions"
)

var (
	staticResultID platform.ID
)

func init() {
	staticResultID.DecodeFromString("1")
}

func GetProxyQueryServiceBridge() query.ProxyQueryServiceBridge {
	config := control.Config{
		ConcurrencyQuota: 1,
		MemoryBytesQuota: math.MaxInt64,
	}

	c := control.New(config)

	return query.ProxyQueryServiceBridge{
		QueryService: query.QueryServiceBridge{
			AsyncQueryService: c,
		},
	}
}

func ReplaceFromSpec(q *query.Spec, csvSrc string) {
	for _, op := range q.Operations {
		if op.Spec.Kind() == functions.FromKind {
			op.Spec = &functions.FromCSVOpSpec{
				File: csvSrc,
			}
		}
	}
}
