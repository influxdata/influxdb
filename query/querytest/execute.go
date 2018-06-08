package querytest

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/id"
)

var (
	staticResultID platform.ID
)

func init() {
	staticResultID.DecodeFromString("1")
}

// wrapController is needed to make *control.Controller implement platform.AsyncQueryService.
// TODO(nathanielc/adam): copied from queryd main.go, in which there's a note to remove this type by a better design
type wrapController struct {
	*control.Controller
}

func (c wrapController) Query(ctx context.Context, orgID platform.ID, query *query.Spec) (query.Query, error) {
	q, err := c.Controller.Query(ctx, id.ID(orgID), query)
	return q, err
}

func (c wrapController) QueryWithCompile(ctx context.Context, orgID platform.ID, query string) (query.Query, error) {
	q, err := c.Controller.QueryWithCompile(ctx, id.ID(orgID), query)
	return q, err
}

func GetQueryServiceBridge() *query.QueryServiceBridge {
	config := control.Config{
		ConcurrencyQuota: 1,
		MemoryBytesQuota: math.MaxInt64,
	}

	c := control.New(config)

	return &query.QueryServiceBridge{
		AsyncQueryService: wrapController{Controller: c},
	}
}

func GetQueryEncodedResults(qs query.QueryService, spec *query.Spec, inputFile string, enc query.MultiResultEncoder) (string, error) {
	results, err := qs.Query(context.Background(), staticResultID, spec)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	if err := enc.Encode(buf, results); err != nil {
		return "", err
	}
	return buf.String(), results.Err()
}

func GetTestData(prefix, suffix string) (string, error) {
	datafile := prefix + suffix
	if _, err := os.Stat(datafile); err != nil {
		return "", err
	}
	csv, err := ioutil.ReadFile(datafile)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %s", datafile)
	}
	return string(csv), nil
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
