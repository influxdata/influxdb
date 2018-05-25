package querytest

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/id"
)

var (
	staticResultID platform.ID
)

func init() {
	staticResultID.DecodeFromString("1")
}

// wrapController is needed to make *ifql.Controller implement platform.AsyncQueryService.
// TODO(nathanielc/adam): copied from ifqlde main.go, in which there's a note to remove this type by a better design
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

func GetQueryEncodedResults(qs *query.QueryServiceBridge, spec *query.Spec, inputFile string) (string, error) {
	results, err := qs.Query(context.Background(), staticResultID, spec)
	if err != nil {
		return "", err
	}
	enc := csv.NewResultEncoder(csv.DefaultEncoderConfig())
	buf := new(bytes.Buffer)
	// we are only expecting one result, for now
	for results.More() {
		res := results.Next()

		err := enc.Encode(buf, res)
		if err != nil {
			results.Cancel()
			return "", err
		}

	}
	return buf.String(), nil
}

func GetTestData(prefix, suffix string) (string, error) {
	datafile := prefix + suffix
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
