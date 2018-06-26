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
)

var (
	staticResultID platform.ID
)

func init() {
	staticResultID.DecodeFromString("1")
}

func GetQueryServiceBridge() *query.QueryServiceBridge {
	config := control.Config{
		ConcurrencyQuota: 1,
		MemoryBytesQuota: math.MaxInt64,
	}

	c := control.New(config)

	return &query.QueryServiceBridge{
		AsyncQueryService: c,
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
