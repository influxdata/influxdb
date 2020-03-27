package client

import (
	"testing"

	"github.com/influxdata/flux/csv"
	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestRequest_ProxyRequest_Annotations(t *testing.T) {

	for _, tt := range []struct {
		qr          *QueryRequest
		annotations []string
	}{
		{
			qr:          &QueryRequest{Query: "from"},
			annotations: []string{"datatype", "group", "default"},
		},
		{
			qr:          &QueryRequest{Query: "from", Dialect: QueryDialect{Annotations: []string{"datatype", "group", "default"}}},
			annotations: []string{"datatype", "group", "default"},
		},
		{
			qr:          &QueryRequest{Query: "from", Dialect: QueryDialect{Annotations: []string{"datatype", "group"}}},
			annotations: []string{"datatype", "group"},
		},
		{
			qr:          &QueryRequest{Query: "from", Dialect: QueryDialect{Annotations: nil}},
			annotations: []string{"datatype", "group", "default"},
		},
		{
			qr:          &QueryRequest{Query: "from", Dialect: QueryDialect{Annotations: []string{}}},
			annotations: []string{},
		},
	} {
		dialect := tt.qr.ProxyRequest().Dialect.(csv.Dialect)
		assert.Equal(t, dialect.ResultEncoderConfig.Annotations, tt.annotations, "invalid annotations")
	}
}
