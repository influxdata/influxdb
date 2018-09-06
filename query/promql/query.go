package promql

import (
	"fmt"

	"github.com/influxdata/flux"
)

func ParsePromQL(promql string, opts ...Option) (interface{}, error) {
	f, err := Parse("", []byte(promql), opts...)
	if err != nil {
		return "", err
	}
	return f, nil
}

func Build(promql string, opts ...Option) (*flux.Spec, error) {
	parsed, err := ParsePromQL(promql, opts...)
	if err != nil {
		return nil, err
	}
	builder, ok := parsed.(QueryBuilder)
	if !ok {
		return nil, fmt.Errorf("Unable to build as %t is not a QueryBuilder", parsed)
	}
	return builder.QuerySpec()
}
