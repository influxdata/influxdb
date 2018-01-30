package prometheus

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/influxdata/influxql"
)

const (
	// measurementName is where all prometheus time series go to
	measurementName = "_"

	// fieldName is the field all prometheus values get written to
	fieldName = "f64"
)

var ErrNaNDropped = errors.New("dropped NaN from Prometheus since they are not supported")

// WriteRequestToPoints converts a Prometheus remote write request of time series and their
// samples into Points that can be written into Influx
func WriteRequestToPoints(req *remote.WriteRequest) ([]models.Point, error) {
	var maxPoints int
	for _, ts := range req.Timeseries {
		maxPoints += len(ts.Samples)
	}
	points := make([]models.Point, 0, maxPoints)

	var droppedNaN error

	for _, ts := range req.Timeseries {
		tags := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			tags[l.Name] = l.Value
		}

		for _, s := range ts.Samples {
			// skip NaN values, which are valid in Prometheus
			if math.IsNaN(s.Value) {
				droppedNaN = ErrNaNDropped
				continue
			}

			// convert and append
			t := time.Unix(0, s.TimestampMs*int64(time.Millisecond))
			fields := map[string]interface{}{fieldName: s.Value}
			p, err := models.NewPoint(measurementName, models.NewTags(tags), fields, t)
			if err != nil {
				return nil, err
			}

			points = append(points, p)
		}
	}
	return points, droppedNaN
}

// ReadRequestToInfluxQLQuery converts a Prometheus remote read request to an equivalent InfluxQL
// query that will return the requested data when executed
func ReadRequestToInfluxQLQuery(req *remote.ReadRequest, db, rp string) (*influxql.Query, error) {
	if len(req.Queries) != 1 {
		return nil, errors.New("Prometheus read endpoint currently only supports one query at a time")
	}
	promQuery := req.Queries[0]

	stmt := &influxql.SelectStatement{
		IsRawQuery: true,
		Fields: []*influxql.Field{
			{Expr: &influxql.VarRef{Val: fieldName}},
		},
		Sources: []influxql.Source{&influxql.Measurement{
			Name:            measurementName,
			Database:        db,
			RetentionPolicy: rp,
		}},
		Dimensions: []*influxql.Dimension{{Expr: &influxql.Wildcard{}}},
	}

	cond, err := condFromMatchers(promQuery, promQuery.Matchers)
	if err != nil {
		return nil, err
	}

	stmt.Condition = cond

	return &influxql.Query{Statements: []influxql.Statement{stmt}}, nil
}

// condFromMatcher converts a Prometheus LabelMatcher into an equivalent InfluxQL BinaryExpr
func condFromMatcher(m *remote.LabelMatcher) (*influxql.BinaryExpr, error) {
	var op influxql.Token
	var rhs influxql.Expr

	switch m.Type {
	case remote.MatchType_EQUAL:
		op = influxql.EQ
	case remote.MatchType_NOT_EQUAL:
		op = influxql.NEQ
	case remote.MatchType_REGEX_MATCH:
		op = influxql.EQREGEX
	case remote.MatchType_REGEX_NO_MATCH:
		op = influxql.NEQREGEX
	default:
		return nil, fmt.Errorf("unknown match type %v", m.Type)
	}

	if op == influxql.EQREGEX || op == influxql.NEQREGEX {
		re, err := regexp.Compile(m.Value)
		if err != nil {
			return nil, err
		}

		// Convert regex values to InfluxDB format.
		rhs = &influxql.RegexLiteral{Val: re}
	} else {
		rhs = &influxql.StringLiteral{Val: m.Value}
	}

	return &influxql.BinaryExpr{
		Op:  op,
		LHS: &influxql.VarRef{Val: m.Name},
		RHS: rhs,
	}, nil
}

// condFromMatchers converts a Prometheus remote query and a collection of Prometheus label matchers
// into an equivalent influxql.BinaryExpr. This assume a schema that is written via the Prometheus
// remote write endpoint, which uses a measurement name of _ and a field name of f64. Tags and labels
// are kept equivalent.
func condFromMatchers(q *remote.Query, matchers []*remote.LabelMatcher) (*influxql.BinaryExpr, error) {
	if len(matchers) > 0 {
		lhs, err := condFromMatcher(matchers[0])
		if err != nil {
			return nil, err
		}
		rhs, err := condFromMatchers(q, matchers[1:])
		if err != nil {
			return nil, err
		}

		return &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: lhs,
			RHS: rhs,
		}, nil
	}

	return &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.GTE,
			LHS: &influxql.VarRef{Val: "time"},
			RHS: &influxql.TimeLiteral{Val: time.Unix(0, q.StartTimestampMs*int64(time.Millisecond))},
		},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.LTE,
			LHS: &influxql.VarRef{Val: "time"},
			RHS: &influxql.TimeLiteral{Val: time.Unix(0, q.EndTimestampMs*int64(time.Millisecond))},
		},
	}, nil
}

// TagsToLabelPairs converts a map of Influx tags into a slice of Prometheus label pairs
func TagsToLabelPairs(tags map[string]string) []*remote.LabelPair {
	pairs := make([]*remote.LabelPair, 0, len(tags))
	for k, v := range tags {
		if v == "" {
			// If we select metrics with different sets of labels names,
			// InfluxDB returns *all* possible tag names on all returned
			// series, with empty tag values on series where they don't
			// apply. In Prometheus, an empty label value is equivalent
			// to a non-existent label, so we just skip empty ones here
			// to make the result correct.
			continue
		}
		pairs = append(pairs, &remote.LabelPair{
			Name:  k,
			Value: v,
		})
	}
	return pairs
}
