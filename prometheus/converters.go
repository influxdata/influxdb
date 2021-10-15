package prometheus

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	// measurementName is the default name used if no Prometheus name can be found on write
	measurementName = "prom_metric_not_specified"

	// fieldName is the field all prometheus values get written to
	fieldName = "value"

	// fieldTagKey is the tag key that all field names use in the new storage processor
	fieldTagKey = "_field"

	// prometheusNameTag is the tag key that Prometheus uses for metric names
	prometheusNameTag = "__name__"

	// measurementTagKey is the tag key that all measurement names use in the new storage processor
	measurementTagKey = "_measurement"
)

// A DroppedValuesError is returned when the prometheus write request contains
// unsupported float64 values.
type DroppedValuesError struct {
	nan  uint64
	ninf uint64
	inf  uint64
}

// Error returns a descriptive error of the values dropped.
func (e DroppedValuesError) Error() string {
	return fmt.Sprintf("dropped unsupported Prometheus values: [NaN = %d, +Inf = %d, -Inf = %d]", e.nan, e.inf, e.ninf)
}

// WriteRequestToPoints converts a Prometheus remote write request of time series and their
// samples into Points that can be written into Influx
func WriteRequestToPoints(req *prompb.WriteRequest) ([]models.Point, error) {
	var maxPoints int
	for _, ts := range req.Timeseries {
		maxPoints += len(ts.Samples)
	}
	points := make([]models.Point, 0, maxPoints)

	// Track any dropped values.
	var nan, inf, ninf uint64

	for _, ts := range req.Timeseries {
		measurement := measurementName

		tags := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			tags[l.Name] = l.Value
			if l.Name == prometheusNameTag {
				measurement = l.Value
			}
		}

		for _, s := range ts.Samples {
			if v := s.Value; math.IsNaN(v) {
				nan++
				continue
			} else if math.IsInf(v, -1) {
				ninf++
				continue
			} else if math.IsInf(v, 1) {
				inf++
				continue
			}

			// convert and append
			t := time.Unix(0, s.Timestamp*int64(time.Millisecond))
			fields := map[string]interface{}{fieldName: s.Value}
			p, err := models.NewPoint(measurement, models.NewTags(tags), fields, t)
			if err != nil {
				return nil, err
			}
			points = append(points, p)
		}
	}

	if nan+inf+ninf > 0 {
		return points, DroppedValuesError{nan: nan, inf: inf, ninf: ninf}
	}
	return points, nil
}

// ReadRequestToInfluxStorageRequest converts a Prometheus remote read request into one using the
// new storage API that IFQL uses.
func ReadRequestToInfluxStorageRequest(req *prompb.ReadRequest, db, rp string) (*datatypes.ReadFilterRequest, error) {
	if len(req.Queries) != 1 {
		return nil, errors.New("Prometheus read endpoint currently only supports one query at a time")
	}
	q := req.Queries[0]

	src, err := anypb.New(&storage.ReadSource{Database: db, RetentionPolicy: rp})
	if err != nil {
		return nil, err
	}

	sreq := &datatypes.ReadFilterRequest{
		ReadSource: src,
		Range: &datatypes.TimestampRange{
			Start: time.Unix(0, q.StartTimestampMs*int64(time.Millisecond)).UnixNano(),
			End:   time.Unix(0, q.EndTimestampMs*int64(time.Millisecond)).UnixNano(),
		},
	}

	pred, err := predicateFromMatchers(q.Matchers)
	if err != nil {
		return nil, err
	}

	sreq.Predicate = pred
	return sreq, nil
}

// RemoveInfluxSystemTags will remove tags that are Influx internal (_measurement and _field)
func RemoveInfluxSystemTags(tags models.Tags) models.Tags {
	var t models.Tags
	for _, tt := range tags {
		if string(tt.Key) == measurementTagKey || string(tt.Key) == fieldTagKey {
			continue
		}
		t = append(t, tt)
	}

	return t
}

// predicateFromMatchers takes Prometheus label matchers and converts them to a storage
// predicate that works with the schema that is written in, which assumes a single field
// named value
func predicateFromMatchers(matchers []*prompb.LabelMatcher) (*datatypes.Predicate, error) {
	left, err := nodeFromMatchers(matchers)
	if err != nil {
		return nil, err
	}
	right := fieldNode()

	return &datatypes.Predicate{
		Root: &datatypes.Node{
			NodeType: datatypes.Node_TypeLogicalExpression,
			Value:    &datatypes.Node_Logical_{Logical: datatypes.Node_LogicalAnd},
			Children: []*datatypes.Node{left, right},
		},
	}, nil
}

// fieldNode returns a datatypes.Node that will match that the fieldTagKey == fieldName
// which matches how Prometheus data is fed into the system
func fieldNode() *datatypes.Node {
	children := []*datatypes.Node{
		&datatypes.Node{
			NodeType: datatypes.Node_TypeTagRef,
			Value: &datatypes.Node_TagRefValue{
				TagRefValue: []byte(fieldTagKey),
			},
		},
		&datatypes.Node{
			NodeType: datatypes.Node_TypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: fieldName,
			},
		},
	}

	return &datatypes.Node{
		NodeType: datatypes.Node_TypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: datatypes.Node_ComparisonEqual},
		Children: children,
	}
}

func nodeFromMatchers(matchers []*prompb.LabelMatcher) (*datatypes.Node, error) {
	if len(matchers) == 0 {
		return nil, errors.New("expected matcher")
	} else if len(matchers) == 1 {
		return nodeFromMatcher(matchers[0])
	}

	left, err := nodeFromMatcher(matchers[0])
	if err != nil {
		return nil, err
	}

	right, err := nodeFromMatchers(matchers[1:])
	if err != nil {
		return nil, err
	}

	children := []*datatypes.Node{left, right}
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.Node_LogicalAnd},
		Children: children,
	}, nil
}

func nodeFromMatcher(m *prompb.LabelMatcher) (*datatypes.Node, error) {
	var op datatypes.Node_Comparison
	switch m.Type {
	case prompb.LabelMatcher_EQ:
		op = datatypes.Node_ComparisonEqual
	case prompb.LabelMatcher_NEQ:
		op = datatypes.Node_ComparisonNotEqual
	case prompb.LabelMatcher_RE:
		op = datatypes.Node_ComparisonRegex
	case prompb.LabelMatcher_NRE:
		op = datatypes.Node_ComparisonNotRegex
	default:
		return nil, fmt.Errorf("unknown match type %v", m.Type)
	}

	name := m.Name
	if m.Name == prometheusNameTag {
		name = measurementTagKey
	}

	left := &datatypes.Node{
		NodeType: datatypes.Node_TypeTagRef,
		Value: &datatypes.Node_TagRefValue{
			TagRefValue: []byte(name),
		},
	}

	var right *datatypes.Node

	if op == datatypes.Node_ComparisonRegex || op == datatypes.Node_ComparisonNotRegex {
		right = &datatypes.Node{
			NodeType: datatypes.Node_TypeLiteral,
			Value: &datatypes.Node_RegexValue{
				// To comply with PromQL, see
				// https://github.com/prometheus/prometheus/blob/daf382e4a9f5ca380b2b662c8e60755a56675f14/pkg/labels/regexp.go#L30
				RegexValue: "^(?:" + m.Value + ")$",
			},
		}
	} else {
		right = &datatypes.Node{
			NodeType: datatypes.Node_TypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: m.Value,
			},
		}
	}

	children := []*datatypes.Node{left, right}
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: op},
		Children: children,
	}, nil
}

// ModelTagsToLabelPairs converts models.Tags to a slice of Prometheus label pairs
func ModelTagsToLabelPairs(tags models.Tags) []prompb.Label {
	pairs := make([]prompb.Label, 0, len(tags))
	for _, t := range tags {
		if string(t.Value) == "" {
			continue
		}
		pairs = append(pairs, prompb.Label{
			Name:  string(t.Key),
			Value: string(t.Value),
		})
	}
	return pairs
}

// TagsToLabelPairs converts a map of Influx tags into a slice of Prometheus label pairs
func TagsToLabelPairs(tags map[string]string) []*prompb.Label {
	pairs := make([]*prompb.Label, 0, len(tags))
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
		pairs = append(pairs, &prompb.Label{
			Name:  k,
			Value: v,
		})
	}
	return pairs
}

// PrometheusToStatistics converts a prometheus metric family (from Registry.Gather)
// to a []model.Statistics for /debug/vars .
// This code is strongly inspired by the telegraf prometheus plugin.
func PrometheusToStatistics(family []*dto.MetricFamily, tags map[string]string) []models.Statistic {
	statistics := []models.Statistic{}
	for _, mf := range family {
		for _, m := range mf.Metric {
			newTags := make(map[string]string, len(tags)+len(m.Label))
			for key, value := range tags {
				newTags[key] = value
			}
			for _, lp := range m.Label {
				newTags[lp.GetName()] = lp.GetValue()
			}

			// reading fields
			var fields map[string]interface{}
			if mf.GetType() == dto.MetricType_SUMMARY {
				// summary metric
				fields = makeQuantiles(m)
				fields["count"] = float64(m.GetSummary().GetSampleCount())
				fields["sum"] = float64(m.GetSummary().GetSampleSum())
			} else if mf.GetType() == dto.MetricType_HISTOGRAM {
				// histogram metric
				fields = makeBuckets(m)
				fields["count"] = float64(m.GetHistogram().GetSampleCount())
				fields["sum"] = float64(m.GetHistogram().GetSampleSum())
			} else {
				// standard metric
				fields = getNameAndValue(m)
			}

			statistics = append(statistics, models.Statistic{
				Name:   *mf.Name,
				Tags:   tags,
				Values: fields,
			})
		}
	}
	return statistics
}

// Get Quantiles from summary metric
func makeQuantiles(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, q := range m.GetSummary().Quantile {
		if !math.IsNaN(q.GetValue()) {
			fields[fmt.Sprint(q.GetQuantile())] = float64(q.GetValue())
		}
	}
	return fields
}

// Get Buckets  from histogram metric
func makeBuckets(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, b := range m.GetHistogram().Bucket {
		fields[fmt.Sprint(b.GetUpperBound())] = float64(b.GetCumulativeCount())
	}
	return fields
}

// Get name and value from metric
func getNameAndValue(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	if m.Gauge != nil {
		if !math.IsNaN(m.GetGauge().GetValue()) {
			fields["gauge"] = float64(m.GetGauge().GetValue())
		}
	} else if m.Counter != nil {
		if !math.IsNaN(m.GetCounter().GetValue()) {
			fields["counter"] = float64(m.GetCounter().GetValue())
		}
	} else if m.Untyped != nil {
		if !math.IsNaN(m.GetUntyped().GetValue()) {
			fields["value"] = float64(m.GetUntyped().GetValue())
		}
	}
	return fields
}
