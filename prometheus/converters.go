package prometheus

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/influxdata/influxdb/services/storage"
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
		measurement := measurementName

		tags := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			tags[l.Name] = l.Value
			if l.Name == prometheusNameTag {
				measurement = l.Value
			}
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
			p, err := models.NewPoint(measurement, models.NewTags(tags), fields, t)
			if err != nil {
				return nil, err
			}

			points = append(points, p)
		}
	}
	return points, droppedNaN
}

// ReadRequestToInfluxStorageRequest converts a Prometheus remote read request into one using the
// new storage API that IFQL uses.
func ReadRequestToInfluxStorageRequest(req *remote.ReadRequest, db, rp string) (*storage.ReadRequest, error) {
	if len(req.Queries) != 1 {
		return nil, errors.New("Prometheus read endpoint currently only supports one query at a time")
	}
	q := req.Queries[0]

	if rp != "" {
		db = db + "/" + rp
	}

	sreq := &storage.ReadRequest{
		Database: db,
		TimestampRange: storage.TimestampRange{
			Start: time.Unix(0, q.StartTimestampMs*int64(time.Millisecond)).UnixNano(),
			End:   time.Unix(0, q.EndTimestampMs*int64(time.Millisecond)).UnixNano(),
		},
		PointsLimit: math.MaxInt64,
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
func predicateFromMatchers(matchers []*remote.LabelMatcher) (*storage.Predicate, error) {
	left, err := nodeFromMatchers(matchers)
	if err != nil {
		return nil, err
	}
	right := fieldNode()

	return &storage.Predicate{
		Root: &storage.Node{
			NodeType: storage.NodeTypeLogicalExpression,
			Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
			Children: []*storage.Node{left, right},
		},
	}, nil
}

// fieldNode returns a storage.Node that will match that the fieldTagKey == fieldName
// which matches how Prometheus data is fed into the system
func fieldNode() *storage.Node {
	children := []*storage.Node{
		&storage.Node{
			NodeType: storage.NodeTypeTagRef,
			Value: &storage.Node_TagRefValue{
				TagRefValue: fieldTagKey,
			},
		},
		&storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value: &storage.Node_StringValue{
				StringValue: fieldName,
			},
		},
	}

	return &storage.Node{
		NodeType: storage.NodeTypeComparisonExpression,
		Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
		Children: children,
	}
}

func nodeFromMatchers(matchers []*remote.LabelMatcher) (*storage.Node, error) {
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

	children := []*storage.Node{left, right}
	return &storage.Node{
		NodeType: storage.NodeTypeLogicalExpression,
		Value:    &storage.Node_Logical_{Logical: storage.LogicalAnd},
		Children: children,
	}, nil
}

func nodeFromMatcher(m *remote.LabelMatcher) (*storage.Node, error) {
	var op storage.Node_Comparison
	switch m.Type {
	case remote.MatchType_EQUAL:
		op = storage.ComparisonEqual
	case remote.MatchType_NOT_EQUAL:
		op = storage.ComparisonNotEqual
	case remote.MatchType_REGEX_MATCH:
		op = storage.ComparisonRegex
	case remote.MatchType_REGEX_NO_MATCH:
		op = storage.ComparisonNotRegex
	default:
		return nil, fmt.Errorf("unknown match type %v", m.Type)
	}

	name := m.Name
	if m.Name == prometheusNameTag {
		name = measurementTagKey
	}

	left := &storage.Node{
		NodeType: storage.NodeTypeTagRef,
		Value: &storage.Node_TagRefValue{
			TagRefValue: name,
		},
	}

	var right *storage.Node

	if op == storage.ComparisonRegex || op == storage.ComparisonNotRegex {
		right = &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value: &storage.Node_RegexValue{
				RegexValue: m.Value,
			},
		}
	} else {
		right = &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value: &storage.Node_StringValue{
				StringValue: m.Value,
			},
		}
	}

	children := []*storage.Node{left, right}
	return &storage.Node{
		NodeType: storage.NodeTypeComparisonExpression,
		Value:    &storage.Node_Comparison_{Comparison: op},
		Children: children,
	}, nil
}

// ModelTagsToLabelPairs converts models.Tags to a slice of Prometheus label pairs
func ModelTagsToLabelPairs(tags models.Tags) []*remote.LabelPair {
	pairs := make([]*remote.LabelPair, 0, len(tags))
	for _, t := range tags {
		if string(t.Value) == "" {
			continue
		}
		pairs = append(pairs, &remote.LabelPair{
			Name:  string(t.Key),
			Value: string(t.Value),
		})
	}
	return pairs
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
