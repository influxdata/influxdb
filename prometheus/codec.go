package prometheus

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// Encoder transforms metric families into bytes.
type Encoder interface {
	// Encode encodes metrics into bytes.
	Encode(mfs []*dto.MetricFamily) ([]byte, error)
}

// Expfmt encodes metric families into prometheus exposition format.
type Expfmt struct {
	Format expfmt.Format
}

// Encode encodes metrics into prometheus exposition format bytes.
func (e *Expfmt) Encode(mfs []*dto.MetricFamily) ([]byte, error) {
	return EncodeExpfmt(mfs, e.Format)
}

// DecodeExpfmt decodes the reader of format into metric families.
func DecodeExpfmt(r io.Reader, format expfmt.Format) ([]*dto.MetricFamily, error) {
	dec := expfmt.NewDecoder(r, format)
	mfs := []*dto.MetricFamily{}
	for {
		var mf dto.MetricFamily
		if err := dec.Decode(&mf); err != nil {
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}
		mfs = append(mfs, &mf)
	}
	return mfs, nil
}

// EncodeExpfmt encodes the metrics family (defaults to expfmt.FmtProtoDelim).
func EncodeExpfmt(mfs []*dto.MetricFamily, opts ...expfmt.Format) ([]byte, error) {
	format := expfmt.FmtProtoDelim
	if len(opts) != 0 && opts[0] != "" {
		format = opts[0]
	}
	buf := &bytes.Buffer{}
	enc := expfmt.NewEncoder(buf, format)
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// JSON encodes metric families into JSON.
type JSON struct{}

// Encode encodes metrics JSON bytes. This not always works
// as some prometheus values are NaN or Inf.
func (j *JSON) Encode(mfs []*dto.MetricFamily) ([]byte, error) {
	return EncodeJSON(mfs)
}

// DecodeJSON decodes a JSON array of metrics families.
func DecodeJSON(r io.Reader) ([]*dto.MetricFamily, error) {
	dec := json.NewDecoder(r)
	families := []*dto.MetricFamily{}
	for {
		mfs := []*dto.MetricFamily{}

		if err := dec.Decode(&mfs); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		families = append(families, mfs...)
	}
	return families, nil
}

// EncodeJSON encodes the metric families to JSON.
func EncodeJSON(mfs []*dto.MetricFamily) ([]byte, error) {
	return json.Marshal(mfs)
}

const (
	// just in case the definition of time.Nanosecond changes from 1.
	nsPerMilliseconds = int64(time.Millisecond / time.Nanosecond)
)

// LineProtocol encodes metric families into influxdb line protocol.
type LineProtocol struct{}

// Encode encodes metrics into line protocol format bytes.
func (l *LineProtocol) Encode(mfs []*dto.MetricFamily) ([]byte, error) {
	return EncodeLineProtocol(mfs)
}

// EncodeLineProtocol converts prometheus metrics into line protocol.
func EncodeLineProtocol(mfs []*dto.MetricFamily) ([]byte, error) {
	var b bytes.Buffer

	pts := points(mfs)
	for _, p := range pts {
		if _, err := b.WriteString(p.String()); err != nil {
			return nil, err
		}
		if err := b.WriteByte('\n'); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func points(mfs []*dto.MetricFamily) models.Points {
	pts := make(models.Points, 0, len(mfs))
	for _, mf := range mfs {
		mts := make(models.Points, 0, len(mf.Metric))
		name := mf.GetName()
		for _, m := range mf.Metric {
			ts := tags(m.Label)
			fs := fields(mf.GetType(), m)
			tm := timestamp(m)

			pt, err := models.NewPoint(name, ts, fs, tm)
			if err != nil {
				continue
			}
			mts = append(mts, pt)
		}
		pts = append(pts, mts...)
	}

	return pts
}

func timestamp(m *dto.Metric) time.Time {
	var tm time.Time
	if m.GetTimestampMs() > 0 {
		tm = time.Unix(0, m.GetTimestampMs()*nsPerMilliseconds)
	}
	return tm

}

func tags(labels []*dto.LabelPair) models.Tags {
	ts := make(models.Tags, len(labels))
	for i, label := range labels {
		ts[i] = models.NewTag([]byte(label.GetName()), []byte(label.GetValue()))
	}
	return ts
}

func fields(typ dto.MetricType, m *dto.Metric) models.Fields {
	switch typ {
	case dto.MetricType_SUMMARY:
		return summary(m.GetSummary())
	case dto.MetricType_HISTOGRAM:
		return histogram(m.GetHistogram())
	case dto.MetricType_GAUGE:
		return value("gauge", m.GetGauge())
	case dto.MetricType_COUNTER:
		return value("counter", m.GetCounter())
	case dto.MetricType_UNTYPED:
		return value("value", m.GetUntyped())
	default:
		return nil
	}
}

func summary(s *dto.Summary) map[string]interface{} {
	fields := make(map[string]interface{}, len(s.Quantile)+2)
	for _, q := range s.Quantile {
		v := q.GetValue()
		if !math.IsNaN(v) {
			key := strconv.FormatFloat(q.GetQuantile(), 'f', -1, 64)
			fields[key] = v
		}
	}

	fields["count"] = float64(s.GetSampleCount())
	fields["sum"] = float64(s.GetSampleSum())
	return fields
}

func histogram(hist *dto.Histogram) map[string]interface{} {
	fields := make(map[string]interface{}, len(hist.Bucket)+2)
	for _, b := range hist.Bucket {
		k := strconv.FormatFloat(b.GetUpperBound(), 'f', -1, 64)
		fields[k] = float64(b.GetCumulativeCount())
	}

	fields["count"] = float64(hist.GetSampleCount())
	fields["sum"] = float64(hist.GetSampleSum())

	return fields
}

type valuer interface {
	GetValue() float64
}

func value(typ string, m valuer) models.Fields {
	vs := make(models.Fields, 1)

	v := m.GetValue()
	if !math.IsNaN(v) {
		vs[typ] = v
	}

	return vs
}
