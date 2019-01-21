package prometheus

import (
	"bytes"
	"encoding/json"
	"io"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

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
	if len(opts) != 0 {
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
