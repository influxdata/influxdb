package prometheus_test

import (
	"bytes"
	"testing"

	pr "github.com/influxdata/influxdb/v2/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func Test_CodecExpfmt(t *testing.T) {
	mf1 := []*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1"))}
	mf2 := []*dto.MetricFamily{NewCounter("mf2", 1.0, pr.L("n2", "v2"))}

	b1, err := pr.EncodeExpfmt(mf1)
	if err != nil {
		t.Fatalf("encodeExpfmt() error = %v", err)
	}

	got1, err := pr.DecodeExpfmt(bytes.NewBuffer(b1), expfmt.FmtProtoDelim)
	if err != nil {
		t.Fatalf("decodeExpfmt() error = %v", err)
	}

	for i := range got1 {
		if got1[i].String() != mf1[i].String() {
			t.Errorf("codec1() = %v, want %v", got1[i].String(), mf1[i].String())
		}
	}

	b2, err := pr.EncodeExpfmt(mf2)
	if err != nil {
		t.Fatalf("encodeExpfmt() error = %v", err)
	}

	got2, err := pr.DecodeExpfmt(bytes.NewBuffer(b2), expfmt.FmtProtoDelim)
	if err != nil {
		t.Fatalf("decodeExpfmt() error = %v", err)
	}

	for i := range got2 {
		if got2[i].String() != mf2[i].String() {
			t.Errorf("codec2() = %v, want %v", got2[i].String(), mf2[i].String())
		}
	}

	b3 := append(b2, b1...)
	b3 = append(b3, b2...)

	mf3 := []*dto.MetricFamily{
		NewCounter("mf2", 1.0, pr.L("n2", "v2")),
		NewCounter("mf1", 1.0, pr.L("n1", "v1")),
		NewCounter("mf2", 1.0, pr.L("n2", "v2")),
	}

	got3, err := pr.DecodeExpfmt(bytes.NewBuffer(b3), expfmt.FmtProtoDelim)
	if err != nil {
		t.Fatalf("decodeExpfmt() error = %v", err)
	}

	for i := range got3 {
		if got3[i].String() != mf3[i].String() {
			t.Errorf("codec3() = %v, want %v", got3[i].String(), mf3[i].String())
		}
	}
}

func Test_CodecJSON(t *testing.T) {
	mf1 := []*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1")), NewCounter("mf1", 1.0, pr.L("n1", "v1"))}
	mf2 := []*dto.MetricFamily{NewCounter("mf2", 1.0, pr.L("n2", "v2"))}

	b1, err := pr.EncodeJSON(mf1)
	if err != nil {
		t.Fatalf("encodeJSON() error = %v", err)
	}

	got1, err := pr.DecodeJSON(bytes.NewBuffer(b1))
	if err != nil {
		t.Fatalf("decodeJSON() error = %v", err)
	}

	for i := range got1 {
		if got1[i].String() != mf1[i].String() {
			t.Errorf("codec1() = %v, want %v", got1[i].String(), mf1[i].String())
		}
	}

	b2, err := pr.EncodeJSON(mf2)
	if err != nil {
		t.Fatalf("encodeJSON() error = %v", err)
	}

	got2, err := pr.DecodeJSON(bytes.NewBuffer(b2))
	if err != nil {
		t.Fatalf("decodeJSON() error = %v", err)
	}

	for i := range got2 {
		if got2[i].String() != mf2[i].String() {
			t.Errorf("codec2() = %v, want %v", got2[i].String(), mf2[i].String())
		}
	}

	b3 := append(b2, b1...)
	b3 = append(b3, b2...)

	mf3 := []*dto.MetricFamily{
		NewCounter("mf2", 1.0, pr.L("n2", "v2")),
		NewCounter("mf1", 1.0, pr.L("n1", "v1")),
		NewCounter("mf1", 1.0, pr.L("n1", "v1")),
		NewCounter("mf2", 1.0, pr.L("n2", "v2")),
	}

	got3, err := pr.DecodeJSON(bytes.NewBuffer(b3))
	if err != nil {
		t.Fatalf("decodeJSON() error = %v", err)
	}

	for i := range got3 {
		if got3[i].String() != mf3[i].String() {
			t.Errorf("codec3() = %v, want %v", got3[i].String(), mf3[i].String())
		}
	}
}
