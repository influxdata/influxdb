package tlv_test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/tlv"
)

func TestReadLV_LengthExceedsMax(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int64(tlv.MaxMessageSize)); err != nil {
		t.Fatal(err)
	}

	_, err := tlv.ReadLV(&buf)
	if err == nil {
		t.Fatal("ReadLV should have rejected message with L = MaxMessageSize")
	}

	if !strings.Contains(err.Error(), "max message size") {
		t.Fatalf("got error %q, expected message about max message size", err.Error())
	}
}

func TestReadLV_LengthNegative(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int64(-1)); err != nil {
		t.Fatal(err)
	}

	_, err := tlv.ReadLV(&buf)
	if err == nil {
		t.Fatal("ReadLV should have rejected message with negative length")
	}

	if !strings.Contains(err.Error(), "negative message size") {
		t.Fatalf("got error %q, expected message about negative message size", err.Error())
	}
}
