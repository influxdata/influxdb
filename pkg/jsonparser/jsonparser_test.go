package jsonparser_test

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/pkg/jsonparser"
)

func TestGetID(t *testing.T) {
	t.Run("decode valid id", func(t *testing.T) {
		json := `{ "id": "000000000000000a" }`
		got, err := jsonparser.GetID([]byte(json), "id")
		if err != nil {
			t.Error("unexpected error:", err)
		}

		if exp := platform.ID(10); got != exp {
			t.Error("unexpected value: -got/+exp", cmp.Diff(got, exp))
		}
	})

	t.Run("error invalid id", func(t *testing.T) {
		json := `{ "id": "00000000000a" }`
		_, err := jsonparser.GetID([]byte(json), "id")
		if err == nil {
			t.Error("expected error")
		}
	})
}

func TestGetOptionalID(t *testing.T) {
	t.Run("missing id", func(t *testing.T) {
		json := `{ "name": "foo" }`
		_, got, err := jsonparser.GetOptionalID([]byte(json), "id")
		if err != nil {
			t.Error("unexpected error:", err)
		}

		if exp := false; got != exp {
			t.Error("unexpected value: -got/+exp", cmp.Diff(got, exp))
		}
	})
}
