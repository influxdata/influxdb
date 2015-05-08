package meta_test

import (
	"reflect"
	"testing"

	"github.com/influxdb/influxdb/meta"
)

// Ensure the node info can be encoded to and from a binary format.
func TestNodeInfo_Marshal(t *testing.T) {
	// Encode object.
	n := meta.NodeInfo{ID: 100, Host: "server0"}
	buf, err := n.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Decode object.
	var other meta.NodeInfo
	if err := other.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(n, other) {
		t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", n, other)
	}
}
