package gen

import (
	"fmt"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
)

func visit(root *Schema) string {
	w := &strings.Builder{}

	walkFn := func(node SchemaNode) bool {
		switch n := node.(type) {
		case *Schema:

		case Measurements:
			fmt.Fprintln(w, "Measurements: ")

		case *Measurement:
			fmt.Fprintln(w)
			fmt.Fprintf(w, "  Name: %s\n", n.Name)

		case Tags:
			fmt.Fprintln(w, "  Tags:")

		case Fields:
			fmt.Fprintln(w, "  Fields:")

		case *Field:
			if n.TimePrecision != nil {
				fmt.Fprintf(w, "    %s: %s, count=%d, time-precision=%s\n", n.Name, n.Source, n.Count, *n.TimePrecision)
			} else {
				fmt.Fprintf(w, "    %s: %s, count=%d, time-interval=%s\n", n.Name, n.Source, n.Count, n.TimeInterval)
			}

		case *Tag:
			fmt.Fprintf(w, "    %s: %s\n", n.Name, n.Source)

		}

		return true
	}

	WalkDown(VisitorFn(walkFn), root)

	return w.String()
}

func TestSchema(t *testing.T) {
	in := `
title = "example schema"
series-limit = 10

[[measurements]]
    name = "constant"
    series-limit = 5

    [[measurements.tags]]
        name   = "tag0"
        source = [ "host1", "host2" ]

    [[measurements.tags]]
        name   = "tag1"
        source = { type = "file", path = "foo.txt" }

    [[measurements.fields]]
        name   = "floatC"
        count  = 5000
        source = 0.5
        time-precision = "us"

    [[measurements.fields]]
        name   = "integerC"
        count  = 5000
        source = 3
        time-precision = "hour"

    [[measurements.fields]]
        name   = "stringC"
        count  = 5000
        source = "hello"
        time-interval = "60s"

    [[measurements.fields]]
        name   = "stringA"
        count  = 5000
        source = ["hello", "world"]

    [[measurements.fields]]
        name   = "boolf"
        count  = 5000
        source = false

[[measurements]]
name = "random"

    [[measurements.tags]]
        name   = "tagSeq"
        source = { type = "sequence", format = "value%s", start = 0, count = 100 }

    [[measurements.fields]]
        name   = "floatR"
        count  = 5000
        source = { type = "rand<float>", min = 0.5, max = 50.1, seed = 10 }
        time-precision = "us"

[[measurements]]
name = "array"

    [[measurements.tags]]
        name   = "tagSeq"
        source = { type = "sequence", format = "value%s", start = 0, count = 100 }

    [[measurements.tags]]
        name   = "tagFile"
        source = { type = "file", path = "foo.txt" }

    [[measurements.fields]]
        name   = "stringA"
        count  = 1000
        source = ["this", "that"]
        time-precision = "us"

    [[measurements.fields]]
        name   = "integerA"
        count  = 1000
        source = [5, 6, 7]
        time-interval = "90s"
`
	var out Schema
	_, err := toml.Decode(in, &out)
	if err != nil {
		t.Fatalf("unxpected error: %v", err)
	}

	exp := `Measurements: 

  Name: constant
  Tags:
    tag0: array, source=[]string{"host1", "host2"}
    tag1: file, path=foo.txt
  Fields:
    floatC: constant, source=0.5, count=5000, time-precision=Microsecond
    integerC: constant, source=3, count=5000, time-precision=Hour
    stringC: constant, source="hello", count=5000, time-interval=1m0s
    stringA: array, source=[]string{"hello", "world"}, count=5000, time-precision=Millisecond
    boolf: constant, source=false, count=5000, time-precision=Millisecond

  Name: random
  Tags:
    tagSeq: sequence, prefix="value%s", range=[0,100)
  Fields:
    floatR: rand<float>, seed=10, min=50.100000, max=50.100000, count=5000, time-precision=Microsecond

  Name: array
  Tags:
    tagSeq: sequence, prefix="value%s", range=[0,100)
    tagFile: file, path=foo.txt
  Fields:
    stringA: array, source=[]string{"this", "that"}, count=1000, time-precision=Microsecond
    integerA: array, source=[]int64{5, 6, 7}, count=1000, time-interval=1m30s
`
	if got := visit(&out); !cmp.Equal(got, exp) {
		t.Errorf("unexpected value, -got/+exp\n%s", cmp.Diff(got, exp))
	}
}
