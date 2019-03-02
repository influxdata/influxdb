package models_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
)

func init() {
	// Force uint support to be enabled for testing.
	models.EnableUintSupport()
}

var (
	tags   = models.NewTags(map[string]string{"foo": "bar", "apple": "orange", "host": "serverA", "region": "uswest"})
	fields = models.Fields{
		"int64":         int64(math.MaxInt64),
		"uint32":        uint32(math.MaxUint32),
		"string":        "String field that has a decent length, probably some log message or something",
		"boolean":       false,
		"float64-tiny":  float64(math.SmallestNonzeroFloat64),
		"float64-large": float64(math.MaxFloat64),
	}
	maxFloat64 = strconv.FormatFloat(math.MaxFloat64, 'f', 1, 64)
	minFloat64 = strconv.FormatFloat(-math.MaxFloat64, 'f', 1, 64)

	sink interface{}
)

func TestMarshal(t *testing.T) {
	got := tags.HashKey()
	if exp := ",apple=orange,foo=bar,host=serverA,region=uswest"; string(got) != exp {
		t.Log("got: ", string(got))
		t.Log("exp: ", exp)
		t.Error("invalid match")
	}
}

func TestMarshalFields(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value interface{}
		exp   string
	}{
		{
			name:  "Float",
			value: float64(2),
			exp:   `value=2`,
		},
		{
			name:  "Integer",
			value: int64(2),
			exp:   `value=2i`,
		},
		{
			name:  "Unsigned",
			value: uint64(2),
			exp:   `value=2u`,
		},
		{
			name:  "String",
			value: "foobar",
			exp:   `value="foobar"`,
		},
		{
			name:  "Boolean",
			value: true,
			exp:   `value=true`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fields := map[string]interface{}{"value": tt.value}
			if have, want := models.Fields(fields).MarshalBinary(), []byte(tt.exp); !bytes.Equal(have, want) {
				t.Fatalf("unexpected field output: %s != %s", string(have), string(want))
			}
		})
	}
}

func TestTags_HashKey(t *testing.T) {
	tags := models.NewTags(map[string]string{"A FOO": "bar", "APPLE": "orange", "host": "serverA", "region": "uswest"})
	got := tags.HashKey()
	if exp := ",A\\ FOO=bar,APPLE=orange,host=serverA,region=uswest"; string(got) != exp {
		t.Log("got: ", string(got))
		t.Log("exp: ", exp)
		t.Error("invalid match")
	}
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tags.HashKey()
	}
}

func TestPoint_Tags(t *testing.T) {
	examples := []struct {
		Point string
		Tags  models.Tags
	}{
		{`cpu value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value"})},
		{"cpu,tag0=v0 value=1", models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": "v0"})},
		{"cpu,tag0=v0,tag1=v0 value=1", models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": "v0", "tag1": "v0"})},
		{`cpu,tag0=v\ 0 value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": "v 0"})},
		{`cpu,tag0=v\ 0\ 1,tag1=v2 value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": "v 0 1", "tag1": "v2"})},
		{`cpu,tag0=\, value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": ","})},
		{`cpu,ta\ g0=\, value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "ta g0": ","})},
		{`cpu,tag0=\,1 value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": ",1"})},
		{`cpu,tag0=1\"\",t=k value=1`, models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "tag0": `1\"\"`, "t": "k"})},
	}

	for _, example := range examples {
		t.Run(example.Point, func(t *testing.T) {
			pts, err := models.ParsePointsString(example.Point, "mm")
			if err != nil {
				t.Fatal(err)
			} else if len(pts) != 1 {
				t.Fatalf("parsed %d points, expected 1", len(pts))
			}

			// Repeat to test Tags() caching
			for i := 0; i < 2; i++ {
				tags := pts[0].Tags()
				if !reflect.DeepEqual(tags, example.Tags) {
					t.Fatalf("tag mismatch\ngot %s - %#v\nexp %s - %#v", tags.String(), tags, example.Tags.String(), example.Tags)
				}
			}
		})
	}
}

func TestPoint_StringSize(t *testing.T) {
	testPoint_cube(t, func(p models.Point) {
		l := p.StringSize()
		s := p.String()

		if l != len(s) {
			t.Errorf("Incorrect length for %q. got %v, exp %v", s, l, len(s))
		}
	})
}

func TestPoint_AppendString(t *testing.T) {
	testPoint_cube(t, func(p models.Point) {
		got := p.AppendString(nil)
		exp := []byte(p.String())

		if !reflect.DeepEqual(exp, got) {
			t.Errorf("AppendString() didn't match String(): got %v, exp %v", got, exp)
		}
	})
}

func testPoint_cube(t *testing.T, f func(p models.Point)) {
	// heard of a table-driven test? let's make a cube-driven test...
	tagList := []models.Tags{nil, {models.NewTag([]byte("foo"), []byte("bar"))}, tags}
	fieldList := []models.Fields{{"a": 42.0}, {"a": 42, "b": "things"}, fields}
	timeList := []time.Time{time.Time{}, time.Unix(0, 0), time.Unix(-34526, 0), time.Unix(231845, 0), time.Now()}

	for _, tagSet := range tagList {
		for _, fieldSet := range fieldList {
			for _, pointTime := range timeList {
				p, err := models.NewPoint("test", tagSet, fieldSet, pointTime)
				if err != nil {
					t.Errorf("unexpected error creating point: %v", err)
					continue
				}

				f(p)
			}
		}
	}
}

func TestTag_Clone(t *testing.T) {
	tag := models.NewTag([]byte("key"), []byte("value"))

	c := tag.Clone()

	if &c.Key == &tag.Key || !bytes.Equal(c.Key, tag.Key) {
		t.Fatalf("key %s should have been a clone of %s", c.Key, tag.Key)
	}

	if &c.Value == &tag.Value || !bytes.Equal(c.Value, tag.Value) {
		t.Fatalf("value %s should have been a clone of %s", c.Value, tag.Value)
	}
}

func TestTags_Clone(t *testing.T) {
	tags := models.NewTags(map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"})

	clone := tags.Clone()

	for i := range tags {
		tag := tags[i]
		c := clone[i]
		if &c.Key == &tag.Key || !bytes.Equal(c.Key, tag.Key) {
			t.Fatalf("key %s should have been a clone of %s", c.Key, tag.Key)
		}

		if &c.Value == &tag.Value || !bytes.Equal(c.Value, tag.Value) {
			t.Fatalf("value %s should have been a clone of %s", c.Value, tag.Value)
		}
	}
}

var p models.Point

func BenchmarkNewPoint(b *testing.B) {
	ts := time.Now()
	for i := 0; i < b.N; i++ {
		p, _ = models.NewPoint("measurement", tags, fields, ts)
	}
}

func BenchmarkNewPointFromBinary(b *testing.B) {
	pts, err := models.ParsePointsString("cpu value1=1.0,value2=1.0,value3=3.0,value4=4,value5=\"five\" 1000000000", "")
	if err != nil {
		b.Fatalf("unexpected error ParsePointsString: %v", err)
	}

	bytes, err := pts[0].MarshalBinary()
	if err != nil {
		b.Fatalf("unexpected error MarshalBinary: %v", err)
	}

	for i := 0; i < b.N; i++ {
		_, err := models.NewPointFromBytes(bytes)
		if err != nil {
			b.Fatalf("unexpected error NewPointsFromBytes: %v", err)
		}
	}
}

func BenchmarkParsePointNoTags5000(b *testing.B) {
	var batch [5000]string
	for i := 0; i < len(batch); i++ {
		batch[i] = `cpu value=1i 1000000000`
	}
	lines := strings.Join(batch[:], "\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		models.ParsePoints([]byte(lines), []byte("mm"))
		b.SetBytes(int64(len(lines)))
	}
}

func BenchmarkParsePointNoTags(b *testing.B) {
	line := `cpu value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointWithPrecisionN(b *testing.B) {
	line := `cpu value=1i 1000000000`
	defaultTime := time.Now().UTC()
	for i := 0; i < b.N; i++ {
		models.ParsePointsWithPrecision([]byte(line), []byte("mm"), defaultTime, "ns")
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointWithPrecisionU(b *testing.B) {
	line := `cpu value=1i 1000000000`
	defaultTime := time.Now().UTC()
	for i := 0; i < b.N; i++ {
		models.ParsePointsWithPrecision([]byte(line), []byte("mm"), defaultTime, "us")
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointsTagsSorted2(b *testing.B) {
	line := `cpu,host=serverA,region=us-west value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointsTagsSorted5(b *testing.B) {
	line := `cpu,env=prod,host=serverA,region=us-west,target=servers,zone=1c value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointsTagsSorted10(b *testing.B) {
	line := `cpu,env=prod,host=serverA,region=us-west,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5,target=servers,zone=1c value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointsTagsUnSorted2(b *testing.B) {
	line := `cpu,region=us-west,host=serverA value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		pt, _ := models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
		pt[0].Key()
	}
}

func BenchmarkParsePointsTagsUnSorted5(b *testing.B) {
	line := `cpu,region=us-west,host=serverA,env=prod,target=servers,zone=1c value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		pt, _ := models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
		pt[0].Key()
	}
}

func BenchmarkParsePointsTagsUnSorted10(b *testing.B) {
	line := `cpu,region=us-west,host=serverA,env=prod,target=servers,zone=1c,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5 value=1i 1000000000`
	for i := 0; i < b.N; i++ {
		pt, _ := models.ParsePoints([]byte(line), []byte("mm"))
		b.SetBytes(int64(len(line)))
		pt[0].Key()
	}
}

func BenchmarkParseKey(b *testing.B) {
	line := `cpu,region=us-west,host=serverA,env=prod,target=servers,zone=1c,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5`
	for i := 0; i < b.N; i++ {
		models.ParseKey([]byte(line))
	}
}

// TestPoint wraps a models.Point but also makes available the raw
// arguments to the Point.
//
// This is useful for ensuring that comparisons between results of
// operations on Points match the expected input data to the Point,
// since models.Point does not expose the raw input data (e.g., tags)
// via its API.
type TestPoint struct {
	RawFields models.Fields
	RawTags   models.Tags
	RawTime   time.Time
	models.Point
}

// NewTestPoint returns a new TestPoint.
//
// NewTestPoint panics if it is not a valid models.Point.
func NewTestPoint(name string, tags models.Tags, fields models.Fields, time time.Time) TestPoint {
	return TestPoint{
		RawTags:   tags,
		RawFields: fields,
		RawTime:   time,
		Point:     models.MustNewPoint(name, tags, fields, time),
	}
}

func testParsePoints(t *testing.T, line string, mm string, points ...TestPoint) {
	t.Helper()

	pts, err := models.ParsePointsWithPrecision([]byte(line), []byte(mm), time.Unix(0, 0), "ns")
	if err != nil {
		t.Fatalf(`ParsePoints("%s") mismatch. got %v, exp nil`, line, err)
	}

	if exp := len(points); len(pts) != exp {
		t.Fatalf(`ParsePoints("%s") len mismatch. got %d, exp %d`, line, len(pts), exp)
	}

	for i, point := range points {
		if exp := point.Key(); !bytes.Equal(pts[i].Key(), exp) {
			t.Errorf("%d. ParsePoints(\"%s\") key mismatch.\ngot %v\nexp %v", i, line, string(pts[i].Key()), string(exp))
		}

		if exp := len(point.Tags()); len(pts[i].Tags()) != exp {
			t.Errorf(`%d. ParsePoints("%s") tags mismatch. got %v, exp %v`, i, line, pts[i].Tags(), exp)
		}

		for _, tag := range pts[i].Tags() {
			if !bytes.Equal(tag.Value, point.RawTags.Get(tag.Key)) {
				t.Errorf(`%d. ParsePoints("%s") tags mismatch. got %s, exp %s`, i, line, tag.Value, point.RawTags.Get(tag.Key))
			}
		}

		for name, value := range point.RawFields {
			fields, err := pts[i].Fields()
			if err != nil {
				t.Fatal(err)
			}
			val := fields[name]
			expfval, ok := val.(float64)

			if ok && math.IsNaN(expfval) {
				gotfval, ok := value.(float64)
				if ok && !math.IsNaN(gotfval) {
					t.Errorf(`%d. ParsePoints("%s") field '%s' mismatch. exp NaN`, i, line, name)
				}
			}
			if !reflect.DeepEqual(val, value) {
				t.Errorf(`%d. ParsePoints("%s") field '%s' mismatch. got %[3]v (%[3]T), exp %[4]v (%[4]T)`, i, line, name, val, value)
			}
		}

		if !pts[i].Time().Equal(point.Time()) {
			t.Errorf(`%d. ParsePoints("%s") time mismatch. got %v, exp %v`, i, line, pts[i].Time(), point.Time())
		}
	}
}

func TestParsePointNoValue(t *testing.T) {
	pts, err := models.ParsePointsString("", "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, "", err)
	}

	if exp := 0; len(pts) != exp {
		t.Errorf(`ParsePoints("%s") len mismatch. got %v, exp %v`, "", len(pts), exp)
	}
}

func TestParsePointWhitespaceValue(t *testing.T) {
	pts, err := models.ParsePointsString(" ", "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, "", err)
	}

	if exp := 0; len(pts) != exp {
		t.Errorf(`ParsePoints("%s") len mismatch. got %v, exp %v`, "", len(pts), exp)
	}
}

func TestParsePointNoFields(t *testing.T) {
	expectedSuffix := "missing fields"
	examples := []string{
		"cpu_load_short,host=server01,region=us-west",
		"cpu",
		"cpu,host==",
		"=",
	}

	for i, example := range examples {
		_, err := models.ParsePointsString(example, "mm")
		if err == nil {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got nil, exp error`, i, example)
		} else if !strings.HasSuffix(err.Error(), expectedSuffix) {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got %q, exp suffix %q`, i, example, err, expectedSuffix)
		}
	}
}

func TestParsePointNoTimestamp(t *testing.T) {
	testParsePoints(t, "cpu value=1", "mm", NewTestPoint("mm", models.NewTags(map[string]string{"_f": "value", "_m": "cpu"}), models.Fields{"value": 1.0}, time.Unix(0, 0)))
}

func TestParsePointMissingQuote(t *testing.T) {
	expectedSuffix := "unbalanced quotes"
	examples := []string{
		`cpu,host=serverA value="test`,
		`cpu,host=serverA value="test""`,
	}

	for i, example := range examples {
		_, err := models.ParsePointsString(example, "mm")
		if err == nil {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got nil, exp error`, i, example)
		} else if !strings.HasSuffix(err.Error(), expectedSuffix) {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got %q, exp suffix %q`, i, example, err, expectedSuffix)
		}
	}
}

func TestParsePointMissingTagKey(t *testing.T) {
	expectedSuffix := "missing tag key"
	examples := []string{
		`cpu, value=1`,
		`cpu,`,
		`cpu,,,`,
		`cpu,host=serverA,=us-east value=1i`,
		`cpu,host=serverAa\,,=us-east value=1i`,
		`cpu,host=serverA\,,=us-east value=1i`,
		`cpu, =serverA value=1i`,
	}

	for i, example := range examples {
		_, err := models.ParsePointsString(example, "mm")
		if err == nil {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got nil, exp error`, i, example)
		} else if !strings.HasSuffix(err.Error(), expectedSuffix) {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got %q, exp suffix %q`, i, example, err, expectedSuffix)
		}
	}

	_, err := models.ParsePointsString(`cpu,host=serverA,\ =us-east value=1i`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,\ =us-east value=1i`, err)
	}
}

func TestParsePointMissingTagValue(t *testing.T) {
	expectedSuffix := "missing tag value"
	examples := []string{
		`cpu,host`,
		`cpu,host,`,
		`cpu,host=`,
		`cpu,host value=1i`,
		`cpu,host=serverA,region value=1i`,
		`cpu,host=serverA,region= value=1i`,
		`cpu,host=serverA,region=,zone=us-west value=1i`,
	}

	for i, example := range examples {
		_, err := models.ParsePointsString(example, "mm")
		if err == nil {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got nil, exp error`, i, example)
		} else if !strings.HasSuffix(err.Error(), expectedSuffix) {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got %q, exp suffix %q`, i, example, err, expectedSuffix)
		}
	}
}

func TestParsePointInvalidTagFormat(t *testing.T) {
	expectedSuffix := "invalid tag format"
	examples := []string{
		`cpu,host=f=o,`,
		`cpu,host=f\==o,`,
	}

	for i, example := range examples {
		_, err := models.ParsePointsString(example, "mm")
		if err == nil {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got nil, exp error`, i, example)
		} else if !strings.HasSuffix(err.Error(), expectedSuffix) {
			t.Errorf(`[Example %d] ParsePoints("%s") mismatch. got %q, exp suffix %q`, i, example, err, expectedSuffix)
		}
	}
}

func TestParsePointMissingFieldName(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west =`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west =`)
	}

	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west =123i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west =123i`)
	}

	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west a\ =123i`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west a\ =123i`)
	}
	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=123i,=456i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=123i,=456i`)
	}
}

func TestParsePointMissingFieldValue(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=`)
	}

	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value= 1000000000i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value= 1000000000i`)
	}

	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=,value2=1i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=,value2=1i`)
	}

	_, err = models.ParsePointsString(`cpu,host=server01,region=us-west 1434055562000000000i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=server01,region=us-west 1434055562000000000i`)
	}

	_, err = models.ParsePointsString(`cpu,host=server01,region=us-west value=1i,b`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=server01,region=us-west value=1i,b`)
	}

	_, err = models.ParsePointsString(`m f="blah"=123,r 1531703600000000000`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `m f="blah"=123,r 1531703600000000000`)
	}
}

func TestParsePointBadNumber(t *testing.T) {
	for _, tt := range []string{
		"cpu v=- ",
		"cpu v=-i ",
		"cpu v=-. ",
		"cpu v=. ",
		"cpu v=1.0i ",
		"cpu v=1ii ",
		"cpu v=1a ",
		"cpu v=-e-e-e ",
		"cpu v=42+3 ",
		"cpu v= ",
		"cpu v=-123u",
	} {
		_, err := models.ParsePointsString(tt, "mm")
		if err == nil {
			t.Errorf("Point %q should be invalid", tt)
		}
	}
}

func TestParsePointMaxInt64(t *testing.T) {
	// out of range
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=9223372036854775808i`, "mm")
	exp := `unable to parse 'cpu,host=serverA,region=us-west value=9223372036854775808i': unable to parse integer 9223372036854775808: strconv.ParseInt: parsing "9223372036854775808": value out of range`
	if err == nil || (err != nil && err.Error() != exp) {
		t.Fatalf("Error mismatch:\nexp: %s\ngot: %v", exp, err)
	}

	// max int
	p, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=9223372036854775807i`, "mm")
	if err != nil {
		t.Fatalf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=9223372036854775807i`, err)
	}
	fields, err := p[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := int64(9223372036854775807), fields["value"].(int64); exp != got {
		t.Fatalf("ParsePoints Value mismatch. \nexp: %v\ngot: %v", exp, got)
	}

	// leading zeros
	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=0009223372036854775807i`, "mm")
	if err != nil {
		t.Fatalf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=0009223372036854775807i`, err)
	}
}

func TestParsePointMinInt64(t *testing.T) {
	// out of range
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=-9223372036854775809i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=-9223372036854775809i`)
	}

	// min int
	p, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=-9223372036854775808i`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=-9223372036854775808i`, err)
	}
	fields, err := p[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := int64(-9223372036854775808), fields["value"].(int64); exp != got {
		t.Fatalf("ParsePoints Value mismatch. \nexp: %v\ngot: %v", exp, got)
	}

	// leading zeros
	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=-0009223372036854775808i`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=-0009223372036854775808i`, err)
	}
}

func TestParsePointMaxFloat64(t *testing.T) {
	// out of range
	_, err := models.ParsePointsString(fmt.Sprintf(`cpu,host=serverA,region=us-west value=%s`, "1"+string(maxFloat64)), "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=...`)
	}

	// max float
	p, err := models.ParsePointsString(fmt.Sprintf(`cpu,host=serverA,region=us-west value=%s`, string(maxFloat64)), "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=9223372036854775807`, err)
	}
	fields, err := p[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := math.MaxFloat64, fields["value"].(float64); exp != got {
		t.Fatalf("ParsePoints Value mismatch. \nexp: %v\ngot: %v", exp, got)
	}

	// leading zeros
	_, err = models.ParsePointsString(fmt.Sprintf(`cpu,host=serverA,region=us-west value=%s`, "0000"+string(maxFloat64)), "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=0009223372036854775807`, err)
	}
}

func TestParsePointMinFloat64(t *testing.T) {
	// out of range
	_, err := models.ParsePointsString(fmt.Sprintf(`cpu,host=serverA,region=us-west value=%s`, "-1"+string(minFloat64)[1:]), "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=...`)
	}

	// min float
	p, err := models.ParsePointsString(fmt.Sprintf(`cpu,host=serverA,region=us-west value=%s`, string(minFloat64)), "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=...`, err)
	}
	fields, err := p[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := -math.MaxFloat64, fields["value"].(float64); exp != got {
		t.Fatalf("ParsePoints Value mismatch. \nexp: %v\ngot: %v", exp, got)
	}

	// leading zeros
	_, err = models.ParsePointsString(fmt.Sprintf(`cpu,host=serverA,region=us-west value=%s`, "-0000000"+string(minFloat64)[1:]), "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=...`, err)
	}
}

func TestParsePointMaxUint64(t *testing.T) {
	// out of range
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=18446744073709551616u`, "mm")
	exp := `unable to parse 'cpu,host=serverA,region=us-west value=18446744073709551616u': unable to parse unsigned 18446744073709551616: strconv.ParseUint: parsing "18446744073709551616": value out of range`
	if err == nil || (err != nil && err.Error() != exp) {
		t.Fatalf("Error mismatch:\nexp: %s\ngot: %v", exp, err)
	}

	// max int
	p, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=18446744073709551615u`, "mm")
	if err != nil {
		t.Fatalf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=18446744073709551615u`, err)
	}
	fields, err := p[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := uint64(18446744073709551615), fields["value"].(uint64); exp != got {
		t.Fatalf("ParsePoints Value mismatch. \nexp: %v\ngot: %v", exp, got)
	}

	// leading zeros
	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=00018446744073709551615u`, "mm")
	if err != nil {
		t.Fatalf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=00018446744073709551615u`, err)
	}
}

func TestParsePointMinUint64(t *testing.T) {
	// out of range
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=--1u`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=-1u`)
	}

	// min int
	p, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=0u`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=0u`, err)
	}
	fields, err := p[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := uint64(0), fields["value"].(uint64); exp != got {
		t.Fatalf("ParsePoints Value mismatch. \nexp: %v\ngot: %v", exp, got)
	}

	// leading zeros
	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=0000u`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=0000u`, err)
	}
}

func TestParsePointNumberNonNumeric(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=.1a`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=.1a`)
	}
}

func TestParsePointNegativeWrongPlace(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=0.-1`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=0.-1`)
	}
}

func TestParsePointOnlyNegativeSign(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=-`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=-`)
	}
}

func TestParsePointFloatMultipleDecimals(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1.1.1`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=1.1.1`)
	}
}

func TestParsePointInteger(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1i`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1i`, err)
	}
}

func TestParsePointNegativeInteger(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=-1i`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=-1i`, err)
	}
}

func TestParsePointNegativeFloat(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=-1.0`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=-1.0`, err)
	}
}

func TestParsePointFloatNoLeadingDigit(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=.1`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=-1.0`, err)
	}
}

func TestParsePointFloatScientific(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1.0e4`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1.0e4`, err)
	}

	pts, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1e4`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1.0e4`, err)
	}

	fields, err := pts[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if fields["value"] != 1e4 {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1e4`, err)
	}
}

func TestParsePointFloatScientificUpper(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1.0E4`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1.0E4`, err)
	}

	pts, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1E4`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1.0E4`, err)
	}

	fields, err := pts[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if fields["value"] != 1e4 {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1E4`, err)
	}
}

func TestParsePointFloatScientificDecimal(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=1.0e-4`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=1.0e-4`, err)
	}
}

func TestParsePointFloatNegativeScientific(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=-1.0e-4`, "mm")
	if err != nil {
		t.Errorf(`ParsePoints("%s") mismatch. got %v, exp nil`, `cpu,host=serverA,region=us-west value=-1.0e-4`, err)
	}
}

func TestParsePointBooleanInvalid(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=a`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=a`)
	}
}

func TestParsePointScientificIntInvalid(t *testing.T) {
	_, err := models.ParsePointsString(`cpu,host=serverA,region=us-west value=9ie10`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=9ie10`)
	}

	_, err = models.ParsePointsString(`cpu,host=serverA,region=us-west value=9e10i`, "mm")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, `cpu,host=serverA,region=us-west value=9e10i`)
	}
}

func TestParsePointWhitespace(t *testing.T) {
	examples := []string{
		`cpu    value=1.0 1257894000000000000`,
		`cpu value=1.0     1257894000000000000`,
		`cpu      value=1.0     1257894000000000000`,
		`cpu value=1.0 1257894000000000000   `,
		`cpu value=1.0 1257894000000000000
`,
		`cpu   value=1.0 1257894000000000000
`,
	}

	expPoint := NewTestPoint("mm", models.NewTags(map[string]string{"_f": "value", "_m": "cpu"}), models.Fields{"value": 1.0}, time.Unix(0, 1257894000000000000))
	for i, example := range examples {
		pts, err := models.ParsePoints([]byte(example), []byte("mm"))
		if err != nil {
			t.Fatalf(`[Example %d] ParsePoints("%s") error. got %v, exp nil`, i, example, err)
		}

		if got, exp := len(pts), 1; got != exp {
			t.Fatalf("[Example %d] got %d points, expected %d", i, got, exp)
		}

		if got, exp := string(pts[0].Name()), string(expPoint.Name()); got != exp {
			t.Fatalf("[Example %d] got %v measurement, expected %v", i, got, exp)
		}

		fields, err := pts[0].Fields()
		if err != nil {
			t.Fatal(err)
		}
		eFields, err := expPoint.Fields()
		if err != nil {
			t.Fatal(err)
		}
		if got, exp := len(fields), len(eFields); got != exp {
			t.Fatalf("[Example %d] got %d fields, expected %d", i, got, exp)
		}

		if got, exp := fields["value"], eFields["value"]; got != exp {
			t.Fatalf(`[Example %d] got %v for field "value", expected %v`, i, got, exp)
		}

		if got, exp := pts[0].Time().UnixNano(), expPoint.Time().UnixNano(); got != exp {
			t.Fatalf(`[Example %d] got %d time, expected %d`, i, got, exp)
		}
	}
}

func TestParsePointUnescape(t *testing.T) {
	// commas in measurement name
	testParsePoints(t, `foo\,bar value=1i`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{"_f": "value", "_m": "foo,bar"}), // comma in the name
			models.Fields{
				"value": int64(1),
			},
			time.Unix(0, 0)))

	// comma in measurement name with tags
	testParsePoints(t, `cpu\,main,regions=east value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      "cpu,main", // comma in the name
				"regions": "east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// spaces in measurement name
	testParsePoints(t, `cpu\ load,region=east value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":     "value",
				"_m":     "cpu load", // space in the name
				"region": "east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// equals in measurement name
	testParsePoints(t, `cpu\=load,region=east value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":     "value",
				"_m":     `cpu\=load`, // backslash is literal
				"region": "east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// equals in measurement name
	testParsePoints(t, `cpu=load,region=east value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":     "value",
				"_m":     `cpu=load`, // literal equals is fine in measurement name
				"region": "east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// commas in tag names
	testParsePoints(t, `cpu,region\,zone=east value=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":          "value",
				"_m":          `cpu`,
				"region,zone": "east", // comma in the tag key
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// spaces in tag name
	testParsePoints(t, `cpu,region\ zone=east value=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":          "value",
				"_m":          `cpu`,
				"region zone": "east", // space in the tag name
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// backslash with escaped equals in tag name
	testParsePoints(t, `cpu,reg\\=ion=east value=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":       "value",
				"_m":       `cpu`,
				`reg\=ion`: "east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// space is tag name
	testParsePoints(t, `cpu,\ =east value=1.0`, "mm",
		NewTestPoint("mm",
			models.Tags{
				{Key: []byte("_f"), Value: []byte("value")},
				{Key: []byte("_m"), Value: []byte("cpu")},
				{Key: []byte(" "), Value: []byte("east")}, // tag name is single space
			},
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// commas in tag values
	testParsePoints(t, `cpu,regions=east\,west value=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": "east,west", // comma in the tag value
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// backslash literal followed by escaped space
	testParsePoints(t, `cpu,regions=\\ east value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": `\ east`,
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// backslash literal followed by escaped space
	testParsePoints(t, `cpu,regions=eas\\ t value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": `eas\ t`,
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// backslash literal followed by trailing space
	testParsePoints(t, `cpu,regions=east\\  value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": `east\ `,
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// spaces in tag values
	testParsePoints(t, `cpu,regions=east\ west value=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": "east west", // comma in the tag value
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// commas in field keys
	testParsePoints(t, `cpu,regions=east value\,ms=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":      "value,ms",
				"_m":      `cpu`,
				"regions": "east",
			}),
			models.Fields{
				"value,ms": 1.0, // comma in the field keys
			},
			time.Unix(0, 0)))

	// spaces in field keys
	testParsePoints(t, `cpu,regions=east value\ ms=1.0`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":      "value ms",
				"_m":      `cpu`,
				"regions": "east",
			}),
			models.Fields{
				"value ms": 1.0, // comma in the field keys
			},
			time.Unix(0, 0)))

	// tag with no value
	testParsePoints(t, `cpu,regions=east value="1"`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": "east",
				"foobar":  "",
			}),
			models.Fields{
				"value": "1",
			},
			time.Unix(0, 0)))

	// commas in field values
	testParsePoints(t, `cpu,regions=east value="1,0"`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": "east",
			}),
			models.Fields{
				"value": "1,0", // comma in the field value
			},
			time.Unix(0, 0)))

	// random character escaped
	testParsePoints(t, `cpu,regions=eas\t value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": "eas\\t",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// backslash literal followed by escaped characters
	testParsePoints(t, `cpu,regions=\\,\,\=east value=1.0`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":      "value",
				"_m":      `cpu`,
				"regions": `\,,=east`,
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, 0)))

	// field keys using escape char.
	testParsePoints(t, `cpu \a=1i`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f": "\\a",
				"_m": `cpu`,
			}),
			models.Fields{
				"\\a": int64(1), // Left as parsed since it's not a known escape sequence.
			},
			time.Unix(0, 0)))

	// measurement, tag and tag value with equals
	testParsePoints(t, `cpu=load,equals\=foo=tag\=value value=1i`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_f":         "value",
				"_m":         `cpu=load`,  // Not escaped
				"equals=foo": "tag=value", // Tag and value unescaped
			}),
			models.Fields{
				"value": int64(1),
			},
			time.Unix(0, 0)))
}

func TestParsePointWithTags(t *testing.T) {
	testParsePoints(t,
		"cpu,host=serverA,region=us-east value=1.0 1000000000", "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{"_m": "cpu", "_f": "value", "host": "serverA", "region": "us-east"}),
			models.Fields{"value": 1.0}, time.Unix(1, 0)))
}

func TestParsePointWithDuplicateTags(t *testing.T) {
	for i, tt := range []struct {
		line string
		err  string
	}{
		{
			line: `cpu,host=serverA,host=serverB value=1i 1000000000`,
			err:  `unable to parse 'cpu,host=serverA,host=serverB value=1i 1000000000': duplicate tags`,
		},
		{
			line: `cpu,b=2,b=1,c=3 value=1i 1000000000`,
			err:  `unable to parse 'cpu,b=2,b=1,c=3 value=1i 1000000000': duplicate tags`,
		},
		{
			line: `cpu,b=2,c=3,b=1 value=1i 1000000000`,
			err:  `unable to parse 'cpu,b=2,c=3,b=1 value=1i 1000000000': duplicate tags`,
		},
	} {
		_, err := models.ParsePointsString(tt.line, "mm")
		if err == nil || tt.err != err.Error() {
			t.Errorf("%d. ParsePoint() expected error '%s'. got '%s'", i, tt.err, err)
		}
	}
}

func TestParsePointWithStringField(t *testing.T) {
	testParsePoints(t, `cpu,host=serverA,region=us-east value=1.0,str="foo",str2="bar" 1000000000`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": "foo",
			},
			time.Unix(1, 0)),
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str2",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str2": "bar",
			},
			time.Unix(1, 0)),
	)

	testParsePoints(t, `cpu,host=serverA,region=us-east str="foo \" bar" 1000000000`, "mm",
		NewTestPoint("mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": `foo " bar`,
			},
			time.Unix(1, 0)),
	)

}

func TestParsePointWithStringWithSpaces(t *testing.T) {
	testParsePoints(t, `cpu,host=serverA,region=us-east value=1.0,str="foo bar" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": "foo bar", // spaces in string value
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointWithStringWithNewline(t *testing.T) {
	testParsePoints(t, "cpu,host=serverA,region=us-east value=1.0,str=\"foo\nbar\" 1000000000", "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": "foo\nbar", // newline in string value
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointWithStringWithCommas(t *testing.T) {
	// escaped comma
	testParsePoints(t, `cpu,host=serverA,region=us-east value=1.0,str="foo\,bar" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": `foo\,bar`, // commas in string value
			},
			time.Unix(1, 0)),
	)

	// non-escaped comma
	testParsePoints(t, `cpu,host=serverA,region=us-east value=1.0,str="foo,bar" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": "foo,bar", // commas in string value
			},
			time.Unix(1, 0)),
	)

	// string w/ trailing escape chars
	testParsePoints(t, `cpu,host=serverA,region=us-east str="foo\\",str2="bar" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": "foo\\", // trailing escape char
			},
			time.Unix(1, 0)),
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str2",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str2": "bar",
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointQuotedMeasurement(t *testing.T) {
	// non-escaped comma
	testParsePoints(t, `"cpu",host=serverA,region=us-east value=1.0 1000000000`, "mm",
		NewTestPoint(
			`mm`,
			models.NewTags(map[string]string{
				"_m":     `"cpu"`,
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointQuotedTags(t *testing.T) {
	testParsePoints(t, `cpu,"host"="serverA",region=us-east value=1.0 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.Tags{
				{Key: []byte("_f"), Value: []byte("value")},
				{Key: []byte("_m"), Value: []byte("cpu")},
				{Key: []byte(`"host"`), Value: []byte(`"serverA"`)},
				{Key: []byte("region"), Value: []byte("us-east")},
			},
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
	)
}

func TestParsePoint_TrailingSlash(t *testing.T) {
	_, err := models.ParsePointsString(`a v=1 0\`, "mm")
	if err == nil {
		t.Fatalf("ParsePoints failed: %v", err)
	} else if !strings.Contains(err.Error(), "bad timestamp") {
		t.Fatalf("ParsePoints unexpected error: %v", err)
	}
}

func TestParsePointsUnbalancedQuotedTags(t *testing.T) {
	pts, err := models.ParsePointsString("baz,mytag=\"a x=1 1441103862125\nbaz,mytag=a z=1 1441103862126", "mm")
	if err != nil {
		t.Fatalf("ParsePoints failed: %v", err)
	}

	if exp := 2; len(pts) != exp {
		t.Fatalf("ParsePoints count mismatch. got %v, exp %v", len(pts), exp)
	}

	// Expected " in the tag value
	exp := models.MustNewPoint("mm", models.NewTags(map[string]string{"_f": "x", "_m": "baz", "mytag": `"a`}),
		models.Fields{"x": float64(1)}, time.Unix(0, 1441103862125))

	if pts[0].String() != exp.String() {
		t.Errorf("Point mismatch:\ngot: %v\nexp: %v", pts[0].String(), exp.String())
	}

	// Expected two points to ensure we did not overscan the line
	exp = models.MustNewPoint("mm", models.NewTags(map[string]string{"_m": "baz", "_f": "z", "mytag": `a`}),
		models.Fields{"z": float64(1)}, time.Unix(0, 1441103862126))

	if pts[1].String() != exp.String() {
		t.Errorf("Point mismatch:\ngot: %v\nexp: %v", pts[1].String(), exp.String())
	}
}

func TestParsePointEscapedStringsAndCommas(t *testing.T) {
	// non-escaped comma and quotes
	testParsePoints(t, `cpu,host=serverA,region=us-east value="{Hello\"{,}\" World}" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": `{Hello"{,}" World}`,
			},
			time.Unix(1, 0)),
	)

	// escaped comma and quotes
	testParsePoints(t, `cpu,host=serverA,region=us-east value="{Hello\"{\,}\" World}" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": `{Hello"{\,}" World}`,
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointWithStringWithEquals(t *testing.T) {
	testParsePoints(t, `cpu,host=serverA,region=us-east str="foo=bar",value=1.0 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "str",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"str": "foo=bar", // spaces in string value
			},
			time.Unix(1, 0)),
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointWithStringWithBackslash(t *testing.T) {
	testParsePoints(t, `cpu value="test\\\"" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": `test\"`,
			},
			time.Unix(1, 0)),
	)

	testParsePoints(t, `cpu value="test\\" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": `test\`,
			},
			time.Unix(1, 0)),
	)

	testParsePoints(t, `cpu value="test\\\"" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": `test\"`,
			},
			time.Unix(1, 0)),
	)

	testParsePoints(t, `cpu value="test\"" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": `test"`,
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointWithBoolField(t *testing.T) {
	testParsePoints(t, `cpu,host=serverA,region=us-east true=true,t=t,T=T,TRUE=TRUE,True=True,false=false,f=f,F=F,FALSE=FALSE,False=False 1000000000`, "mm",
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "true", "host": "serverA", "region": "us-east"}), models.Fields{"true": true}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "t", "host": "serverA", "region": "us-east"}), models.Fields{"t": true}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "T", "host": "serverA", "region": "us-east"}), models.Fields{"T": true}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "TRUE", "host": "serverA", "region": "us-east"}), models.Fields{"TRUE": true}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "True", "host": "serverA", "region": "us-east"}), models.Fields{"True": true}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "false", "host": "serverA", "region": "us-east"}), models.Fields{"false": false}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "f", "host": "serverA", "region": "us-east"}), models.Fields{"f": false}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "F", "host": "serverA", "region": "us-east"}), models.Fields{"F": false}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "FALSE", "host": "serverA", "region": "us-east"}), models.Fields{"FALSE": false}, time.Unix(1, 0)),
		NewTestPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "False", "host": "serverA", "region": "us-east"}), models.Fields{"False": false}, time.Unix(1, 0)),
	)
}

func TestParsePointUnicodeString(t *testing.T) {
	testParsePoints(t, `cpu,host=serverA,region=us-east value="wè" 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m":     "cpu",
				"_f":     "value",
				"host":   "serverA",
				"region": "us-east",
			}),
			models.Fields{
				"value": "wè",
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointNegativeTimestamp(t *testing.T) {
	testParsePoints(t, `cpu value=1 -1`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, -1)),
	)
}

func TestParsePointMaxTimestamp(t *testing.T) {
	testParsePoints(t, fmt.Sprintf(`cpu value=1 %d`, models.MaxNanoTime), "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, models.MaxNanoTime)),
	)
}

func TestParsePointMinTimestamp(t *testing.T) {
	testParsePoints(t, `cpu value=1 -9223372036854775806`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(0, models.MinNanoTime)),
	)
}

func TestParsePointInvalidTimestamp(t *testing.T) {
	examples := []string{
		"cpu value=1 9223372036854775808",
		"cpu value=1 -92233720368547758078",
		"cpu value=1 -",
		"cpu value=1 -/",
		"cpu value=1 -1?",
		"cpu value=1 1-",
		"cpu value=1 9223372036854775807 12",
	}

	for i, example := range examples {
		_, err := models.ParsePointsString(example, "mm")
		if err == nil {
			t.Fatalf("[Example %d] ParsePoints failed: %v", i, err)
		}
	}
}

func TestNewPointFloatWithoutDecimal(t *testing.T) {
	testParsePoints(t, `cpu value=1 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
	)
}
func TestNewPointNegativeFloat(t *testing.T) {
	testParsePoints(t, `cpu value=-0.64 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": -0.64,
			},
			time.Unix(1, 0)),
	)
}

func TestNewPointFloatNoDecimal(t *testing.T) {
	testParsePoints(t, `cpu value=1. 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": 1.0,
			},
			time.Unix(1, 0)),
	)
}

func TestNewPointFloatScientific(t *testing.T) {
	testParsePoints(t, `cpu value=6.632243e+06 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": float64(6632243),
			},
			time.Unix(1, 0)),
	)
}

func TestNewPointLargeInteger(t *testing.T) {
	testParsePoints(t, `cpu value=6632243i 1000000000`, "mm",
		NewTestPoint(
			"mm",
			models.NewTags(map[string]string{
				"_m": "cpu",
				"_f": "value",
			}),
			models.Fields{
				"value": int64(6632243), // if incorrectly encoded as a float, it would show up as 6.632243e+06
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointNaN(t *testing.T) {
	_, err := models.ParsePointsString("cpu value=NaN 1000000000", "mm")
	if err == nil {
		t.Fatalf("ParsePoints expected error, got nil")
	}

	_, err = models.ParsePointsString("cpu value=nAn 1000000000", "mm")
	if err == nil {
		t.Fatalf("ParsePoints expected error, got nil")
	}

	_, err = models.ParsePointsString("cpu value=NaN", "mm")
	if err == nil {
		t.Fatalf("ParsePoints expected error, got nil")
	}
}

func TestNewPointLargeNumberOfTags(t *testing.T) {
	tags := ""
	for i := 0; i < 255; i++ {
		tags += fmt.Sprintf(",tag%d=value%d", i, i)
	}

	pt, err := models.ParsePointsString(fmt.Sprintf("cpu%s value=1", tags), "mm")
	if err != nil {
		t.Fatalf("ParsePoints() with max tags failed: %v", err)
	}

	if len(pt[0].Tags()) != 257 { // add two for _m & _f
		t.Fatalf("expected %d tags, got %d", 255, len(pt[0].Tags()))
	}
}

func TestParsePointIntsFloats(t *testing.T) {
	pts, err := models.ParsePoints([]byte(`cpu,host=serverA,region=us-east int=10i,float=11.0,float2=12.1 1000000000`), []byte("mm"))
	if err != nil {
		t.Fatalf(`ParsePoints() failed. got %s`, err)
	}

	if exp := 3; len(pts) != exp {
		t.Errorf("ParsePoint() len mismatch: got %v, exp %v", len(pts), exp)
	}

	if fields, err := pts[0].Fields(); err != nil {
		t.Fatal(err)
	} else if _, ok := fields["int"].(int64); !ok {
		t.Errorf("ParsePoint() int field mismatch: got %T, exp %T", fields["int"], int64(10))
	}

	if fields, err := pts[1].Fields(); err != nil {
		t.Fatal(err)
	} else if _, ok := fields["float"].(float64); !ok {
		t.Errorf("ParsePoint() float field mismatch: got %T, exp %T", fields["float64"], float64(11.0))
	}

	if fields, err := pts[2].Fields(); err != nil {
		t.Fatal(err)
	} else if _, ok := fields["float2"].(float64); !ok {
		t.Errorf("ParsePoint() float field mismatch: got %T, exp %T", fields["float64"], float64(12.1))
	}
}

func TestParsePointKeyUnsorted(t *testing.T) {
	pts, err := models.ParsePoints([]byte("cpu,last=1,first=2 value=1i"), []byte("mm"))
	if err != nil {
		t.Fatalf(`ParsePoints() failed. got %s`, err)
	}

	if exp := 1; len(pts) != exp {
		t.Errorf("ParsePoint() len mismatch: got %v, exp %v", len(pts), exp)
	}
	pt := pts[0]

	if exp := "cpu,first=2,last=1"; string(pt.Key()) != `mm,_f=value,_m=cpu,first=2,last=1` {
		t.Errorf("ParsePoint key not sorted. got %v, exp %v", string(pt.Key()), exp)
	}
}

func TestParsePointToString(t *testing.T) {
	for i, tt := range []struct {
		line string
		exp  string
		pt   models.Point
	}{
		{
			line: `cpu,host=serverA,region=us-east bool=false 1000000000`,
			exp:  `mm,_f=bool,_m=cpu,host=serverA,region=us-east bool=false 1000000000`,
			pt:   models.MustNewPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "bool", "host": "serverA", "region": "us-east"}), models.Fields{"bool": false}, time.Unix(1, 0)),
		},
		{
			line: `cpu,host=serverA,region=us-east float=11 1000000000`,
			exp:  `mm,_f=float,_m=cpu,host=serverA,region=us-east float=11 1000000000`,
			pt:   models.MustNewPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "float", "host": "serverA", "region": "us-east"}), models.Fields{"float": float64(11.0)}, time.Unix(1, 0)),
		},
		{
			line: `cpu,host=serverA,region=us-east float2=12.123 1000000000`,
			exp:  `mm,_f=float2,_m=cpu,host=serverA,region=us-east float2=12.123 1000000000`,
			pt:   models.MustNewPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "float2", "host": "serverA", "region": "us-east"}), models.Fields{"float2": float64(12.123)}, time.Unix(1, 0)),
		},
		{
			line: `cpu,host=serverA,region=us-east int=10i 1000000000`,
			exp:  `mm,_f=int,_m=cpu,host=serverA,region=us-east int=10i 1000000000`,
			pt:   models.MustNewPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "int", "host": "serverA", "region": "us-east"}), models.Fields{"int": 10}, time.Unix(1, 0)),
		},
		{
			line: `cpu,host=serverA,region=us-east str="string val" 1000000000`,
			exp:  `mm,_f=str,_m=cpu,host=serverA,region=us-east str="string val" 1000000000`,
			pt:   models.MustNewPoint("mm", models.NewTags(map[string]string{"_m": "cpu", "_f": "str", "host": "serverA", "region": "us-east"}), models.Fields{"str": "string val"}, time.Unix(1, 0)),
		},
	} {
		pts, err := models.ParsePoints([]byte(tt.line), []byte("mm"))
		if err != nil {
			t.Fatalf(`%d. ParsePoints() failed. got %s`, i, err)
		}
		if exp := 1; len(pts) != exp {
			t.Errorf("%d. ParsePoint() len mismatch: got %v, exp %v", i, len(pts), exp)
		}
		pt := pts[0]

		got := pt.String()
		if tt.exp != got {
			t.Errorf("%d. ParsePoint() to string mismatch:\n got %v\n exp %v", i, got, tt.exp)
		}

		got = tt.pt.String()
		if tt.exp != got {
			t.Errorf("%d. NewPoint() to string mismatch:\n got %v\n exp %v", i, got, tt.exp)
		}
	}
}

func TestParsePointsWithPrecision(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		precision string
		exp       string
	}{
		{
			name:      "nanosecond by default",
			line:      `cpu,host=serverA,region=us-east value=1.0 946730096789012345`,
			precision: "",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
		},
		{
			name:      "nanosecond",
			line:      `cpu,host=serverA,region=us-east value=1.0 946730096789012345`,
			precision: "ns",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
		},
		{
			name:      "microsecond",
			line:      `cpu,host=serverA,region=us-east value=1.0 946730096789012`,
			precision: "us",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012000",
		},
		{
			name:      "millisecond",
			line:      `cpu,host=serverA,region=us-east value=1.0 946730096789`,
			precision: "ms",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789000000",
		},
		{
			name:      "second",
			line:      `cpu,host=serverA,region=us-east value=1.0 946730096`,
			precision: "s",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096000000000",
		},
	}
	for _, test := range tests {
		pts, err := models.ParsePointsWithPrecision([]byte(test.line), []byte("mm"), time.Now().UTC(), test.precision)
		if err != nil {
			t.Fatalf(`%s: ParsePoints() failed. got %s`, test.name, err)
		}
		if exp := 1; len(pts) != exp {
			t.Errorf("%s: ParsePoint() len mismatch: got %v, exp %v", test.name, len(pts), exp)
		}
		pt := pts[0]

		got := pt.String()
		if got != test.exp {
			t.Errorf("%s: ParsePoint() to string mismatch:\n got %v\n exp %v", test.name, got, test.exp)
		}
	}
}

func TestParsePointsWithPrecisionNoTime(t *testing.T) {
	line := `cpu,host=serverA,region=us-east value=1.0`
	tm, _ := time.Parse(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z")
	tests := []struct {
		name      string
		precision string
		exp       string
	}{
		{
			name:      "no precision",
			precision: "",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
		},
		{
			name:      "nanosecond precision",
			precision: "ns",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
		},
		{
			name:      "microsecond precision",
			precision: "us",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012000",
		},
		{
			name:      "millisecond precision",
			precision: "ms",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789000000",
		},
		{
			name:      "second precision",
			precision: "s",
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096000000000",
		},
	}

	for _, test := range tests {
		pts, err := models.ParsePointsWithPrecision([]byte(line), []byte("mm"), tm, test.precision)
		if err != nil {
			t.Fatalf(`%s: ParsePoints() failed. got %s`, test.name, err)
		}
		if exp := 1; len(pts) != exp {
			t.Errorf("%s: ParsePoint() len mismatch: got %v, exp %v", test.name, len(pts), exp)
		}
		pt := pts[0]

		got := pt.String()
		if got != test.exp {
			t.Errorf("%s: ParsePoint() to string mismatch:\n got %v\n exp %v", test.name, got, test.exp)
		}
	}
}

func TestParsePointsWithPrecisionComments(t *testing.T) {
	tests := []struct {
		name      string
		batch     string
		exp       string
		lenPoints int
	}{
		{
			name:      "comment only",
			batch:     `# comment only`,
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
			lenPoints: 0,
		},
		{
			name: "point with comment above",
			batch: `# a point is below
cpu,host=serverA,region=us-east value=1.0 946730096789012345`,
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
			lenPoints: 1,
		},
		{
			name: "point with comment below",
			batch: `cpu,host=serverA,region=us-east value=1.0 946730096789012345
# end of points`,
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
			lenPoints: 1,
		},
		{
			name: "indented comment",
			batch: `	# a point is below
cpu,host=serverA,region=us-east value=1.0 946730096789012345`,
			exp:       "mm,_f=value,_m=cpu,host=serverA,region=us-east value=1.0 946730096789012345",
			lenPoints: 1,
		},
	}
	for _, test := range tests {
		pts, err := models.ParsePointsWithPrecision([]byte(test.batch), []byte("mm"), time.Now().UTC(), "")
		if err != nil {
			t.Fatalf(`%s: ParsePoints() failed. got %s`, test.name, err)
		}
		pointsLength := len(pts)
		if exp := test.lenPoints; pointsLength != exp {
			t.Errorf("%s: ParsePoint() len mismatch: got %v, exp %v", test.name, pointsLength, exp)
		}

		if pointsLength > 0 {
			pt := pts[0]

			got := pt.String()
			if got != test.exp {
				t.Errorf("%s: ParsePoint() to string mismatch:\n got %v\n exp %v", test.name, got, test.exp)
			}
		}
	}
}

func TestNewPointEscaped(t *testing.T) {
	// commas
	pt := models.MustNewPoint("cpu,main", models.NewTags(map[string]string{"tag,bar": "value"}), models.Fields{"name,bar": 1.0}, time.Unix(0, 0))
	if exp := `cpu\,main,tag\,bar=value name\,bar=1 0`; pt.String() != exp {
		t.Errorf("NewPoint().String() mismatch.\ngot %v\nexp %v", pt.String(), exp)
	}

	// spaces
	pt = models.MustNewPoint("cpu main", models.NewTags(map[string]string{"tag bar": "value"}), models.Fields{"name bar": 1.0}, time.Unix(0, 0))
	if exp := `cpu\ main,tag\ bar=value name\ bar=1 0`; pt.String() != exp {
		t.Errorf("NewPoint().String() mismatch.\ngot %v\nexp %v", pt.String(), exp)
	}

	// equals
	pt = models.MustNewPoint("cpu=main", models.NewTags(map[string]string{"tag=bar": "value=foo"}), models.Fields{"name=bar": 1.0}, time.Unix(0, 0))
	if exp := `cpu=main,tag\=bar=value\=foo name\=bar=1 0`; pt.String() != exp {
		t.Errorf("NewPoint().String() mismatch.\ngot %v\nexp %v", pt.String(), exp)
	}
}

func TestNewPointWithoutField(t *testing.T) {
	_, err := models.NewPoint("cpu", models.NewTags(map[string]string{"tag": "bar"}), models.Fields{}, time.Unix(0, 0))
	if err == nil {
		t.Fatalf(`NewPoint() expected error. got nil`)
	}
}

func TestNewPointUnhandledType(t *testing.T) {
	// nil value
	pt := models.MustNewPoint("cpu", nil, models.Fields{"value": nil}, time.Unix(0, 0))
	if exp := `cpu value= 0`; pt.String() != exp {
		t.Errorf("NewPoint().String() mismatch.\ngot %v\nexp %v", pt.String(), exp)
	}

	// unsupported type gets stored as string
	now := time.Unix(0, 0).UTC()
	pt = models.MustNewPoint("cpu", nil, models.Fields{"value": now}, time.Unix(0, 0))
	if exp := `cpu value="1970-01-01 00:00:00 +0000 UTC" 0`; pt.String() != exp {
		t.Errorf("NewPoint().String() mismatch.\ngot %v\nexp %v", pt.String(), exp)
	}

	fields, err := pt.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if exp := "1970-01-01 00:00:00 +0000 UTC"; fields["value"] != exp {
		t.Errorf("NewPoint().String() mismatch.\ngot %v\nexp %v", pt.String(), exp)
	}
}

func TestMakeKeyEscaped(t *testing.T) {
	if exp, got := `cpu\ load`, models.MakeKey([]byte(`cpu\ load`), models.NewTags(map[string]string{})); string(got) != exp {
		t.Errorf("MakeKey() mismatch.\ngot %v\nexp %v", got, exp)
	}

	if exp, got := `cpu\ load`, models.MakeKey([]byte(`cpu load`), models.NewTags(map[string]string{})); string(got) != exp {
		t.Errorf("MakeKey() mismatch.\ngot %v\nexp %v", got, exp)
	}

	if exp, got := `cpu\,load`, models.MakeKey([]byte(`cpu\,load`), models.NewTags(map[string]string{})); string(got) != exp {
		t.Errorf("MakeKey() mismatch.\ngot %v\nexp %v", got, exp)
	}

	if exp, got := `cpu\,load`, models.MakeKey([]byte(`cpu,load`), models.NewTags(map[string]string{})); string(got) != exp {
		t.Errorf("MakeKey() mismatch.\ngot %v\nexp %v", got, exp)
	}

}

func TestPrecisionString(t *testing.T) {
	tags := map[string]interface{}{"value": float64(1)}
	tm, _ := time.Parse(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z")
	tests := []struct {
		name      string
		precision string
		exp       string
	}{
		{
			name:      "no precision",
			precision: "",
			exp:       "cpu value=1 946730096789012345",
		},
		{
			name:      "nanosecond precision",
			precision: "ns",
			exp:       "cpu value=1 946730096789012345",
		},
		{
			name:      "microsecond precision",
			precision: "us",
			exp:       "cpu value=1 946730096789012",
		},
		{
			name:      "millisecond precision",
			precision: "ms",
			exp:       "cpu value=1 946730096789",
		},
		{
			name:      "second precision",
			precision: "s",
			exp:       "cpu value=1 946730096",
		},
	}

	for _, test := range tests {
		pt := models.MustNewPoint("cpu", nil, tags, tm)
		act := pt.PrecisionString(test.precision)

		if act != test.exp {
			t.Errorf("%s: PrecisionString() mismatch:\n actual:	%v\n exp:		%v",
				test.name, act, test.exp)
		}
	}
}

func TestRoundedString(t *testing.T) {
	tags := map[string]interface{}{"value": float64(1)}
	tm, _ := time.Parse(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z")
	tests := []struct {
		name      string
		precision time.Duration
		exp       string
	}{
		{
			name:      "no precision",
			precision: time.Duration(0),
			exp:       "cpu value=1 946730096789012345",
		},
		{
			name:      "nanosecond precision",
			precision: time.Nanosecond,
			exp:       "cpu value=1 946730096789012345",
		},
		{
			name:      "microsecond precision",
			precision: time.Microsecond,
			exp:       "cpu value=1 946730096789012000",
		},
		{
			name:      "millisecond precision",
			precision: time.Millisecond,
			exp:       "cpu value=1 946730096789000000",
		},
		{
			name:      "second precision",
			precision: time.Second,
			exp:       "cpu value=1 946730097000000000",
		},
	}

	for _, test := range tests {
		pt := models.MustNewPoint("cpu", nil, tags, tm)
		act := pt.RoundedString(test.precision)

		if act != test.exp {
			t.Errorf("%s: RoundedString() mismatch:\n actual:	%v\n exp:		%v",
				test.name, act, test.exp)
		}
	}
}

func TestParsePointsStringWithExtraBuffer(t *testing.T) {
	b := make([]byte, 70*5000)
	buf := bytes.NewBuffer(b)
	buf.WriteString(fmt.Sprintf("%s value=%.3f 1\n", "cpu,host=A,region=uswest", rand.Float64()))

	points, err := models.ParsePointsString(buf.String(), "mm")
	if err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	pointKey := string(points[0].Key())
	exp := `mm,_f=value,_m=cpu,host=A,region=uswest`
	if exp != pointKey {
		t.Fatalf("unexpected key: got %s, exp %s", pointKey, exp)
	}
}

func TestParsePointsQuotesInFieldKey(t *testing.T) {
	buf := `cpu "a=1
cpu value=2 1`
	points, err := models.ParsePointsString(buf, "mm")
	if err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	fields, err := points[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	value, ok := fields["\"a"]
	if !ok {
		t.Fatalf("expected to parse field '\"a'")
	}

	if value != float64(1) {
		t.Fatalf("expected field value to be 1, got %v", value)
	}

	// The following input should not parse
	buf = `cpu "\, '= "\ v=1.0`
	_, err = models.ParsePointsString(buf, "mm")
	if err == nil {
		t.Fatalf("expected parsing failure but got no error")
	}
}

func TestParsePointsQuotesInTags(t *testing.T) {
	buf := `t159,label=hey\ "ya a=1i,value=0i
t159,label=another a=2i,value=1i 1`
	points, err := models.ParsePointsString(buf, "mm")
	if err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if len(points) != 4 {
		t.Fatalf("expected 4 points, got %d", len(points))
	}
}

func TestParsePointsBlankLine(t *testing.T) {
	buf := `cpu value=1i 1000000000

cpu value=2i 2000000000`
	points, err := models.ParsePointsString(buf, "mm")
	if err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}
}

func TestNewPointsWithBytesWithCorruptData(t *testing.T) {
	corrupted := []byte{0, 0, 0, 3, 102, 111, 111, 0, 0, 0, 4, 61, 34, 65, 34, 1, 0, 0, 0, 14, 206, 86, 119, 24, 32, 72, 233, 168, 2, 148}
	p, err := models.NewPointFromBytes(corrupted)
	if p != nil || err == nil {
		t.Fatalf("NewPointFromBytes: got: (%v, %v), expected: (nil, error)", p, err)
	}
}

func TestNewPointsWithShortBuffer(t *testing.T) {
	_, err := models.NewPointFromBytes([]byte{0, 0, 0, 3, 4})
	if err != io.ErrShortBuffer {
		t.Fatalf("NewPointFromBytes: got: (%v, %v), expected: (nil, error)", p, err)
	}
}

func TestNewPointsRejectsEmptyFieldNames(t *testing.T) {
	if _, err := models.NewPoint("foo", nil, models.Fields{"": 1}, time.Now()); err == nil {
		t.Fatalf("new point with empty field name. got: nil, expected: error")
	}
}

func TestNewPointsRejectsMaxKey(t *testing.T) {
	name := "mm"
	key := strings.Repeat("a", models.MaxKeyLength-len("mm,_f=value,_m=")-len("#!~#value"))

	// Test max key len
	if _, err := models.NewPoint(name, models.NewTags(map[string]string{"_f": "value", "_m": key}), models.Fields{"value": 1}, time.Now()); err != nil {
		t.Fatalf("new point with max key. got: %v, expected: nil", err)
	}

	if _, err := models.ParsePointsString(fmt.Sprintf("%v value=1", key), name); err != nil {
		t.Fatalf("parse point with max key. got: %v, expected: nil", err)
	}

	// Test 1 byte over max key len
	key += "a"
	if _, err := models.NewPoint(name, models.NewTags(map[string]string{"_f": "value", "_m": key}), models.Fields{"value": 1}, time.Now()); err == nil {
		t.Fatalf("new point with max key. got: nil, expected: error")
	}

	if _, err := models.ParsePointsString(fmt.Sprintf("%v value=1", key), name); err == nil {
		t.Fatalf("parse point with max key. got: nil, expected: error")
	}

}

func TestPoint_FieldIterator_Simple(t *testing.T) {
	p, err := models.ParsePoints([]byte(`m v=42i,f=42 36`), []byte("mm"))
	if err != nil {
		t.Fatal(err)
	}

	if len(p) != 2 {
		t.Fatalf("wrong number of points, got %d, exp %d", len(p), 1)
	}

	if fi := p[0].FieldIterator(); !fi.Next() {
		t.Fatal("field iterator terminated before first field of first point")
	} else if fi.Type() != models.Integer {
		t.Fatalf("'42i' should be an Integer, got %v", fi.Type())
	} else if iv, err := fi.IntegerValue(); err != nil {
		t.Fatal(err)
	} else if exp, got := int64(42), iv; exp != got {
		t.Fatalf("'42i' should be %d, got %d", exp, got)
	} else if fi.Next() {
		t.Fatal("field iterator didn't terminate")
	}

	if fi := p[1].FieldIterator(); !fi.Next() {
		t.Fatalf("field iterator terminated before first field of second point")
	} else if fi.Type() != models.Float {
		t.Fatalf("'42' should be a Float, got %v", fi.Type())
	} else if fv, err := fi.FloatValue(); err != nil {
		t.Fatal(err)
	} else if exp, got := 42.0, fv; exp != got {
		t.Fatalf("'42' should be %f, got %f", exp, got)
	} else if fi.Next() {
		t.Fatal("field iterator didn't terminate")
	}
}

func toFields(fi models.FieldIterator) models.Fields {
	m := make(models.Fields)
	for fi.Next() {
		var v interface{}
		var err error
		switch fi.Type() {
		case models.Float:
			v, err = fi.FloatValue()
		case models.Integer:
			v, err = fi.IntegerValue()
		case models.Unsigned:
			v, err = fi.UnsignedValue()
		case models.String:
			v = fi.StringValue()
		case models.Boolean:
			v, err = fi.BooleanValue()
		case models.Empty:
			v = nil
		default:
			panic("unknown type")
		}
		if err != nil {
			panic(err)
		}
		m[string(fi.FieldKey())] = v
	}
	return m
}

func TestPoint_FieldIterator_FieldMap(t *testing.T) {
	points, err := models.ParsePointsString(`
m v=42
m v=42i
m v="string"
m v=true
m v="string\"with\"escapes"
m v=42i,f=42,g=42.314,u=123u
m a=2i,b=3i,c=true,d="stuff",e=-0.23,f=123.456
`, "mm")

	if err != nil {
		t.Fatal("failed to parse test points:", err)
	}

	for _, p := range points {
		exp, err := p.Fields()
		if err != nil {
			t.Fatal(err)
		}
		got := toFields(p.FieldIterator())

		if !reflect.DeepEqual(got, exp) {
			t.Errorf("FieldIterator failed for %#q: got %#v, exp %#v", p.String(), got, exp)
		}
	}
}

func TestEscapeStringField(t *testing.T) {
	cases := []struct {
		in     string
		expOut string
	}{
		{in: "abcdefg", expOut: "abcdefg"},
		{in: `one double quote " .`, expOut: `one double quote \" .`},
		{in: `quote " then backslash \ .`, expOut: `quote \" then backslash \\ .`},
		{in: `backslash \ then quote " .`, expOut: `backslash \\ then quote \" .`},
	}

	for _, c := range cases {
		// Unescapes as expected.
		got := models.EscapeStringField(c.in)
		if got != c.expOut {
			t.Errorf("unexpected result from EscapeStringField(%s)\ngot [%s]\nexp [%s]\n", c.in, got, c.expOut)
			continue
		}

		pointLine := fmt.Sprintf(`t s="%s"`, got)
		testParsePoints(t, pointLine, "mm", NewTestPoint(
			"mm",
			models.NewTags(map[string]string{"_f": "s", "_m": "t"}),
			models.Fields{"s": c.in},
			time.Unix(0, 0),
		))
	}
}

func TestParseKeyBytes(t *testing.T) {
	testCases := []struct {
		input        string
		expectedName string
		expectedTags map[string]string
	}{
		{input: "m,k=v", expectedName: "m", expectedTags: map[string]string{"k": "v"}},
		{input: "m\\ q,k=v", expectedName: "m q", expectedTags: map[string]string{"k": "v"}},
		{input: "m,k\\ q=v", expectedName: "m", expectedTags: map[string]string{"k q": "v"}},
		{input: "m\\ q,k\\ q=v", expectedName: "m q", expectedTags: map[string]string{"k q": "v"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			name, tags := models.ParseKeyBytes([]byte(testCase.input))
			if !bytes.Equal([]byte(testCase.expectedName), name) {
				t.Errorf("%s produced measurement %s but expected %s", testCase.input, string(name), testCase.expectedName)
			}
			if !tags.Equal(models.NewTags(testCase.expectedTags)) {
				t.Errorf("%s produced tags %s but expected %s", testCase.input, tags.String(), models.NewTags(testCase.expectedTags).String())
			}
		})
	}
}

func TestParseName(t *testing.T) {
	testCases := []struct {
		input        string
		expectedName string
	}{
		{input: "m,k=v", expectedName: "m"},
		{input: "m\\ q,k=v", expectedName: "m q"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			name := models.ParseName([]byte(testCase.input))
			if !bytes.Equal([]byte(testCase.expectedName), name) {
				t.Errorf("%s produced measurement %s but expected %s", testCase.input, string(name), testCase.expectedName)
			}
		})
	}
}

func BenchmarkEscapeStringField_Plain(b *testing.B) {
	s := "nothing special"
	for i := 0; i < b.N; i++ {
		sink = models.EscapeStringField(s)
	}
}

func BenchmarkEscapeString_Quotes(b *testing.B) {
	s := `Hello, "world"`
	for i := 0; i < b.N; i++ {
		sink = models.EscapeStringField(s)
	}
}

func BenchmarkEscapeString_Backslashes(b *testing.B) {
	s := `C:\windows\system32`
	for i := 0; i < b.N; i++ {
		sink = models.EscapeStringField(s)
	}
}

func BenchmarkEscapeString_QuotesAndBackslashes(b *testing.B) {
	s1 := `a quote " then backslash \ .`
	s2 := `a backslash \ then quote " .`
	for i := 0; i < b.N; i++ {
		sink = [...]string{models.EscapeStringField(s1), models.EscapeStringField(s2)}
	}
}

func BenchmarkParseTags(b *testing.B) {
	tags := []byte("cpu,tag0=value0,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5")
	for i := 0; i < b.N; i++ {
		models.ParseTags(tags)
	}
}

func BenchmarkEscapeMeasurement(b *testing.B) {
	benchmarks := []struct {
		m []byte
	}{
		{[]byte("this_is_a_test")},
		{[]byte("this,is,a,test")},
	}

	for _, bm := range benchmarks {
		b.Run(string(bm.m), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				models.EscapeMeasurement(bm.m)
			}
		})
	}
}

func makeTags(key, val string, n int) models.Tags {
	tags := make(models.Tags, n)
	for i := range tags {
		tags[i].Key = []byte(fmt.Sprintf("%s%03d", key, i))
		tags[i].Value = []byte(fmt.Sprintf("%s%03d", val, i))
	}
	return tags
}

func BenchmarkTags_HashKey(b *testing.B) {
	benchmarks := []struct {
		name string
		t    models.Tags
	}{
		{"5 tags-no esc", makeTags("tag_foo", "val_bar", 5)},
		{"25 tags-no esc", makeTags("tag_foo", "val_bar", 25)},
		{"5 tags-esc", makeTags("tag foo", "val bar", 5)},
		{"25 tags-esc", makeTags("tag foo", "val bar", 25)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bm.t.HashKey()
			}
		})
	}
}

func BenchmarkMakeKey(b *testing.B) {
	benchmarks := []struct {
		m []byte
		t models.Tags
	}{
		{[]byte("this_is_a_test"), nil},
		{[]byte("this,is,a,test"), nil},
		{[]byte(`this\ is\ a\ test`), nil},

		{[]byte("this_is_a_test"), makeTags("tag_foo", "val_bar", 8)},
		{[]byte("this,is,a,test"), makeTags("tag_foo", "val_bar", 8)},
		{[]byte("this_is_a_test"), makeTags("tag_foo", "val bar", 8)},
		{[]byte("this,is,a,test"), makeTags("tag_foo", "val bar", 8)},
	}

	for _, bm := range benchmarks {
		b.Run(string(bm.m), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				models.MakeKey(bm.m, bm.t)
			}
		})
	}
}
