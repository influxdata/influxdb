package tsdb

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

var tags = Tags{"foo": "bar", "apple": "orange", "host": "serverA", "region": "uswest"}

func TestMarshal(t *testing.T) {
	got := tags.hashKey()
	if exp := ",apple=orange,foo=bar,host=serverA,region=uswest"; string(got) != exp {
		t.Log("got: ", string(got))
		t.Log("exp: ", exp)
		t.Error("invalid match")
	}
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tags.hashKey()
	}
}

func BenchmarkParsePointNoTags(b *testing.B) {
	line := `cpu value=1 1000000000`
	for i := 0; i < b.N; i++ {
		ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointTagsSorted2(b *testing.B) {
	line := `cpu,host=serverA,region=us-west value=1 1000000000`
	for i := 0; i < b.N; i++ {
		ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointTagsSorted5(b *testing.B) {
	line := `cpu,env=prod,host=serverA,region=us-west,target=servers,zone=1c value=1 1000000000`
	for i := 0; i < b.N; i++ {
		ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointTagsSorted10(b *testing.B) {
	line := `cpu,env=prod,host=serverA,region=us-west,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5,target=servers,zone=1c value=1 1000000000`
	for i := 0; i < b.N; i++ {
		ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
	}
}

func BenchmarkParsePointTagsUnSorted2(b *testing.B) {
	line := `cpu,region=us-west,host=serverA value=1 1000000000`
	for i := 0; i < b.N; i++ {
		pt, _ := ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
		pt.Key()
	}
}

func BenchmarkParsePointTagsUnSorted5(b *testing.B) {
	line := `cpu,region=us-west,host=serverA,env=prod,target=servers,zone=1c value=1 1000000000`
	for i := 0; i < b.N; i++ {
		pt, _ := ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
		pt.Key()
	}
}

func BenchmarkParsePointTagsUnSorted10(b *testing.B) {
	line := `cpu,region=us-west,host=serverA,env=prod,target=servers,zone=1c,tag1=value1,tag2=value2,tag3=value3,tag4=value4,tag5=value5 value=1 1000000000`
	for i := 0; i < b.N; i++ {
		pt, _ := ParsePoint([]byte(line))
		b.SetBytes(int64(len(line)))
		pt.Key()
	}
}

func test(t *testing.T, line string, point Point) {
	pts, err := ParsePointsString(line)
	if err != nil {
		t.Fatalf(`ParsePoints("%s") mismatch. got %v, exp nil`, line, err)
	}

	if exp := 1; len(pts) != exp {
		t.Fatalf(`ParsePoints("%s") len mismatch. got %d, exp %d`, line, len(pts), exp)
	}

	if exp := point.Key(); !bytes.Equal(pts[0].Key(), exp) {
		t.Errorf("ParsePoints(\"%s\") key mismatch.\ngot %v\nexp %v", line, string(pts[0].Key()), string(exp))
	}

	if exp := len(point.Tags()); len(pts[0].Tags()) != exp {
		t.Errorf(`ParsePoints("%s") tags mismatch. got %v, exp %v`, line, pts[0].Tags(), exp)
	}

	for tag, value := range point.Tags() {
		if pts[0].Tags()[tag] != value {
			t.Errorf(`ParsePoints("%s") tags mismatch. got %v, exp %v`, line, pts[0].Tags()[tag], value)
		}
	}

	for name, value := range point.Fields() {
		if !reflect.DeepEqual(pts[0].Fields()[name], value) {
			t.Errorf(`ParsePoints("%s") field '%s' mismatch. got %v, exp %v`, line, name, pts[0].Fields()[name], value)
		}
	}

	if !pts[0].Time().Equal(point.Time()) {
		t.Errorf(`ParsePoints("%s") time mismatch. got %v, exp %v`, line, pts[0].Time(), point.Time())
	}

	if line != pts[0].String() {
		t.Errorf("ParsePoints string mismatch.\ngot: %v\nexp: %v", pts[0].String(), line)
	}
}

func TestParsePointNoValue(t *testing.T) {
	_, err := ParsePointsString("")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, "")
	}
}

func TestParsePointNoFields(t *testing.T) {
	_, err := ParsePointsString("cpu")
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, "cpu")
	}
}

func TestParsePointNoTimestamp(t *testing.T) {
	test(t, "cpu value=1", NewPoint("cpu", nil, nil, time.Time{}))
}

func TestParsePointMissingQuote(t *testing.T) {
	_, err := ParsePointsString(`cpu,host=serverA value="test`)
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, "cpu")
	}
}

func TestParsePointMissingTagValue(t *testing.T) {
	_, err := ParsePointsString(`cpu,host=serverA,region value=1`)
	if err == nil {
		t.Errorf(`ParsePoints("%s") mismatch. got nil, exp error`, "cpu")
	}
}

func TestParsePointUnescape(t *testing.T) {
	// commas in tag values
	test(t, `cpu,regions=east\,west value=1.0`,
		NewPoint("cpu",
			Tags{
				"regions": "east,west", // comma in the tag value
			},
			Fields{
				"value": 1.0,
			},
			time.Time{}))

	// commas in measuremnt name
	test(t, `cpu\,main,regions=east\,west value=1.0`,
		NewPoint(
			"cpu,main", // comma in the name
			Tags{
				"regions": "east,west",
			},
			Fields{
				"value": 1.0,
			},
			time.Time{}))

	// random character escaped
	test(t, `cpu,regions=eas\t value=1.0`,
		NewPoint(
			"cpu",
			Tags{
				"regions": "eas\\t",
			},
			Fields{
				"value": 1.0,
			},
			time.Time{}))
}

func TestParsePointWithTags(t *testing.T) {
	test(t,
		"cpu,host=serverA,region=us-east value=1.0 1000000000",
		NewPoint("cpu",
			Tags{"host": "serverA", "region": "us-east"},
			Fields{"value": 1.0}, time.Unix(1, 0)))
}

func TestParsPointWithDuplicateTags(t *testing.T) {
	_, err := ParsePoint([]byte(`cpu,host=serverA,host=serverB value=1 1000000000`))
	if err == nil {
		t.Fatalf(`ParsePoint() expected error. got nil`)
	}
}

func TestParsePointWithStringField(t *testing.T) {
	test(t, `cpu,host=serverA,region=us-east value=1.0,str="foo",str2="bar" 1000000000`,
		NewPoint("cpu",
			Tags{
				"host":   "serverA",
				"region": "us-east",
			},
			Fields{
				"value": 1.0,
				"str":   "foo",
				"str2":  "bar",
			},
			time.Unix(1, 0)),
	)

	test(t, `cpu,host=serverA,region=us-east str="foo \" bar" 1000000000`,
		NewPoint("cpu",
			Tags{
				"host":   "serverA",
				"region": "us-east",
			},
			Fields{
				"str": `foo " bar`,
			},
			time.Unix(1, 0)),
	)

}

func TestParsePointWithStringWithSpaces(t *testing.T) {
	test(t, `cpu,host=serverA,region=us-east value=1.0,str="foo bar" 1000000000`,
		NewPoint(
			"cpu",
			Tags{
				"host":   "serverA",
				"region": "us-east",
			},
			Fields{
				"value": 1.0,
				"str":   "foo bar", // spaces in string value
			},
			time.Unix(1, 0)),
	)

}

func TestParsePointWithBoolField(t *testing.T) {
	test(t, `cpu,host=serverA,region=us-east bool=true,boolTrue=t,false=false,falseVal=f 1000000000`,
		NewPoint(
			"cpu",
			Tags{
				"host":   "serverA",
				"region": "us-east",
			},
			Fields{
				"bool":     true,
				"boolTrue": true,
				"false":    false,
				"falseVal": false,
			},
			time.Unix(1, 0)),
	)
}

func TestParsePointIntsFloats(t *testing.T) {
	pt, err := ParsePoint([]byte(`cpu,host=serverA,region=us-east int=10,float=11.0,float2=12.1 1000000000`))
	if err != nil {
		t.Fatalf(`ParsePoints() failed. got %s`, err)
	}

	if _, ok := pt.Fields()["int"].(int64); !ok {
		t.Errorf("ParsePoint() int field mismatch: got %T, exp %T", pt.Fields()["int"], int64(10))
	}

	if _, ok := pt.Fields()["float"].(float64); !ok {
		t.Errorf("ParsePoint() float field mismatch: got %T, exp %T", pt.Fields()["float64"], float64(11.0))
	}

	if _, ok := pt.Fields()["float2"].(float64); !ok {
		t.Errorf("ParsePoint() float field mismatch: got %T, exp %T", pt.Fields()["float64"], float64(12.1))
	}

}

func TestParsePointKeyUnsorted(t *testing.T) {
	pt, err := ParsePoint([]byte("cpu,last=1,first=2 value=1"))
	if err != nil {
		t.Fatalf(`ParsePoints() failed. got %s`, err)
	}

	if exp := "cpu,first=2,last=1"; string(pt.Key()) != exp {
		t.Errorf("ParsePoint key not sorted. got %v, exp %v", pt.Key(), exp)
	}
}

func TestParsePointToString(t *testing.T) {
	line := `cpu,host=serverA,region=us-east bool=false,float=11.0,float2=12.123,int=10,str="string val" 1000000000`
	pt, err := ParsePoint([]byte(line))
	if err != nil {
		t.Fatalf(`ParsePoints() failed. got %s`, err)
	}

	got := pt.String()
	if line != got {
		t.Errorf("ParsePoint() to string mismatch:\n got %v\n exp %v", got, line)
	}

	pt = NewPoint("cpu", Tags{"host": "serverA", "region": "us-east"},
		Fields{"int": 10, "float": float64(11.0), "float2": float64(12.123), "bool": false, "str": "string val"},
		time.Unix(1, 0))

	got = pt.String()
	if line != got {
		t.Errorf("NewPoint() to string mismatch:\n got %v\n exp %v", got, line)
	}
}
