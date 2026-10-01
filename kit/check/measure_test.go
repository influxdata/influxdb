package check

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func shardsProgress(completed, total float64) Measure {
	return Measure{Unit: "shards", Values: map[string]float64{"completed": completed, "total": total}}
}

// TestMeasure_MarshalOrder pins the rendering: values in sorted key order,
// then unit, so two renders of the same measure are byte-identical.
func TestMeasure_MarshalOrder(t *testing.T) {
	b, err := json.Marshal(Measure{Unit: "seconds", Values: map[string]float64{"z": 1, "a": 2.5, "m": 0}})
	require.NoError(t, err)
	require.Equal(t, `{"a":2.5,"m":0,"z":1,"unit":"seconds"}`, string(b))
}

func TestMeasure_RoundTrip(t *testing.T) {
	for _, m := range []Measure{
		shardsProgress(94, 200),
		{Unit: UnitSeconds, Values: map[string]float64{"lag": 0.000123456789}},
		{Unit: UnitSeconds, Values: map[string]float64{"big": 1e21, "neg": -3}},
	} {
		first, err := json.Marshal(m)
		require.NoError(t, err)
		var decoded Measure
		require.NoError(t, json.Unmarshal(first, &decoded))
		require.Equal(t, m, decoded)
		second, err := json.Marshal(decoded)
		require.NoError(t, err)
		require.Equal(t, string(first), string(second))
	}
}

func TestMeasure_UnmarshalRejects(t *testing.T) {
	for name, doc := range map[string]string{
		"not an object":   `"shards"`,
		"null":            `null`,
		"array":           `[1,2]`,
		"missing unit":    `{"completed":1}`,
		"non-string unit": `{"completed":1,"unit":7}`,
		"string value":    `{"completed":"1","unit":"shards"}`,
		"object value":    `{"completed":{},"unit":"shards"}`,
		"null value":      `{"completed":null,"unit":"shards"}`,
		"null unit":       `{"completed":1,"unit":null}`,
		"truncated":       `{"completed":1,`,
	} {
		t.Run(name, func(t *testing.T) {
			var m Measure
			require.Error(t, json.Unmarshal([]byte(doc), &m))
		})
	}
}

// TestBasicResponse_NoMeasuresIsUnchanged pins that a response without
// measures renders exactly the bytes wireResponse always produced, which is
// what keeps every existing /health and /ready consumer unaffected.
func TestBasicResponse_NoMeasuresIsUnchanged(t *testing.T) {
	for _, r := range []BasicResponse{
		NamedPass("a"),
		NamedFail("b", "broken <&>"),
		NewBasicResponse("c", StatusFail, "m", Responses{NamedPass("d")}),
		BasicResponse{},
	} {
		got, err := json.Marshal(r)
		require.NoError(t, err)
		want, err := json.Marshal(r.wireResponse)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
	}
}

func TestBasicResponse_MarshalGroups(t *testing.T) {
	r := NamedFail("shards", "loading").
		WithMeasure("progress", shardsProgress(94, 200)).
		WithMeasure("failures", Measure{Unit: "shards", Values: map[string]float64{"count": 2}})
	b, err := json.Marshal(r)
	require.NoError(t, err)
	require.Equal(t,
		`{"name":"shards","status":"fail","message":"loading",`+
			`"failures":{"count":2,"unit":"shards"},`+
			`"progress":{"completed":94,"total":200,"unit":"shards"}}`,
		string(b))
}

// TestBasicResponse_RoundTrip is the marshaling round trip: marshal, decode,
// marshal again, and the bytes must be identical, nested checks included.
func TestBasicResponse_RoundTrip(t *testing.T) {
	nested := NamedFail("inner", "slow").
		WithMeasure("dispatch", Measure{Unit: UnitSeconds, Values: map[string]float64{"lag": 31.5}})
	for name, r := range map[string]BasicResponse{
		"no measures":   NamedFail("a", "msg"),
		"one group":     NamedFail("a", "msg").WithMeasure("progress", shardsProgress(1, 3)),
		"two groups":    Pass().WithMeasure("x", shardsProgress(1, 3)).WithMeasure("y", shardsProgress(2, 3)),
		"nested groups": NewBasicResponse("outer", StatusFail, "", Responses{nested}).WithMeasure("progress", shardsProgress(0, 0)),
	} {
		t.Run(name, func(t *testing.T) {
			first, err := json.Marshal(r)
			require.NoError(t, err)
			var decoded BasicResponse
			require.NoError(t, json.Unmarshal(first, &decoded))
			assertResponseEqual(t, r, decoded)
			second, err := json.Marshal(decoded)
			require.NoError(t, err)
			require.Equal(t, string(first), string(second))
		})
	}
}

// TestBasicResponse_UnmarshalIgnoresUnknown pins encoding/json's usual
// tolerance for unknown fields: QueryHealthCheck decodes a whole /health body,
// whose version and commit are strings, and an unknown object that is not
// measure-shaped is not an error either.
func TestBasicResponse_UnmarshalIgnoresUnknown(t *testing.T) {
	doc := `{"name":"influxdb","status":"pass","message":"healthy",
		"checks":[{"name":"shards","status":"pass","failures":{"count":0,"unit":"shards"}}],
		"version":"v2.9.0","commit":"abc123",
		"extra":{"nested":{"x":1}},
		"badunit":{"n":1,"unit":2},
		"list":[1,2,3],
		"progress":{"completed":5,"total":9,"unit":"shards"}}`
	var r BasicResponse
	require.NoError(t, json.Unmarshal([]byte(doc), &r))
	require.Equal(t, "influxdb", r.Name())
	require.Equal(t, Measures{"progress": shardsProgress(5, 9)}, r.Measures())
	require.Len(t, r.Checks(), 1)
	require.Equal(t,
		Measures{"failures": {Unit: "shards", Values: map[string]float64{"count": 0}}},
		r.Checks()[0].Measures())
}

func TestBasicResponse_UnmarshalRejectsBadFixedFields(t *testing.T) {
	var r BasicResponse
	require.Error(t, json.Unmarshal([]byte(`{"name":7,"status":"pass"}`), &r))
	require.Error(t, json.Unmarshal([]byte(`[]`), &r))
}

// TestWithMeasure_Copies pins that the caller's maps are not retained: a
// response may be served, or frozen, while the producer reuses its map.
func TestWithMeasure_Copies(t *testing.T) {
	m := shardsProgress(1, 2)
	r := Pass().WithMeasure("progress", m)
	m.Values["completed"] = 99
	m.Values["extra"] = 1
	require.Equal(t, shardsProgress(1, 2), r.Measures()["progress"])

	// Adding a group to a copy does not reach the original.
	r2 := r.WithMeasure("other", shardsProgress(3, 4))
	require.Len(t, r.Measures(), 1)
	require.Len(t, r2.Measures(), 2)

	// Replacing a group replaces it.
	r3 := r.WithMeasure("progress", shardsProgress(5, 6))
	require.Equal(t, shardsProgress(5, 6), r3.Measures()["progress"])
	require.Equal(t, shardsProgress(1, 2), r.Measures()["progress"])
}

func TestWithMeasure_DropsInvalid(t *testing.T) {
	valid := shardsProgress(1, 2)
	for name, tc := range map[string]struct {
		group string
		m     Measure
	}{
		"reserved name":      {"name", valid},
		"reserved status":    {"status", valid},
		"reserved message":   {"message", valid},
		"reserved checks":    {"checks", valid},
		"empty group":        {"", valid},
		"uppercase group":    {"Progress", valid},
		"punctuated group":   {"pro-gress", valid},
		"empty unit":         {"g", Measure{Values: valid.Values}},
		"bad unit":           {"g", Measure{Unit: "shards/s", Values: valid.Values}},
		"no values":          {"g", Measure{Unit: "shards"}},
		"only unit key":      {"g", Measure{Unit: "shards", Values: map[string]float64{"unit": 1}}},
		"only NaN":           {"g", Measure{Unit: "shards", Values: map[string]float64{"a": math.NaN()}}},
		"only +Inf and -Inf": {"g", Measure{Unit: "shards", Values: map[string]float64{"a": math.Inf(1), "b": math.Inf(-1)}}},
		"only bad keys":      {"g", Measure{Unit: "shards", Values: map[string]float64{"": 1, "A": 2, "a b": 3}}},
	} {
		t.Run(name, func(t *testing.T) {
			r := NamedPass("c").WithMeasure(tc.group, tc.m)
			require.Nil(t, r.Measures())
			b, err := json.Marshal(r)
			require.NoError(t, err)
			require.Equal(t, `{"name":"c","status":"pass"}`, string(b))
		})
	}

	t.Run("bad entries dropped, good kept", func(t *testing.T) {
		r := NamedPass("c").WithMeasure("g", Measure{Unit: "shards", Values: map[string]float64{
			"ok": 1, "nan": math.NaN(), "inf": math.Inf(1), "unit": 3, "Bad": 4,
		}})
		require.Equal(t, Measures{"g": {Unit: "shards", Values: map[string]float64{"ok": 1}}}, r.Measures())
		_, err := json.Marshal(r)
		require.NoError(t, err)
	})

	t.Run("invalid replacement keeps existing group", func(t *testing.T) {
		r := NamedPass("c").WithMeasure("g", valid).WithMeasure("g", Measure{Unit: "shards"})
		require.Equal(t, valid, r.Measures()["g"])
	})
}

// TestSnapshot_KeepsMeasures covers the flattening used by Freeze: groups on a
// BasicResponse, on one nested inside it, and on a FreshnessResponse reached
// through the renamedResponse wrapper all survive.
func TestSnapshot_KeepsMeasures(t *testing.T) {
	inner := NamedPass("inner").WithMeasure("progress", shardsProgress(1, 2))
	outer := NewBasicResponse("outer", StatusPass, "", Responses{inner}).
		WithMeasure("failures", Measure{Unit: "shards", Values: map[string]float64{"count": 0}})

	flat := snapshot(Rename(outer, "renamed"))
	require.Equal(t, "renamed", flat.Name())
	require.Equal(t, outer.Measures(), flat.Measures())
	require.Len(t, flat.Checks(), 1)
	require.Equal(t, inner.Measures(), flat.Checks()[0].Measures())

	f := NewFreshnessResponse("fresh", time.Hour)
	f.Update(inner)
	renamed := Rename(f, "wrapped")
	_, isWrapper := renamed.(renamedResponse)
	require.True(t, isWrapper, "a stateful Response must be wrapped, or this test proves nothing")
	require.Equal(t, inner.Measures(), renamed.Measures())
	require.Equal(t, inner.Measures(), snapshot(renamed).Measures())
}
