package models_test

import (
	"encoding/json"
	"testing"
	"unsafe"

	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/require"
)

func TestRow_SameSeries(t *testing.T) {
	year := models.GroupingKey(1)
	month := models.GroupingKey(3)
	for _, tt := range []struct {
		name string
		a, b models.Row
		want bool
	}{
		{
			name: "same name and tags",
			a:    models.Row{Name: "cpu", Tags: map[string]string{"host": "a"}},
			b:    models.Row{Name: "cpu", Tags: map[string]string{"host": "a"}},
			want: true,
		},
		{
			name: "different name",
			a:    models.Row{Name: "cpu"},
			b:    models.Row{Name: "mem"},
			want: false,
		},
		{
			name: "different tags",
			a:    models.Row{Name: "cpu", Tags: map[string]string{"host": "a"}},
			b:    models.Row{Name: "cpu", Tags: map[string]string{"host": "b"}},
			want: false,
		},
		{
			name: "same grouping key",
			a:    models.Row{Name: "cpu", GroupingKey: year},
			b:    models.Row{Name: "cpu", GroupingKey: year},
			want: true,
		},
		{
			name: "different grouping keys",
			a:    models.Row{Name: "cpu", GroupingKey: year},
			b:    models.Row{Name: "cpu", GroupingKey: month},
			want: false,
		},
		{
			name: "grouping key only on one side",
			a:    models.Row{Name: "cpu", GroupingKey: year},
			b:    models.Row{Name: "cpu"},
			want: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.a.SameSeries(&tt.b))
			require.Equal(t, tt.want, tt.b.SameSeries(&tt.a), "SameSeries must be symmetric")
		})
	}
}

// The grouping key must not grow Row: queries that do not group by date_part
// can return millions of rows.
func TestRow_GroupingKeyAddsNoSize(t *testing.T) {
	type rowWithoutGroupingKey struct {
		Name    string
		Tags    map[string]string
		Columns []string
		Values  [][]interface{}
		Partial bool
	}
	require.Equal(t, unsafe.Sizeof(rowWithoutGroupingKey{}), unsafe.Sizeof(models.Row{}))
}

func TestRow_GroupingKeyJSON(t *testing.T) {
	b, err := json.Marshal(models.Row{Name: "cpu", GroupingKey: models.GroupingKey(3)})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"cpu","grouping_keys":["month"]}`, string(b))

	// Omitted entirely for a row without a grouping key.
	b, err = json.Marshal(models.Row{Name: "cpu"})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"cpu"}`, string(b))

	var r models.Row
	require.NoError(t, json.Unmarshal([]byte(`{"name":"cpu","grouping_keys":["month"]}`), &r))
	require.Equal(t, "month", r.GroupingKey.String())

	// A part this client does not know decodes to no key instead of failing.
	r = models.Row{}
	require.NoError(t, json.Unmarshal([]byte(`{"name":"cpu","grouping_keys":["fortnight"]}`), &r))
	require.Zero(t, r.GroupingKey)
}
