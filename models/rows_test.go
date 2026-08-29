package models_test

import (
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/require"
)

func TestRow_SameSeries(t *testing.T) {
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
			name: "same grouping keys",
			a:    models.Row{Name: "cpu", GroupingKeys: []string{"month", "year"}},
			b:    models.Row{Name: "cpu", GroupingKeys: []string{"month", "year"}},
			want: true,
		},
		{
			name: "different grouping keys",
			a:    models.Row{Name: "cpu", GroupingKeys: []string{"year"}},
			b:    models.Row{Name: "cpu", GroupingKeys: []string{"month"}},
			want: false,
		},
		{
			name: "grouping keys only on one side",
			a:    models.Row{Name: "cpu", GroupingKeys: []string{"year"}},
			b:    models.Row{Name: "cpu"},
			want: false,
		},
		{
			name: "grouping keys prefix of the other",
			a:    models.Row{Name: "cpu", GroupingKeys: []string{"year"}},
			b:    models.Row{Name: "cpu", GroupingKeys: []string{"month", "year"}},
			want: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.a.SameSeries(&tt.b))
			require.Equal(t, tt.want, tt.b.SameSeries(&tt.a), "SameSeries must be symmetric")
		})
	}
}
