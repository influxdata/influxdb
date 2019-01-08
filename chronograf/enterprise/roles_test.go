package enterprise

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
)

func TestRoles_ToChronograf(t *testing.T) {
	tests := []struct {
		name  string
		roles []Role
		want  []chronograf.Role
	}{
		{
			name:  "empty roles",
			roles: []Role{},
			want:  []chronograf.Role{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Roles{
				Roles: tt.roles,
			}
			if got := r.ToChronograf(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Roles.ToChronograf() = %v, want %v", got, tt.want)
			}
		})
	}
}
