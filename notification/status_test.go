package notification

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
)

func TestStatusJSON(t *testing.T) {
	cases := []struct {
		name   string
		src    StatusRule
		target StatusRule
	}{
		{
			name: "regular status rule",
			src: StatusRule{
				CurrentLevel:  LevelRule{Operation: true, CheckLevel: Warn},
				PreviousLevel: &LevelRule{Operation: false, CheckLevel: Critical},
				Count:         3,
				Period:        influxdb.Duration{Duration: time.Minute * 13},
			},
			target: StatusRule{
				CurrentLevel:  LevelRule{Operation: true, CheckLevel: Warn},
				PreviousLevel: &LevelRule{Operation: false, CheckLevel: Critical},
				Count:         3,
				Period:        influxdb.Duration{Duration: time.Minute * 13},
			},
		},
		{
			name:   "empty",
			src:    StatusRule{},
			target: StatusRule{},
		},
		{
			name: "invalid status",
			src: StatusRule{
				CurrentLevel: LevelRule{CheckLevel: CheckLevel(-10)},
			},
			target: StatusRule{
				CurrentLevel: LevelRule{CheckLevel: Unknown},
			},
		},
	}
	for _, c := range cases {
		serialized, err := json.Marshal(c.src)
		if err != nil {
			t.Errorf("%s marshal failed, err: %s", c.name, err)
		}
		var got StatusRule
		err = json.Unmarshal(serialized, &got)
		if err != nil {
			t.Errorf("%s unmarshal failed, err: %s", c.name, err)
		}
		if diff := cmp.Diff(got, c.target); diff != "" {
			t.Errorf("status rules are different -got/+want\ndiff %s", diff)
		}
	}
}
