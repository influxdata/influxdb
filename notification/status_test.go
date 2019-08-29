package notification

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func checkLvlPtr(l CheckLevel) *CheckLevel {
	return &l
}

func TestStatusJSON(t *testing.T) {
	cases := []struct {
		name   string
		src    StatusRule
		target StatusRule
	}{
		{
			name: "regular status rule",
			src: StatusRule{
				CurrentLevel:  Warn,
				PreviousLevel: checkLvlPtr(Critical),
			},
			target: StatusRule{
				CurrentLevel:  Warn,
				PreviousLevel: checkLvlPtr(Critical),
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
				CurrentLevel: CheckLevel(-10),
			},
			target: StatusRule{
				CurrentLevel: Unknown,
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
