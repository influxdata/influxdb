package migrator

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	influxtesting "github.com/influxdata/influxdb/testing"
)

func TestTelegrafConfigUp(t *testing.T) {
	cases := []struct {
		name string
		old  *OldTelegrafConfig
		new  *NewTelegrafConfig
		err  error
	}{
		{
			name: "newest version",
			old: &OldTelegrafConfig{
				Version: influxdb.NewestTelegrafConfig,
				ID:      influxtesting.MustIDBase16("020f755c3c082000"),
			},
			new: &NewTelegrafConfig{
				Version: influxdb.NewestTelegrafConfig,
				ID:      influxtesting.MustIDBase16("020f755c3c082000"),
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "This entry doesn't need to be converted.",
			},
		},
		{
			name: "new version unmarked",
			old: &OldTelegrafConfig{
				ID:    influxtesting.MustIDBase16("020f755c3c082000"),
				OrgID: influxtesting.MustIDBase16("020f755c3c082001"),
			},
			new: &NewTelegrafConfig{
				Version: influxdb.NewestTelegrafConfig,
				ID:      influxtesting.MustIDBase16("020f755c3c082000"),
				OrgID:   influxtesting.MustIDBase16("020f755c3c082001"),
			},
		},
		{
			name: "regular conversion",
			old: &OldTelegrafConfig{
				ID:             influxtesting.MustIDBase16("020f755c3c082000"),
				OrganizationID: influxtesting.MustIDBase16("020f755c3c082001"),
			},
			new: &NewTelegrafConfig{
				Version: influxdb.NewestTelegrafConfig,
				ID:      influxtesting.MustIDBase16("020f755c3c082000"),
				OrgID:   influxtesting.MustIDBase16("020f755c3c082001"),
			},
		},
	}
	for _, c := range cases {
		m := new(TelegrafConfigMigrator)
		src, _ := json.Marshal(c.old)
		dst, err := m.Up(src)
		influxtesting.ErrorsEqual(t, err, c.err)
		if c.new != nil {
			result := new(NewTelegrafConfig)
			_ = json.Unmarshal(dst, result)
			if diff := cmp.Diff(result, c.new); c.new != nil && diff != "" {
				t.Errorf("TelegrafConfig up are different -got/+want\ndiff %s", diff)
			}
		}
	}
}

func TestTelegrafConfigDown(t *testing.T) {
	cases := []struct {
		name string
		new  *NewTelegrafConfig
		old  *OldTelegrafConfig
		err  error
	}{
		{
			name: "regular conversion",
			old: &OldTelegrafConfig{
				ID:             influxtesting.MustIDBase16("020f755c3c082000"),
				OrganizationID: influxtesting.MustIDBase16("020f755c3c082001"),
			},
			new: &NewTelegrafConfig{
				Version: influxdb.NewestTelegrafConfig,
				ID:      influxtesting.MustIDBase16("020f755c3c082000"),
				OrgID:   influxtesting.MustIDBase16("020f755c3c082001"),
			},
		},
		{
			name: "bad org id",
			old:  nil,
			new: &NewTelegrafConfig{
				Version: influxdb.NewestTelegrafConfig,
				ID:      influxtesting.MustIDBase16("020f755c3c082000"),
			},
			err: &influxdb.Error{
				Code: influxdb.EInternal,
				Msg:  "This entry has a damaged data.",
			},
		},
	}
	for _, c := range cases {
		m := new(TelegrafConfigMigrator)
		src, _ := json.Marshal(c.new)
		dst, err := m.Down(src)
		influxtesting.ErrorsEqual(t, err, c.err)
		if c.old != nil {
			result := new(OldTelegrafConfig)
			_ = json.Unmarshal(dst, result)
			if diff := cmp.Diff(result, c.old); diff != "" {
				t.Errorf("TelegrafConfig up are different -got/+want\ndiff %s", diff)
			}
		}
	}
}
