package migration

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

func ErrInvalidMigration(n string) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf(`DB contains record of unknown migration %q - if you are downgrading from a more recent version of influxdb, please run the "influxd downgrade" command from that version to revert your metadata to be compatible with this version prior to starting influxd.`, n),
	}
}
