package migration

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

func ErrInvalidMigration(n string) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf(`DB contains record of unknown migration %q - if you are downgrading from a more recent version of influxdb, please run the "influxd downgrade" command from that version to revert your metadata to be compatible with this version prior to starting influxd.`, n),
	}
}
