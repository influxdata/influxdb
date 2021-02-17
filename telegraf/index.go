package telegraf

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var (
	// ByOrganizationIndexMapping is the mapping definition for fetching
	// telegrafs by organization ID.
	ByOrganizationIndexMapping = kv.NewIndexMapping(
		[]byte("telegrafv1"),
		[]byte("telegrafbyorgindexv1"),
		func(v []byte) ([]byte, error) {
			var telegraf influxdb.TelegrafConfig
			if err := json.Unmarshal(v, &telegraf); err != nil {
				return nil, err
			}

			id, _ := telegraf.OrgID.Encode()
			return id, nil
		},
	)
)
