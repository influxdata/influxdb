package dbrp

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var (
	ByOrgIDIndexMapping = kv.NewIndexMapping(bucket, byOrgIDIndexBucket, func(v []byte) ([]byte, error) {
		var dbrp influxdb.DBRPMapping
		if err := json.Unmarshal(v, &dbrp); err != nil {
			return nil, err
		}

		id, _ := dbrp.OrganizationID.Encode()
		return id, nil
	})
)
