package index

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

// URMByUserIndeMappingx is the mapping description of an index
// between a user and a URM
var URMByUserIndexMapping = kv.NewIndexMapping(
	[]byte("userresourcemappingsv1"),
	[]byte("userresourcemappingsbyuserindexv1"),
	func(v []byte) ([]byte, error) {
		var urm influxdb.UserResourceMapping
		if err := json.Unmarshal(v, &urm); err != nil {
			return nil, err
		}

		id, _ := urm.UserID.Encode()
		return id, nil
	},
)
