package influx

import (
	"encoding/json"

	"github.com/influxdata/influxdb/client/v2"
)

type response struct {
	*client.Response
	err error
}

func (r response) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Results)
}
