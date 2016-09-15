package influx

import (
	"encoding/json"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/mrfusion"
)

type response struct {
	*client.Response
	err error
}

func (r response) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Results)
}

func (r response) Results() ([]mrfusion.Result, error) {
	return []mrfusion.Result{}, nil
}
