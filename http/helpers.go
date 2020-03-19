package http

import (
	"context"
	"net/url"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb"
)

// TODO remove this file once bucket and org service are moved to the tenant service

func decodeIDFromCtx(ctx context.Context, name string) (influxdb.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	idStr := params.ByName(name)

	if idStr == "" {
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing " + name,
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(idStr); err != nil {
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return i, nil
}

func decodeIDFromQuery(v url.Values, key string) (influxdb.ID, error) {
	idStr := v.Get(key)
	if idStr == "" {
		return 0, nil
	}

	id, err := influxdb.IDFromString(idStr)
	if err != nil {
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return *id, nil
}
