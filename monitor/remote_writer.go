package monitor

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

// A remotePointsWriter implements the models.PointsWriter interface
// but redirects points to be writte to a remote node, using an influx
// client.
type remotePointsWriter struct {
	client client.Client
}

func newRemotePointsWriter(addr, user, password string) (*remotePointsWriter, error) {
	conf := client.HTTPConfig{
		Addr:     addr,
		Username: user,
		Password: password,
		Timeout:  30 * time.Second,
	}
	clt, err := client.NewHTTPClient(conf)
	if err != nil {
		return nil, err
	}

	return &remotePointsWriter{client: clt}, nil
}

// WritePoints writes the provided points to a remote node via an
// influx client over HTTP.
func (w *remotePointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	conf := client.BatchPointsConfig{
		Database:        database,
		RetentionPolicy: retentionPolicy,
	}

	bp, err := client.NewBatchPoints(conf)
	if err != nil {
		return err
	}

	for _, point := range points {
		bp.AddPoint(client.NewPointFrom(point))
	}

	return w.client.Write(bp)
}
