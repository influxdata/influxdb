package transport

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var remotesBucket = []byte("remotesv2")

func newTelemetryCollectingService(kv kv.Store, underlying RemoteConnectionService) *telemetryService {
	return &telemetryService{
		kv:         kv,
		underlying: underlying,
	}
}

type telemetryService struct {
	kv         kv.Store
	underlying RemoteConnectionService
}

func (t telemetryService) ListRemoteConnections(ctx context.Context, filter influxdb.RemoteConnectionListFilter) (*influxdb.RemoteConnections, error) {
	return t.underlying.ListRemoteConnections(ctx, filter)
}

func (t telemetryService) GetRemoteConnection(ctx context.Context, id platform.ID) (*influxdb.RemoteConnection, error) {
	return t.underlying.GetRemoteConnection(ctx, id)
}

func (t telemetryService) UpdateRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	return t.underlying.UpdateRemoteConnection(ctx, id, request)
}

func (t telemetryService) CreateRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	conn, err := t.underlying.CreateRemoteConnection(ctx, request)
	if err != nil {
		return conn, err
	}
	if err := t.kv.Update(ctx, func(tx kv.Tx) error {
		encodedID, err := request.OrgID.Encode()
		if err != nil {
			return platform.ErrInvalidID
		}
		bucket, err := tx.Bucket(remotesBucket)
		if err != nil {
			return err // todo wrap a better error here?
		}
		count, err := t.countRemotes(ctx, request.OrgID)
		if err != nil {
			return err
		}
		return bucket.Put(encodedID, count)
	}); err != nil {
		return nil, err // todo wrap a better error here?
	}
	return conn, err
}

func (t telemetryService) DeleteRemoteConnection(ctx context.Context, id platform.ID) error {
	rc, err := t.underlying.GetRemoteConnection(ctx, id)
	if err != nil {
		return err
	}
	orgID := rc.OrgID

	err = t.underlying.DeleteRemoteConnection(ctx, id)
	if err != nil {
		return err
	}
	return t.kv.Update(ctx, func(tx kv.Tx) error {
		encodedID, err := orgID.Encode()
		if err != nil {
			return err
		}
		bucket, err := tx.Bucket(remotesBucket)
		if err != nil {
			return err
		}
		count, err := bucket.Get(encodedID)
		if err != nil {
			return err
		}

		c, err := t.unmarshalCount(count)
		if err != nil {
			return err
		}
		c--

		b, err := t.marshalCount(int64(c))
		if err != nil {
			return err
		}

		return bucket.Put(encodedID, b)
	})
}

// todo this might be able to be done in a smarter way
func (t telemetryService) countRemotes(ctx context.Context, orgID platform.ID) ([]byte, error) {
	req := influxdb.RemoteConnectionListFilter{
		OrgID: orgID,
	}
	list, err := t.underlying.ListRemoteConnections(ctx, req)
	if err != nil {
		return nil, err // todo wrap a better error here?
	}
	return t.marshalCount(int64(len(list.Remotes)))
}

func (t telemetryService) unmarshalCount(buf []byte) (int64, error) {
	var count int64
	err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &count)
	return count, err
}

func (t telemetryService) marshalCount(count int64) ([]byte, error) {
	b := make([]byte, 0, 8)
	buf := bytes.NewBuffer(b)
	err := binary.Write(buf, binary.BigEndian, count)
	return buf.Bytes(), err
}
