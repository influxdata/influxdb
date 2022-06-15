package transport

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var replicationsBucket = []byte("replicationsv2")

func newTelemetryCollectingService(kv kv.Store, underlying ReplicationService) *telemetryService {
	return &telemetryService{
		kv:         kv,
		underlying: underlying,
	}
}

type telemetryService struct {
	kv         kv.Store
	underlying ReplicationService
}

func (t telemetryService) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	return t.underlying.ListReplications(ctx, filter)
}

func (t telemetryService) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	return t.underlying.GetReplication(ctx, id)
}

func (t telemetryService) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
	return t.underlying.UpdateReplication(ctx, id, request)
}

func (t telemetryService) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	return t.underlying.ValidateNewReplication(ctx, request)
}

func (t telemetryService) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	return t.underlying.ValidateUpdatedReplication(ctx, id, request)
}

func (t telemetryService) ValidateReplication(ctx context.Context, id platform.ID) error {
	return t.underlying.ValidateReplication(ctx, id)
}

func (t telemetryService) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	conn, err := t.underlying.CreateReplication(ctx, request)
	if err != nil {
		return conn, err
	}
	if err := t.kv.Update(ctx, func(tx kv.Tx) error {
		encodedID, err := request.OrgID.Encode()
		if err != nil {
			return platform.ErrInvalidID
		}
		bucket, err := tx.Bucket(replicationsBucket)
		if err != nil {
			return err // todo wrap a better error here?
		}
		count, err := t.countReplications(ctx, request.OrgID)
		if err != nil {
			return err
		}
		return bucket.Put(encodedID, count)
	}); err != nil {
		return nil, err // todo wrap a better error here?
	}
	return conn, err
}

func (t telemetryService) DeleteReplication(ctx context.Context, id platform.ID) error {
	rc, err := t.underlying.GetReplication(ctx, id)
	if err != nil {
		return err
	}
	orgID := rc.OrgID

	err = t.underlying.DeleteReplication(ctx, id)
	if err != nil {
		return err
	}
	return t.kv.Update(ctx, func(tx kv.Tx) error {
		encodedID, err := orgID.Encode()
		if err != nil {
			return err
		}
		bucket, err := tx.Bucket(replicationsBucket)
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

func (t telemetryService) countReplications(ctx context.Context, orgID platform.ID) ([]byte, error) {
	req := influxdb.ReplicationListFilter{
		OrgID: orgID,
	}
	list, err := t.underlying.ListReplications(ctx, req)
	if err != nil {
		return nil, err // todo wrap a better error here?
	}
	return t.marshalCount(int64(len(list.Replications)))
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
