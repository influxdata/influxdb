package data

import (
	"errors"
	"time"

	"github.com/influxdb/influxdb"
)

type ReplicaAck int

const (
	// ReplicaAckAny allows for hinted hand off, potentially no write happened yet
	ReplicaAckAny ReplicaAck = iota
	// ReplicaAckOne
	ReplicaAckOne
	ReplicaAckQuorum
	ReplicaAckOwner
	ReplicaAckAll
)

const defaultReadTimeout = 5 * time.Second

var ErrTimeout = errors.New("timeout")

type Payload struct {
	Database        string
	RetentionPolicy string
	ReplicaAck      ReplicaAck
	Points          []influxdb.Point
}

// replicator handles writes only to replicas within the same shard
type replicator struct {
	ws  []PayloadWriter
	pol ackPolicy
}

func (r *replicator) WritePayload(p *Payload) error {
	type result struct {
		writerID int
		err      error
	}
	ch := make(chan result, len(r.ws))
	for i, w := range r.ws {
		go func(id int, w PayloadWriter) {
			err := w.WritePayload(p)
			ch <- result{id, err}
		}(i, w)
	}
	timeout := time.After(defaultReadTimeout)
	for range r.ws {
		select {
		case <-timeout:
			// return timeout error to caller
			return ErrTimeout
		case res := <-ch:
			if !r.pol.IsDone(res.writerID, res.err) {
				continue
			}
			if res.err != nil {
				return res.err
			}
			return nil
		}

	}
	panic("unreachable or bad policy impl")
}

func newAckPolicyN(need int) ackPolicy {
	return &policyNum{
		need: need,
	}
}

func newAckOwnerPolicy(ownerID int) ackPolicy {
	return &policyOwner{
		ownerID: ownerID,
	}
}

type ackPolicy interface {
	IsDone(writerID int, err error) bool
}

// policyNum implements One, Quorum, and All
type policyNum struct {
	failed, succeeded, need int
}

func (p *policyNum) IsDone(writerID int, err error) bool {
	if err == nil {
		p.succeeded++
		return p.succeeded >= p.need
	}
	p.failed++
	return p.need-p.failed-p.succeeded >= p.need-p.succeeded

}

type policyOwner struct {
	ownerID int
}

func (p *policyOwner) IsDone(writerID int, err error) bool {
	return p.ownerID == writerID
}

type localWriter struct {
	//Shards Shards
}

func (w localWriter) WritePayload(p *Payload) error {
	return nil
}

type remoteWriter struct {
	//ShardInfo []ShardInfo
	//DataNodes DataNodes
}

func (w remoteWriter) WritePayload(p *Payload) error {
	return nil
}
