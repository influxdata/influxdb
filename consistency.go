package influxdb

// ConsistencyLevel represent a required replication criteria before a write can
// be returned as successful
type ConsistencyLevel int

const (
	// ConsistencyLevelAny allows for hinted hand off, potentially no write happened yet
	ConsistencyLevelAny ConsistencyLevel = iota

	// ConsistencyLevelOne requires at least one data node acknowledged a write
	ConsistencyLevelOne

	// ConsistencyLevelOne requires a quorum of data nodes to acknowledge a write
	ConsistencyLevelQuorum

	// ConsistencyLevelAll requires all data nodes to acknowledge a write
	ConsistencyLevelAll
)

func newConsistencyPolicyN(need int) ConsistencyPolicy {
	return &policyNum{
		need: need,
	}
}

func newConsistencyOwnerPolicy(ownerID int) ConsistencyPolicy {
	return &policyOwner{
		ownerID: ownerID,
	}
}

// ConsistencyPolicy verifies a write consistency level has be met
type ConsistencyPolicy interface {
	IsDone(writerID int, err error) bool
}

// policyNum implements One, Quorum, and All
type policyNum struct {
	failed, succeeded, need int
}

// IsDone determines if the policy has been satisfied based on the given
// writerID or error
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
