package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tests"
)

type ClientTag string

const (
	AdminTag    ClientTag = "admin"
	OwnerTag    ClientTag = "owner"
	MemberTag   ClientTag = "member"
	NoAccessTag ClientTag = "no_access"
)

var AllClientTags = []ClientTag{AdminTag, OwnerTag, MemberTag, NoAccessTag}

// BaseFixture is a Fixture with multiple users in the system.
type BaseFixture struct {
	Admin    *tests.Client
	Owner    *tests.Client
	Member   *tests.Client
	NoAccess *tests.Client
}

// NewBaseFixture creates a BaseFixture with and admin, an org owner, a member, and an outsider
// for the given orgID and bucketID.
func NewBaseFixture(t *testing.T, p *tests.Pipeline, orgID, bucketID influxdb.ID) BaseFixture {
	fx := BaseFixture{}
	admin := p.MustNewAdminClient()
	fx.Admin = admin
	cli, id, err := p.BrowserFor(orgID, bucketID, "owner")
	if err != nil {
		t.Fatalf("error while creating browser client: %v", err)
	}
	admin.MustAddOwner(t, id, influxdb.OrgsResourceType, orgID)
	cli.UserID = id
	fx.Owner = cli

	cli, id, err = p.BrowserFor(orgID, bucketID, "member")
	if err != nil {
		t.Fatalf("error while creating browser client: %v", err)
	}
	admin.MustAddMember(t, id, influxdb.OrgsResourceType, orgID)
	cli.UserID = id
	fx.Member = cli

	cli, id, err = p.BrowserFor(orgID, bucketID, "no_access")
	if err != nil {
		t.Fatalf("error while creating browser client: %v", err)
	}
	cli.UserID = id
	fx.NoAccess = cli
	return fx
}

// GetClient returns the client associated with the given tag.
func (f BaseFixture) GetClient(tag ClientTag) *tests.Client {
	switch tag {
	case AdminTag:
		return f.Admin
	case OwnerTag:
		return f.Owner
	case MemberTag:
		return f.Member
	case NoAccessTag:
		return f.NoAccess
	default:
		panic(fmt.Sprintf("unknown tag %s", tag))
	}
}

// NewDefaultBaseFixture creates a BaseFixture with the default ids.
func NewDefaultBaseFixture(t *testing.T, p *tests.Pipeline) BaseFixture {
	return NewBaseFixture(t, p, p.DefaultOrgID, p.DefaultBucketID)
}

// ResourceFixture is a general purpose fixture.
// It contains a resource, users for every role, and a label.
// Users of the fixture can use it to create URMs from users to the resource or else.
// The label is there to create label mappings.
type ResourceFixture struct {
	BaseFixture
	ResourceType influxdb.ResourceType
	ResourceID   influxdb.ID
	LabelID      influxdb.ID
}

// NewResourceFixture creates a new ResourceFixture.
func NewResourceFixture(t *testing.T, p *tests.Pipeline, typ influxdb.ResourceType) ResourceFixture {
	bfx := NewDefaultBaseFixture(t, p)
	l := &influxdb.Label{
		OrgID:      bfx.Admin.OrgID,
		Name:       "map_me",
		Properties: nil,
	}
	if err := bfx.Admin.CreateLabel(context.Background(), l); err != nil {
		t.Fatalf("error in creating label as admin: %v", err)
	}
	rid := bfx.Admin.MustCreateResource(t, typ)
	return ResourceFixture{
		BaseFixture:  bfx,
		ResourceType: typ,
		ResourceID:   rid,
		LabelID:      l.ID,
	}
}

// SubjectResourceFixture is a ResourceFixture associated with a subject.
// The subject is a client that performs the actions.
type SubjectResourceFixture struct {
	ResourceFixture
	SubjectTag ClientTag
}

// NewSubjectResourceFixture creates a new SubjectResourceFixture.
func NewSubjectResourceFixture(t *testing.T, p *tests.Pipeline, subj ClientTag, rt influxdb.ResourceType) SubjectResourceFixture {
	return SubjectResourceFixture{
		ResourceFixture: NewResourceFixture(t, p, rt),
		SubjectTag:      subj,
	}
}

func (f SubjectResourceFixture) Subject() *tests.Client {
	return f.GetClient(f.SubjectTag)
}

// ResourceSubjectIteration is the iteration on all the resources and subjects given.
// One can specify what do to for an iteration step by passing a ResourceSubjectIterationStepFn
// to `ResourceSubjectIteration.Do` or `ResourceSubjectIteration.ParDo`.
type ResourceSubjectIteration struct {
	subjects []ClientTag
}

// Creates a ResourceSubjectIteration for applying a ResourceSubjectIterationStepFn to each
// resource-subject couple.
func ForEachResourceSubject(subjects ...ClientTag) ResourceSubjectIteration {
	return ResourceSubjectIteration{
		subjects: subjects,
	}
}

// ResourceSubjectIterationStepFn is a function applied to each step of iteration on resources
// and subjects.
type ResourceSubjectIterationStepFn func(subj ClientTag, rt influxdb.ResourceType)

func (p ResourceSubjectIteration) Do(f ResourceSubjectIterationStepFn) {
	for _, typ := range influxdb.OrgResourceTypes {
		for _, subj := range p.subjects {
			f(subj, typ)
		}
	}
}
