package pipeline

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
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
func NewBaseFixture(t *testing.T, p *tests.Pipeline, orgID, bucketID platform.ID) BaseFixture {
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
