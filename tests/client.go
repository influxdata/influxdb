package tests

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/flux/csv"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	influxhttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/influxdb/v2/tenant"
)

type ClientConfig struct {
	UserID             platform.ID
	OrgID              platform.ID
	BucketID           platform.ID
	DocumentsNamespace string

	// If Session is provided, Token is ignored.
	Token   string
	Session *influxdb.Session
}

// Client provides an API for writing, querying, and interacting with
// resources like authorizations, buckets, and organizations.
type Client struct {
	Client *httpc.Client
	*influxhttp.Service

	*authorization.AuthorizationClientService
	*tenant.BucketClientService
	*tenant.OrgClientService
	*tenant.UserClientService

	ClientConfig
}

// NewClient initialises a new Client which is ready to write points to the HTTP write endpoint.
func NewClient(endpoint string, config ClientConfig) (*Client, error) {
	opts := make([]httpc.ClientOptFn, 0)
	if config.Session != nil {
		config.Token = ""
		opts = append(opts, httpc.WithSessionCookie(config.Session.Key))
	}
	hc, err := influxhttp.NewHTTPClient(endpoint, config.Token, false, opts...)
	if err != nil {
		return nil, err
	}

	svc, err := influxhttp.NewService(hc, endpoint, config.Token)
	if err != nil {
		return nil, err
	}
	return &Client{
		Client:                     hc,
		Service:                    svc,
		AuthorizationClientService: &authorization.AuthorizationClientService{Client: hc},
		BucketClientService:        &tenant.BucketClientService{Client: hc},
		OrgClientService:           &tenant.OrgClientService{Client: hc},
		UserClientService:          &tenant.UserClientService{Client: hc},
		ClientConfig:               config,
	}, nil
}

// Open opens the client
func (c *Client) Open() error { return nil }

// Close closes the client
func (c *Client) Close() error { return nil }

// MustWriteBatch calls WriteBatch, panicking if an error is encountered.
func (c *Client) MustWriteBatch(points string) {
	if err := c.WriteBatch(points); err != nil {
		panic(err)
	}
}

// WriteBatch writes the current batch of points to the HTTP endpoint.
func (c *Client) WriteBatch(points string) error {
	return c.WriteService.WriteTo(
		context.Background(),
		influxdb.BucketFilter{
			ID:             &c.BucketID,
			OrganizationID: &c.OrgID,
		},
		strings.NewReader(points),
	)
}

// Query returns the CSV response from a flux query to the HTTP API.
//
// This also remove all the \r to make it easier to write tests.
func (c *Client) QueryFlux(org, query string) (string, error) {
	var csv string
	csvResp := func(resp *http.Response) error {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		// remove the \r to simplify testing against a body of CSV.
		body = bytes.ReplaceAll(body, []byte("\r"), nil)
		csv = string(body)
		return nil
	}

	qr := QueryRequestBody(query)
	err := c.Client.PostJSON(qr, fluxPath).
		QueryParams([2]string{"org", org}).
		Accept("text/csv").
		RespFn(csvResp).
		StatusFn(httpc.StatusIn(http.StatusOK)).
		Do(context.Background())

	return csv, err
}

const (
	fluxPath = "/api/v2/query"
	// This is the only namespace for documents present after init.
	DefaultDocumentsNamespace = "templates"
)

// QueryRequestBody creates a body for a flux query using common CSV output params.
// Headers are included, but, annotations are not.
func QueryRequestBody(flux string) *influxhttp.QueryRequest {
	header := true
	return &influxhttp.QueryRequest{
		Type:  "flux",
		Query: flux,
		Dialect: influxhttp.QueryDialect{
			Header:         &header,
			Delimiter:      ",",
			CommentPrefix:  "#",
			DateTimeFormat: "RFC3339",
			Annotations:    csv.DefaultEncoderConfig().Annotations,
		},
	}
}

// MustCreateAuth creates an auth  or is a fatal error.
// Used in tests where the content of the bucket does not matter.
//
// This authorization token is an operator token for the default
// organization for the default user.
func (c *Client) MustCreateAuth(t *testing.T) platform.ID {
	t.Helper()

	perms := influxdb.OperPermissions()
	auth := &influxdb.Authorization{
		OrgID:       c.OrgID,
		UserID:      c.UserID,
		Permissions: perms,
	}
	err := c.CreateAuthorization(context.Background(), auth)
	if err != nil {
		t.Fatalf("unable to create auth: %v", err)
	}
	return auth.ID
}

// MustCreateBucket creates a bucket or is a fatal error.
// Used in tests where the content of the bucket does not matter.
func (c *Client) MustCreateBucket(t *testing.T) platform.ID {
	t.Helper()

	bucket := &influxdb.Bucket{OrgID: c.OrgID, Name: "n1"}
	err := c.CreateBucket(context.Background(), bucket)
	if err != nil {
		t.Fatalf("unable to create bucket: %v", err)
	}
	return bucket.ID
}

// MustCreateOrg creates an org or is a fatal error.
// Used in tests where the content of the org does not matter.
func (c *Client) MustCreateOrg(t *testing.T) platform.ID {
	t.Helper()

	org := &influxdb.Organization{Name: "n1"}
	err := c.CreateOrganization(context.Background(), org)
	if err != nil {
		t.Fatalf("unable to create org: %v", err)
	}
	return org.ID
}

// MustCreateLabel creates a label or is a fatal error.
// Used in tests where the content of the label does not matter.
func (c *Client) MustCreateLabel(t *testing.T) platform.ID {
	t.Helper()

	l := &influxdb.Label{OrgID: c.OrgID, Name: "n1"}
	err := c.CreateLabel(context.Background(), l)
	if err != nil {
		t.Fatalf("unable to create label: %v", err)
	}
	return l.ID
}

// MustCreateCheck creates a check or is a fatal error.
// Used in tests where the content of the check does not matter.
func (c *Client) MustCreateCheck(t *testing.T) platform.ID {
	t.Helper()

	chk, err := c.CreateCheck(context.Background(), MockCheck("c", c.OrgID, c.UserID))
	if err != nil {
		t.Fatalf("unable to create check: %v", err)
	}
	return chk.ID
}

// MustCreateTelegraf creates a telegraf config or is a fatal error.
// Used in tests where the content of the telegraf config does not matter.
func (c *Client) MustCreateTelegraf(t *testing.T) platform.ID {
	t.Helper()

	tc := &influxdb.TelegrafConfig{
		OrgID:       c.OrgID,
		Name:        "n1",
		Description: "d1",
		Config:      "[[howdy]]",
	}
	unused := platform.ID(1) /* this id is not used in the API */
	err := c.CreateTelegrafConfig(context.Background(), tc, unused)
	if err != nil {
		t.Fatalf("unable to create telegraf config: %v", err)
	}
	return tc.ID
}

// MustCreateUser creates a user or is a fatal error.
// Used in tests where the content of the user does not matter.
func (c *Client) MustCreateUser(t *testing.T) platform.ID {
	t.Helper()

	u := &influxdb.User{Name: "n1"}
	err := c.CreateUser(context.Background(), u)
	if err != nil {
		t.Fatalf("unable to create user: %v", err)
	}
	return u.ID
}

// MustCreateVariable creates a variable or is a fatal error.
// Used in tests where the content of the variable does not matter.
func (c *Client) MustCreateVariable(t *testing.T) platform.ID {
	t.Helper()

	v := &influxdb.Variable{
		OrganizationID: c.OrgID,
		Name:           "n1",
		Arguments: &influxdb.VariableArguments{
			Type:   "constant",
			Values: influxdb.VariableConstantValues{"v1", "v2"},
		},
	}
	err := c.CreateVariable(context.Background(), v)
	if err != nil {
		t.Fatalf("unable to create variable: %v", err)
	}
	return v.ID
}

// MustCreateNotificationEndpoint creates a notification endpoint or is a fatal error.
// Used in tests where the content of the notification endpoint does not matter.
func (c *Client) MustCreateNotificationEndpoint(t *testing.T) platform.ID {
	t.Helper()

	ne := ValidNotificationEndpoint(c.OrgID)
	err := c.CreateNotificationEndpoint(context.Background(), ne, c.UserID)
	if err != nil {
		t.Fatalf("unable to create notification endpoint: %v", err)
	}
	return ne.GetID()
}

// MustCreateNotificationRule creates a Notification Rule or is a fatal error
// Used in tests where the content of the notification rule does not matter
func (c *Client) MustCreateNotificationRule(t *testing.T) platform.ID {
	t.Helper()
	ctx := context.Background()

	ne := ValidCustomNotificationEndpoint(c.OrgID, time.Now().String())
	err := c.CreateNotificationEndpoint(ctx, ne, c.UserID)
	if err != nil {
		t.Fatalf("unable to create notification endpoint: %v", err)
	}
	endpointID := ne.GetID()
	r := ValidNotificationRule(c.OrgID, endpointID)
	rc := influxdb.NotificationRuleCreate{NotificationRule: r, Status: influxdb.Active}

	err = c.CreateNotificationRule(ctx, rc, c.UserID)
	if err != nil {
		t.Fatalf("unable to create notification rule: %v", err)
	}

	// we don't need this endpoint, so delete it to be compatible with other tests
	_, _, err = c.DeleteNotificationEndpoint(ctx, endpointID)
	if err != nil {
		t.Fatalf("unable to delete notification endpoint: %v", err)
	}

	return r.GetID()
}

// MustCreateDBRPMapping creates a DBRP Mapping or is a fatal error.
// Used in tests where the content of the mapping does not matter.
// The created mapping points to the user's default bucket.
func (c *Client) MustCreateDBRPMapping(t *testing.T) platform.ID {
	t.Helper()
	ctx := context.Background()

	m := &influxdb.DBRPMappingV2{
		Database:        "db",
		RetentionPolicy: "rp",
		OrganizationID:  c.OrgID,
		BucketID:        c.BucketID,
	}
	if err := c.DBRPMappingServiceV2.Create(ctx, m); err != nil {
		t.Fatalf("unable to create DBRP mapping: %v", err)
	}
	return m.ID
}

// MustCreateResource will create a generic resource via the API.
// Used in tests where the content of the resource does not matter.
//
//  // Create one of each org resource
//  for _, r := range influxdb.OrgResourceTypes {
//      client.MustCreateResource(t, r)
//  }
//
//
//  // Create a variable:
//  id := client.MustCreateResource(t, influxdb.VariablesResourceType)
//  defer client.MustDeleteResource(t, influxdb.VariablesResourceType, id)
func (c *Client) MustCreateResource(t *testing.T, r influxdb.ResourceType) platform.ID {
	t.Helper()

	switch r {
	case influxdb.AuthorizationsResourceType: // 0
		return c.MustCreateAuth(t)
	case influxdb.BucketsResourceType: // 1
		return c.MustCreateBucket(t)
	case influxdb.OrgsResourceType: // 3
		return c.MustCreateOrg(t)
	case influxdb.SourcesResourceType: // 4
		t.Skip("I think sources are going to be removed right?")
	case influxdb.TasksResourceType: // 5
		t.Skip("Task go client is not yet created")
	case influxdb.TelegrafsResourceType: // 6
		return c.MustCreateTelegraf(t)
	case influxdb.UsersResourceType: // 7
		return c.MustCreateUser(t)
	case influxdb.VariablesResourceType: // 8
		return c.MustCreateVariable(t)
	case influxdb.ScraperResourceType: // 9
		t.Skip("Scraper go client is not yet created")
	case influxdb.SecretsResourceType: // 10
		t.Skip("Secrets go client is not yet created")
	case influxdb.LabelsResourceType: // 11
		return c.MustCreateLabel(t)
	case influxdb.ViewsResourceType: // 12
		t.Skip("Are views still a thing?")
	case influxdb.NotificationRuleResourceType: // 14
		return c.MustCreateNotificationRule(t)
	case influxdb.NotificationEndpointResourceType: // 15
		return c.MustCreateNotificationEndpoint(t)
	case influxdb.ChecksResourceType: // 16
		return c.MustCreateCheck(t)
	case influxdb.DBRPResourceType: // 17
		return c.MustCreateDBRPMapping(t)
	}
	return 0
}

// DeleteResource will remove a resource using the API.
func (c *Client) DeleteResource(t *testing.T, r influxdb.ResourceType, id platform.ID) error {
	t.Helper()

	ctx := context.Background()
	switch r {
	case influxdb.AuthorizationsResourceType: // 0
		return c.DeleteAuthorization(ctx, id)
	case influxdb.BucketsResourceType: // 1
		return c.DeleteBucket(context.Background(), id)
	case influxdb.OrgsResourceType: // 3
		return c.DeleteOrganization(ctx, id)
	case influxdb.SourcesResourceType: // 4
		t.Skip("I think sources are going to be removed right?")
	case influxdb.TasksResourceType: // 5
		t.Skip("Task go client is not yet created")
	case influxdb.TelegrafsResourceType: // 6
		return c.DeleteTelegrafConfig(ctx, id)
	case influxdb.UsersResourceType: // 7
		return c.DeleteUser(ctx, id)
	case influxdb.VariablesResourceType: // 8
		return c.DeleteVariable(ctx, id)
	case influxdb.ScraperResourceType: // 9
		t.Skip("Scraper go client is not yet created")
	case influxdb.SecretsResourceType: // 10
		t.Skip("Secrets go client is not yet created")
	case influxdb.LabelsResourceType: // 11
		return c.DeleteLabel(ctx, id)
	case influxdb.ViewsResourceType: // 12
		t.Skip("Are views still a thing?")
	case influxdb.NotificationRuleResourceType: // 14
		return c.DeleteNotificationRule(ctx, id)
	case influxdb.NotificationEndpointResourceType: // 15
		// Ignore the other results as suggested by goDoc.
		_, _, err := c.DeleteNotificationEndpoint(ctx, id)
		return err
	case influxdb.ChecksResourceType: // 16
		return c.DeleteCheck(ctx, id)
	case influxdb.DBRPResourceType: // 17
		return c.DBRPMappingServiceV2.Delete(ctx, c.OrgID, id)
	}
	return nil
}

// MustDeleteResource requires no error when deleting a resource.
func (c *Client) MustDeleteResource(t *testing.T, r influxdb.ResourceType, id platform.ID) {
	t.Helper()

	if err := c.DeleteResource(t, r, id); err != nil {
		t.Fatalf("unable to delete resource %v %v: %v", r, id, err)
	}
}

// FindAll returns all the IDs of a specific resource type.
func (c *Client) FindAll(t *testing.T, r influxdb.ResourceType) ([]platform.ID, error) {
	t.Helper()

	var ids []platform.ID
	ctx := context.Background()
	switch r {
	case influxdb.AuthorizationsResourceType: // 0
		rs, _, err := c.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.BucketsResourceType: // 1
		rs, _, err := c.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.OrgsResourceType: // 3
		rs, _, err := c.FindOrganizations(ctx, influxdb.OrganizationFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.SourcesResourceType: // 4
		t.Skip("I think sources are going to be removed right?")
	case influxdb.TasksResourceType: // 5
		t.Skip("Task go client is not yet created")
	case influxdb.TelegrafsResourceType: // 6
		rs, _, err := c.FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.UsersResourceType: // 7
		rs, _, err := c.FindUsers(ctx, influxdb.UserFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.VariablesResourceType: // 8
		rs, err := c.FindVariables(ctx, influxdb.VariableFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.ScraperResourceType: // 9
		t.Skip("Scraper go client is not yet created")
	case influxdb.SecretsResourceType: // 10
		t.Skip("Secrets go client is not yet created")
	case influxdb.LabelsResourceType: // 11
		rs, err := c.FindLabels(ctx, influxdb.LabelFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.ViewsResourceType: // 12
		t.Skip("Are views still a thing?")
	case influxdb.NotificationRuleResourceType: // 14
		rs, _, err := c.FindNotificationRules(ctx, influxdb.NotificationRuleFilter{OrgID: &c.OrgID})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.GetID())
		}
		return ids, nil
	case influxdb.NotificationEndpointResourceType: // 15
		rs, _, err := c.FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{OrgID: &c.OrgID})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.GetID())
		}
	case influxdb.ChecksResourceType: // 16
		rs, _, err := c.FindChecks(ctx, influxdb.CheckFilter{})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	case influxdb.DBRPResourceType: // 17
		rs, _, err := c.DBRPMappingServiceV2.FindMany(ctx, influxdb.DBRPMappingFilterV2{OrgID: &c.OrgID})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			ids = append(ids, r.ID)
		}
	}
	return ids, nil
}

// MustFindAll returns all the IDs of a specific resource type; any error
// is fatal.
func (c *Client) MustFindAll(t *testing.T, r influxdb.ResourceType) []platform.ID {
	t.Helper()

	ids, err := c.FindAll(t, r)
	if err != nil {
		t.Fatalf("unexpected error finding resources %v: %v", r, err)
	}
	return ids
}

func (c *Client) AddURM(u platform.ID, typ influxdb.UserType, r influxdb.ResourceType, id platform.ID) error {
	access := &influxdb.UserResourceMapping{
		UserID:       u,
		UserType:     typ,
		MappingType:  influxdb.UserMappingType,
		ResourceType: r,
		ResourceID:   id,
	}

	return c.CreateUserResourceMapping(
		context.Background(),
		access,
	)
}

// AddOwner associates the user as owner of the resource.
func (c *Client) AddOwner(user platform.ID, r influxdb.ResourceType, id platform.ID) error {
	return c.AddURM(user, influxdb.Owner, r, id)
}

// MustAddOwner requires that the user is associated with the resource
// or the test will be stopped fatally.
func (c *Client) MustAddOwner(t *testing.T, user platform.ID, r influxdb.ResourceType, id platform.ID) {
	t.Helper()

	if err := c.AddOwner(user, r, id); err != nil {
		t.Fatalf("unexpected error adding owner %v to %v: %v", user, id, err)
	}
}

// AddMember associates the user as member of the resource.
func (c *Client) AddMember(user platform.ID, r influxdb.ResourceType, id platform.ID) error {
	return c.AddURM(user, influxdb.Member, r, id)
}

// MustAddMember requires that the user is associated with the resource
// or the test will be stopped fatally.
func (c *Client) MustAddMember(t *testing.T, user platform.ID, r influxdb.ResourceType, id platform.ID) {
	t.Helper()

	if err := c.AddMember(user, r, id); err != nil {
		t.Fatalf("unexpected error adding member %v to %v", user, id)
	}
}

// RemoveURM removes association of the user to the resource.
// Interestingly the URM service does not make difference on the user type.
// I.e. removing an URM from a user to a resource, will delete every URM of every type
// from that user to that resource.
// Or, put in another way, there can only be one resource mapping from a user to a
// resource at a time: either you are a member, or an owner (in that case you are a member too).
func (c *Client) RemoveURM(user, id platform.ID) error {
	return c.DeleteUserResourceMapping(context.Background(), id, user)
}

// RemoveSpecificURM gets around a client issue where deletes doesn't have enough context to remove a urm from
// a specific resource type
func (c *Client) RemoveSpecificURM(rt influxdb.ResourceType, ut influxdb.UserType, user, id platform.ID) error {
	return c.SpecificURMSvc(rt, ut).DeleteUserResourceMapping(context.Background(), id, user)
}

// MustRemoveURM requires that the user is removed as owner/member from the resource.
func (c *Client) MustRemoveURM(t *testing.T, user, id platform.ID) {
	t.Helper()

	if err := c.RemoveURM(user, id); err != nil {
		t.Fatalf("unexpected error removing org/resource mapping: %v", err)
	}
}

// CreateLabelMapping creates a label mapping for label `l` to the resource with `id`.
func (c *Client) CreateLabelMapping(l platform.ID, r influxdb.ResourceType, id platform.ID) error {
	mapping := &influxdb.LabelMapping{
		LabelID:      l,
		ResourceType: r,
		ResourceID:   id,
	}
	return c.LabelService.CreateLabelMapping(
		context.Background(),
		mapping,
	)
}

// MustCreateLabelMapping requires that the label is associated with the resource
// or the test will be stopped fatally.
func (c *Client) MustCreateLabelMapping(t *testing.T, l platform.ID, r influxdb.ResourceType, id platform.ID) {
	t.Helper()

	if err := c.CreateLabelMapping(l, r, id); err != nil {
		t.Fatalf("unexpected error attaching label %v to %v: %v", l, id, err)
	}
}

// FindLabelMappings finds the labels for the specified resource.
func (c *Client) FindLabelMappings(r influxdb.ResourceType, id platform.ID) ([]platform.ID, error) {
	filter := influxdb.LabelMappingFilter{
		ResourceType: r,
		ResourceID:   id,
	}
	ls, err := c.LabelService.FindResourceLabels(
		context.Background(),
		filter,
	)
	if err != nil {
		return nil, err
	}
	var ids []platform.ID
	for _, r := range ls {
		ids = append(ids, r.ID)
	}
	return ids, nil
}

// MustFindLabelMappings makes the test fail if an error is found.
func (c *Client) MustFindLabelMappings(t *testing.T, r influxdb.ResourceType, id platform.ID) []platform.ID {
	t.Helper()

	ls, err := c.FindLabelMappings(r, id)
	if err != nil {
		t.Fatalf("unexpected error finding label mappings: %v", err)
	}
	return ls
}

// DeleteLabelMapping deletes the label for the specified resource.
func (c *Client) DeleteLabelMapping(l platform.ID, r influxdb.ResourceType, id platform.ID) error {
	m := &influxdb.LabelMapping{
		ResourceType: r,
		ResourceID:   id,
		LabelID:      l,
	}
	return c.LabelService.DeleteLabelMapping(
		context.Background(),
		m,
	)
}

// MustDeleteLabelMapping makes the test fail if an error is found.
func (c *Client) MustDeleteLabelMapping(t *testing.T, l platform.ID, r influxdb.ResourceType, id platform.ID) {
	t.Helper()

	if err := c.DeleteLabelMapping(l, r, id); err != nil {
		t.Fatalf("unexpected error deleting label %v from %v", l, id)
	}
}
