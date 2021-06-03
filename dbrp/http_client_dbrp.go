package dbrp

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

var _ influxdb.DBRPMappingServiceV2 = (*Client)(nil)

// Client connects to Influx via HTTP using tokens to manage DBRPs.
type Client struct {
	Client *httpc.Client
	Prefix string
}

func NewClient(client *httpc.Client) *Client {
	return &Client{
		Client: client,
		Prefix: PrefixDBRP,
	}
}

func (c *Client) dbrpURL(id platform.ID) string {
	return path.Join(c.Prefix, id.String())
}

func (c *Client) FindByID(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMappingV2, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var resp getDBRPResponse
	if err := c.Client.
		Get(c.dbrpURL(id)).
		QueryParams([2]string{"orgID", orgID.String()}).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return nil, err
	}
	return resp.Content, nil
}

func (c *Client) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	params := influxdb.FindOptionParams(opts...)
	if filter.OrgID != nil {
		params = append(params, [2]string{"orgID", filter.OrgID.String()})
	} else {
		return nil, 0, fmt.Errorf("please filter by orgID")
	}
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}
	if filter.BucketID != nil {
		params = append(params, [2]string{"bucketID", filter.BucketID.String()})
	}
	if filter.Database != nil {
		params = append(params, [2]string{"db", *filter.Database})
	}
	if filter.RetentionPolicy != nil {
		params = append(params, [2]string{"rp", *filter.RetentionPolicy})
	}
	if filter.Default != nil {
		params = append(params, [2]string{"default", strconv.FormatBool(*filter.Default)})
	}

	var resp getDBRPsResponse
	if err := c.Client.
		Get(c.Prefix).
		QueryParams(params...).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return nil, 0, err
	}
	return resp.Content, len(resp.Content), nil
}

func (c *Client) Create(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var newDBRP influxdb.DBRPMappingV2
	if err := c.Client.
		PostJSON(createDBRPRequest{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
			Default:         dbrp.Default,
			OrganizationID:  dbrp.OrganizationID.String(),
			BucketID:        dbrp.BucketID.String(),
		}, c.Prefix).
		DecodeJSON(&newDBRP).
		Do(ctx); err != nil {
		return err
	}
	dbrp.ID = newDBRP.ID
	return nil
}

func (c *Client) Update(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := dbrp.Validate(); err != nil {
		return err
	}

	var newDBRP influxdb.DBRPMappingV2
	if err := c.Client.
		PatchJSON(dbrp, c.dbrpURL(dbrp.ID)).
		QueryParams([2]string{"orgID", dbrp.OrganizationID.String()}).
		DecodeJSON(&newDBRP).
		Do(ctx); err != nil {
		return err
	}
	*dbrp = newDBRP
	return nil
}

func (c *Client) Delete(ctx context.Context, orgID, id platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return c.Client.
		Delete(c.dbrpURL(id)).
		QueryParams([2]string{"orgID", orgID.String()}).
		Do(ctx)
}
