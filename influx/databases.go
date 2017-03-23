package influx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/chronograf"
)

func (c *Client) AllDB(ctx context.Context) ([]chronograf.Database, error) {
	databases, err := c.showDatabases(ctx)
	if err != nil {
		return nil, err
	}

	return databases, nil
}

func (c *Client) CreateDB(ctx context.Context, db *chronograf.Database) (*chronograf.Database, error) {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`CREATE DATABASE "%s"`, db.Name),
	})
	if err != nil {
		return nil, err
	}

	res := &chronograf.Database{Name: db.Name}

	return res, nil
}

func (c *Client) DropDB(ctx context.Context, database string) error {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP DATABASE`),
		DB:      database,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) AllRP(ctx context.Context, database string) ([]chronograf.RetentionPolicy, error) {
	retentionPolicies, err := c.showRetentionPolicies(ctx, database)
	if err != nil {
		return nil, err
	}

	return retentionPolicies, nil
}

func (c *Client) CreateRP(ctx context.Context, database string, rp *chronograf.RetentionPolicy) (*chronograf.RetentionPolicy, error) {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`CREATE RETENTION POLICY "%s" DURATION "%s" REPLICATION "%s"`, rp.Name, rp.Duration, rp.Replication),
		DB:      database,
	})
	if err != nil {
		return nil, err
	}

	res := &chronograf.RetentionPolicy{
		Name:        rp.Name,
		Duration:    rp.Duration,
		Replication: rp.Replication,
	}

	return res, nil
}

func (c *Client) DropRP(ctx context.Context, database string, rp string) error {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP RETENTION POLICY`),
		DB:      database,
		RP:      rp,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) showDatabases(ctx context.Context) ([]chronograf.Database, error) {
	res, err := c.Query(ctx, chronograf.Query{
		Command: `SHOW DATABASES`,
	})
	if err != nil {
		return nil, err
	}
	octets, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := showResults{}
	if err := json.Unmarshal(octets, &results); err != nil {
		return nil, err
	}

	return results.Databases(), nil
}

func (c *Client) showRetentionPolicies(ctx context.Context, name string) ([]chronograf.RetentionPolicy, error) {
	retentionPolicies, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`SHOW RETENTION POLICIES`),
		DB:      name,
	})

	if err != nil {
		return nil, err
	}
	octets, err := retentionPolicies.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := showResults{}
	if err := json.Unmarshal(octets, &results); err != nil {
		return nil, err
	}

	return results.RetentionPolicies(), nil
}
