package influx

import (
	"bytes"
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
		Command: fmt.Sprintf(`DROP DATABASE "%s"`, database),
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
		Command: fmt.Sprintf(`CREATE RETENTION POLICY "%s" ON "%s" DURATION %s REPLICATION %d`, rp.Name, database, rp.Duration, rp.Replication),
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

func (c *Client) UpdateRP(ctx context.Context, database string, name string, rp *chronograf.RetentionPolicy) (*chronograf.RetentionPolicy, error) {
	var buffer bytes.Buffer
	buffer.WriteString("ALTER RETENTION POLICY")
	if len(rp.Duration) > 0 {
		buffer.WriteString(" DURATION " + rp.Duration)
	}
	if rp.Replication > 0 {
		buffer.WriteString(" REPLICATION " + fmt.Sprint(rp.Replication))
	}
	if len(rp.ShardDuration) > 0 {
		buffer.WriteString(" SHARD DURATION " + rp.ShardDuration)
	}
	if rp.Default == true {
		buffer.WriteString(" DEFAULT")
	}

	_, err := c.Query(ctx, chronograf.Query{
		Command: buffer.String(),
		DB:      database,
		RP:      name,
	})
	if err != nil {
		return nil, err
	}

	// TODO: use actual information here
	res := &chronograf.RetentionPolicy{
		Name: name,
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
