package influx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/chronograf"
)

// AllDB returns all databases from within Influx
func (c *Client) AllDB(ctx context.Context) ([]chronograf.Database, error) {
	databases, err := c.showDatabases(ctx)
	if err != nil {
		return nil, err
	}

	return databases, nil
}

// CreateDB creates a database within Influx
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

// DropDB drops a database within Influx
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

// AllRP returns all the retention policies for a specific database
func (c *Client) AllRP(ctx context.Context, database string) ([]chronograf.RetentionPolicy, error) {
	retentionPolicies, err := c.showRetentionPolicies(ctx, database)
	if err != nil {
		return nil, err
	}

	return retentionPolicies, nil
}

func (c *Client) getRP(ctx context.Context, db, name string) (chronograf.RetentionPolicy, error) {
	rps, err := c.AllRP(ctx, db)
	if err != nil {
		return chronograf.RetentionPolicy{}, err
	}

	for _, rp := range rps {
		if rp.Name == name {
			return rp, nil
		}
	}
	return chronograf.RetentionPolicy{}, fmt.Errorf("unknown retention policy")
}

// CreateRP creates a retention policy for a specific database
func (c *Client) CreateRP(ctx context.Context, database string, rp *chronograf.RetentionPolicy) (*chronograf.RetentionPolicy, error) {
	query := fmt.Sprintf(`CREATE RETENTION POLICY "%s" ON "%s" DURATION %s REPLICATION %d`, rp.Name, database, rp.Duration, rp.Replication)
	if len(rp.ShardDuration) != 0 {
		query = fmt.Sprintf(`%s SHARD DURATION %s`, query, rp.ShardDuration)
	}

	if rp.Default {
		query = fmt.Sprintf(`%s DEFAULT`, query)
	}

	_, err := c.Query(ctx, chronograf.Query{
		Command: query,
		DB:      database,
	})
	if err != nil {
		return nil, err
	}

	res, err := c.getRP(ctx, database, rp.Name)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// UpdateRP updates a specific retention policy for a specific database
func (c *Client) UpdateRP(ctx context.Context, database string, name string, rp *chronograf.RetentionPolicy) (*chronograf.RetentionPolicy, error) {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf(`ALTER RETENTION POLICY "%s" ON "%s"`, name, database))
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
	queryRes, err := c.Query(ctx, chronograf.Query{
		Command: buffer.String(),
		DB:      database,
		RP:      name,
	})
	if err != nil {
		return nil, err
	}

	// The ALTER RETENTION POLICIES statements puts the error within the results itself
	// So, we have to crack open the results to see what happens
	octets, err := queryRes.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := make([]struct{ Error string }, 0)
	if err := json.Unmarshal(octets, &results); err != nil {
		return nil, err
	}

	// At last, we can check if there are any error strings
	for _, r := range results {
		if r.Error != "" {
			return nil, fmt.Errorf(r.Error)
		}
	}

	res, err := c.getRP(ctx, database, rp.Name)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// DropRP removes a specific retention policy for a specific database
func (c *Client) DropRP(ctx context.Context, database string, rp string) error {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP RETENTION POLICY "%s" ON "%s"`, rp, database),
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
		Command: fmt.Sprintf(`SHOW RETENTION POLICIES ON "%s"`, name),
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
