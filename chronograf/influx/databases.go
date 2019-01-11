package influx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// AllDB returns all databases from within Influx
func (c *Client) AllDB(ctx context.Context) ([]chronograf.Database, error) {
	return c.showDatabases(ctx)
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
func (c *Client) DropDB(ctx context.Context, db string) error {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP DATABASE "%s"`, db),
		DB:      db,
	})
	if err != nil {
		return err
	}
	return nil
}

// AllRP returns all the retention policies for a specific database
func (c *Client) AllRP(ctx context.Context, db string) ([]chronograf.RetentionPolicy, error) {
	return c.showRetentionPolicies(ctx, db)
}

func (c *Client) getRP(ctx context.Context, db, rp string) (chronograf.RetentionPolicy, error) {
	rs, err := c.AllRP(ctx, db)
	if err != nil {
		return chronograf.RetentionPolicy{}, err
	}

	for _, r := range rs {
		if r.Name == rp {
			return r, nil
		}
	}
	return chronograf.RetentionPolicy{}, fmt.Errorf("unknown retention policy")
}

// CreateRP creates a retention policy for a specific database
func (c *Client) CreateRP(ctx context.Context, db string, rp *chronograf.RetentionPolicy) (*chronograf.RetentionPolicy, error) {
	query := fmt.Sprintf(`CREATE RETENTION POLICY "%s" ON "%s" DURATION %s REPLICATION %d`, rp.Name, db, rp.Duration, rp.Replication)
	if len(rp.ShardDuration) != 0 {
		query = fmt.Sprintf(`%s SHARD DURATION %s`, query, rp.ShardDuration)
	}

	if rp.Default {
		query = fmt.Sprintf(`%s DEFAULT`, query)
	}

	_, err := c.Query(ctx, chronograf.Query{
		Command: query,
		DB:      db,
	})
	if err != nil {
		return nil, err
	}

	res, err := c.getRP(ctx, db, rp.Name)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// UpdateRP updates a specific retention policy for a specific database
func (c *Client) UpdateRP(ctx context.Context, db string, rp string, upd *chronograf.RetentionPolicy) (*chronograf.RetentionPolicy, error) {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf(`ALTER RETENTION POLICY "%s" ON "%s"`, rp, db))
	if len(upd.Duration) > 0 {
		buffer.WriteString(" DURATION " + upd.Duration)
	}
	if upd.Replication > 0 {
		buffer.WriteString(" REPLICATION " + fmt.Sprint(upd.Replication))
	}
	if len(upd.ShardDuration) > 0 {
		buffer.WriteString(" SHARD DURATION " + upd.ShardDuration)
	}
	if upd.Default {
		buffer.WriteString(" DEFAULT")
	}
	queryRes, err := c.Query(ctx, chronograf.Query{
		Command: buffer.String(),
		DB:      db,
		RP:      rp,
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

	res, err := c.getRP(ctx, db, upd.Name)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// DropRP removes a specific retention policy for a specific database
func (c *Client) DropRP(ctx context.Context, db string, rp string) error {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP RETENTION POLICY "%s" ON "%s"`, rp, db),
		DB:      db,
		RP:      rp,
	})
	if err != nil {
		return err
	}
	return nil
}

// GetMeasurements returns measurements in a specified database, paginated by
// optional limit and offset. If no limit or offset is provided, it defaults to
// a limit of 100 measurements with no offset.
func (c *Client) GetMeasurements(ctx context.Context, db string, limit, offset int) ([]chronograf.Measurement, error) {
	return c.showMeasurements(ctx, db, limit, offset)
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

func (c *Client) showRetentionPolicies(ctx context.Context, db string) ([]chronograf.RetentionPolicy, error) {
	retentionPolicies, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`SHOW RETENTION POLICIES ON "%s"`, db),
		DB:      db,
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

func (c *Client) showMeasurements(ctx context.Context, db string, limit, offset int) ([]chronograf.Measurement, error) {
	show := fmt.Sprintf(`SHOW MEASUREMENTS ON "%s"`, db)
	if limit > 0 {
		show += fmt.Sprintf(" LIMIT %d", limit)
	}

	if offset > 0 {
		show += fmt.Sprintf(" OFFSET %d", offset)
	}

	measurements, err := c.Query(ctx, chronograf.Query{
		Command: show,
		DB:      db,
	})

	if err != nil {
		return nil, err
	}
	octets, err := measurements.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := showResults{}
	if err := json.Unmarshal(octets, &results); err != nil {
		return nil, err
	}

	return results.Measurements(), nil
}
