package influx

import (
  "encoding/json"
  "context"
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

func (c *Client) DropDB(ctx context.Context, name string) error {
  _, err := c.Query(ctx, chronograf.Query{
    Command: fmt.Sprintf(`DROP DATABASE "%s"`, name),
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
