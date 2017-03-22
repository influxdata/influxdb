package influx

import (
  "encoding/json"
  "context"

  "github.com/influxdata/chronograf"
)

func (c *Client) AllDB(ctx context.Context) ([]chronograf.Database, error) {
  databases, err := c.showDatabases(ctx)
  if err != nil {
    return nil, err
  }

  return databases, nil
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
