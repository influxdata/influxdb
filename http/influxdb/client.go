package influxdb

import (
	"net/url"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/influx"
)

func newClient(s *platform.Source) (*influx.Client, error) {
	c := &influx.Client{}
	url, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	c.URL = url
	c.Authorizer = DefaultAuthorization(s)
	c.InsecureSkipVerify = s.InsecureSkipVerify
	c.Logger = &chronograf.NoopLogger{}
	return c, nil
}

// DefaultAuthorization creates either a shared JWT builder, basic auth or Noop
// This is copy of the method from chronograf/influx adapted for platform sources.
func DefaultAuthorization(src *platform.Source) influx.Authorizer {
	// Optionally, add the shared secret JWT token creation
	if src.Username != "" && src.SharedSecret != "" {
		return &influx.BearerJWT{
			Username:     src.Username,
			SharedSecret: src.SharedSecret,
		}
	} else if src.Username != "" && src.Password != "" {
		return &influx.BasicAuth{
			Username: src.Username,
			Password: src.Password,
		}
	}
	return &influx.NoAuthorization{}
}
