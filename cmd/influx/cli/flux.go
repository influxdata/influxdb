package cli

import (
	"context"
	"net/url"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/client"
)

func getFluxREPL(u url.URL, username, password string) (*repl.REPL, error) {
	builtin.Initialize()

	c, err := client.NewHTTP(u)
	if err != nil {
		return nil, err
	}
	c.Username = username
	c.Password = password

	deps := flux.NewDefaultDependencies()
	deps.Deps.HTTPClient = c

	// might need: deps.Inject(ctx)
	return repl.New(context.Background(), deps), nil
}
