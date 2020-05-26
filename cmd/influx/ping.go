package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/spf13/cobra"
)

func cmdPing(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	runE := func(cmd *cobra.Command, args []string) error {
		if flags.local {
			return fmt.Errorf("local flag not supported for ping command")
		}

		c := http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: flags.skipVerify},
			},
		}
		url := flags.Host + "/health"
		resp, err := c.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("got %d from '%s'", resp.StatusCode, url)
		}

		var healthResponse check.Response
		if err = json.NewDecoder(resp.Body).Decode(&healthResponse); err != nil {
			return err
		}

		if healthResponse.Status == check.StatusPass {
			fmt.Println("OK")
		} else {
			return fmt.Errorf("health check failed: '%s'", healthResponse.Message)
		}

		return nil
	}

	cmd := opts.newCmd("ping", runE, true)
	cmd.Short = "Check the InfluxDB /health endpoint"
	cmd.Long = `Checks the health of a running InfluxDB instance by querying /health. Does not require valid token.`

	return cmd
}
