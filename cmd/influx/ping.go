package main

import (
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/kit/check"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"time"
)

var pingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Check the InfluxDB /health endpoint",
	Long:  `Checks the health of a running InfluxDB instance by querying /health. Does not require valid token.`,
	RunE:  pingF,
}

func pingF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for ping command")
	}

	c := http.Client{
		Timeout: 5 * time.Second,
	}
	url := flags.host + "/health"
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("got %d from '%s'", resp.StatusCode, url)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var healthResponse check.Response
	if err = json.Unmarshal(b, &healthResponse); err != nil {
		return err
	}

	if healthResponse.Status == check.StatusPass {
		fmt.Println("OK")
	} else {
		return fmt.Errorf("health check failed: '%s'", healthResponse.Message)
	}

	return nil
}
