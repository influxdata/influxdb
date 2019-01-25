package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/kit/check"
	"github.com/spf13/cobra"
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
