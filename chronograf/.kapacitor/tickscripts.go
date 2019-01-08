package kapacitor

import (
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

var _ chronograf.Ticker = &Alert{}

// Alert defines alerting strings in template rendering
type Alert struct{}

// Generate creates a Tickscript from the alertrule
func (a *Alert) Generate(rule chronograf.AlertRule) (chronograf.TICKScript, error) {
	vars, err := Vars(rule)
	if err != nil {
		return "", err
	}
	data, err := Data(rule)
	if err != nil {
		return "", err
	}
	trigger, err := Trigger(rule)
	if err != nil {
		return "", err
	}
	services, err := AlertServices(rule)
	if err != nil {
		return "", err
	}
	output, err := InfluxOut(rule)
	if err != nil {
		return "", err
	}
	http, err := HTTPOut(rule)
	if err != nil {
		return "", err
	}

	raw := fmt.Sprintf("%s\n%s\n%s%s\n%s\n%s", vars, data, trigger, services, output, http)
	tick, err := formatTick(raw)
	if err != nil {
		return "", err
	}
	if err := validateTick(tick); err != nil {
		return tick, err
	}
	return tick, nil
}
