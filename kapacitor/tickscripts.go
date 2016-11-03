package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

var _ chronograf.Ticker = &Alert{}

// Alert defines alerting strings in template rendering
type Alert struct {
	Trigger   string // Specifies the type of alert
	Service   string // Alerting service
	Operator  string // Operator for alert comparison
	Aggregate string // Statistic aggregate over window of data
}

// Generate creates a Tickscript from the alertrule
func (a *Alert) Generate(rule chronograf.AlertRule) (chronograf.TICKScript, error) {
	vars, err := Vars(rule)
	if err != nil {
		return "", nil
	}
	data, err := Data(rule.Query)
	if err != nil {
		return "", nil
	}
	trigger, err := Trigger(rule)
	if err != nil {
		return "", err
	}
	services, err := AlertServices(rule)
	if err != nil {
		return "", err
	}
	output := InfluxOut(rule)
	raw := fmt.Sprintf("%s\n%s\n%s%s\n%s", vars, data, trigger, services, output)
	return formatTick(raw)
}
