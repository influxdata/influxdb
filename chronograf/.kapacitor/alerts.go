package kapacitor

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/pipeline/tick"
)

// AlertServices generates alert chaining methods to be attached to an alert from all rule Services
func AlertServices(rule chronograf.AlertRule) (string, error) {
	node, err := addAlertNodes(rule.AlertNodes)
	if err != nil {
		return "", err
	}

	if err := ValidateAlert(node); err != nil {
		return "", err
	}
	return node, nil
}

func addAlertNodes(handlers chronograf.AlertNodes) (string, error) {
	octets, err := json.Marshal(&handlers)
	if err != nil {
		return "", err
	}

	stream := &pipeline.StreamNode{}
	pipe := pipeline.CreatePipelineSources(stream)
	from := stream.From()
	node := from.Alert()
	if err = json.Unmarshal(octets, node); err != nil {
		return "", err
	}

	aster := tick.AST{}
	err = aster.Build(pipe)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	aster.Program.Format(&buf, "", false)
	rawTick := buf.String()
	return toOldSchema(rawTick), nil
}

var (
	removeID      = regexp.MustCompile(`(?m)\s*\.id\(.*\)$`)      // Remove to use ID variable
	removeMessage = regexp.MustCompile(`(?m)\s*\.message\(.*\)$`) // Remove to use message variable
	removeDetails = regexp.MustCompile(`(?m)\s*\.details\(.*\)$`) // Remove to use details variable
	removeHistory = regexp.MustCompile(`(?m)\s*\.history\(21\)$`) // Remove default history
)

func toOldSchema(rawTick string) string {
	rawTick = strings.Replace(rawTick, "stream\n    |from()\n    |alert()", "", -1)
	rawTick = removeID.ReplaceAllString(rawTick, "")
	rawTick = removeMessage.ReplaceAllString(rawTick, "")
	rawTick = removeDetails.ReplaceAllString(rawTick, "")
	rawTick = removeHistory.ReplaceAllString(rawTick, "")
	return rawTick
}
