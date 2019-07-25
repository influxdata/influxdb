package notification

import (
	"encoding/json"
	"strings"

	"github.com/influxdata/influxdb"
)

// StatusRule includes parametes of status rules.
type StatusRule struct {
	CurrentLevel  LevelRule  `json:"currentLevel"`
	PreviousLevel *LevelRule `json:"previousLevel"`
	// Alert when >= Count per Period
	Count  int               `json:"count"`
	Period influxdb.Duration `json:"period"`
}

// LevelRule is a pair of level + operation.
type LevelRule struct {
	CheckLevel CheckLevel      `json:"level"`
	Operation  StatusOperation `json:"operation"`
}

// StatusOperation is either equal or notequal
type StatusOperation bool

// MarshalJSON implements json.Marshaler interface.
func (op StatusOperation) MarshalJSON() ([]byte, error) {
	if op {
		return json.Marshal("equal")
	}
	return json.Marshal("notequal")
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (op *StatusOperation) UnmarshalJSON(b []byte) error {
	var ss string
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	*op = (ss == "equal")
	return nil
}

// CheckLevel is the enum value of status levels.
type CheckLevel int

// consts of CheckStatusLevel
const (
	Unknown CheckLevel = iota
	Ok
	Info
	Critical
	Warn
)

var checkLevels = []string{
	"UNKNOWN",
	"OK",
	"INFO",
	"CRIT",
	"WARN",
}

var checkLevelMaps = map[string]CheckLevel{
	"UNKNOWN": Unknown,
	"OK":      Ok,
	"INFO":    Info,
	"CRIT":    Critical,
	"WARN":    Warn,
}

// MarshalJSON implements json.Marshaller.
func (cl CheckLevel) MarshalJSON() ([]byte, error) {
	return json.Marshal(cl.String())
}

// UnmarshalJSON implements json.Unmarshaller.
func (cl *CheckLevel) UnmarshalJSON(b []byte) error {
	var ss string
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	*cl = ParseCheckLevel(strings.ToUpper(ss))
	return nil
}

// String returns the string value, invalid CheckLevel will return Unknown.
func (cl CheckLevel) String() string {
	if cl < Unknown || cl > Warn {
		cl = Unknown
	}
	return checkLevels[cl]
}

// ParseCheckLevel will parse the string to checkLevel
func ParseCheckLevel(s string) CheckLevel {
	if cl, ok := checkLevelMaps[s]; ok {
		return cl
	}
	return Unknown
}
