package notification

import (
	"encoding/json"
	"strings"
)

// StatusRule includes parameters of status rules.
type StatusRule struct {
	CurrentLevel  CheckLevel  `json:"currentLevel"`
	PreviousLevel *CheckLevel `json:"previousLevel"`
}

// CheckLevel is the enum value of status levels.
type CheckLevel int

// consts of CheckStatusLevel
const (
	Unknown CheckLevel = iota
	Ok
	Info
	Warn
	Critical
	Any
)

var checkLevels = []string{
	"UNKNOWN",
	"OK",
	"INFO",
	"WARN",
	"CRIT",
	"ANY",
}

var checkLevelMaps = map[string]CheckLevel{
	"UNKNOWN": Unknown,
	"OK":      Ok,
	"INFO":    Info,
	"WARN":    Warn,
	"CRIT":    Critical,
	"ANY":     Any,
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
	if cl < Unknown || cl > Any {
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
