package influxdb

import (
	"fmt"

	"code.google.com/p/log4go"
)

// TimePrecision represents a level of time precision.
type TimePrecision int

const (
	// MicrosecondPrecision is 1/1,000,000 th of a second.
	MicrosecondPrecision TimePrecision = iota
	// MillisecondPrecision is 1/1,000 th of a second.
	MillisecondPrecision
	// SecondPrecision is 1 second precision.
	SecondPrecision
)

func parseTimePrecision(s string) (TimePrecision, error) {
	switch s {
	case "u":
		return MicrosecondPrecision, nil
	case "m":
		log4go.Warn("time_precision=m will be disabled in future release, use time_precision=ms instead")
		fallthrough
	case "ms":
		return MillisecondPrecision, nil
	case "s":
		return SecondPrecision, nil
	case "":
		return MillisecondPrecision, nil
	}

	return 0, fmt.Errorf("Unknown time precision %s", s)
}

func hasDuplicates(ss []string) bool {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		if _, ok := m[s]; ok {
			return true
		}
		m[s] = struct{}{}
	}
	return false
}

func removeField(fields []string, name string) []string {
	index := -1
	for idx, field := range fields {
		if field == name {
			index = idx
			break
		}
	}

	if index == -1 {
		return fields
	}

	return append(fields[:index], fields[index+1:]...)
}

func removeTimestampFieldDefinition(fields []string) []string {
	fields = removeField(fields, "time")
	return removeField(fields, "sequence_number")
}

func mapKeyList(m interface{}) []string {

	switch m.(type) {
	case map[string]string:
		return mapStrStrKeyList(m.(map[string]string))
	case map[string]uint32:
		return mapStrUint32KeyList(m.(map[string]uint32))
	}
	return nil
}

func mapStrStrKeyList(m map[string]string) []string {
	l := make([]string, 0, len(m))
	for k := range m {
		l = append(l, k)
	}
	return l
}

func mapStrUint32KeyList(m map[string]uint32) []string {
	l := make([]string, 0, len(m))
	for k := range m {
		l = append(l, k)
	}
	return l
}
