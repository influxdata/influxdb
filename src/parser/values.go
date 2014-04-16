package parser

import "strings"

type Values []*Value

func (self Values) GetString() string {
	names := make([]string, 0, len(self))
	for _, name := range self {
		names = append(names, name.GetString())
	}
	return strings.Join(names, ",")
}
