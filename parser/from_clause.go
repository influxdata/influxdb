package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"
import (
	"bytes"
	"regexp"
	"strings"
)
import "fmt"

type FromClauseType int

const (
	FromClauseArray      FromClauseType = C.FROM_ARRAY
	FromClauseMerge      FromClauseType = C.FROM_MERGE
	FromClauseInnerJoin  FromClauseType = C.FROM_JOIN
	FromClauseMergeRegex FromClauseType = C.FROM_MERGE_REGEX
	FromClauseJoinRegex  FromClauseType = C.FROM_JOIN_REGEX
)

func (self *TableName) GetAlias() string {
	if self.Alias != "" {
		return self.Alias
	}
	return self.Name.Name
}

func (self *TableName) GetAliasString() string {
	if self.Alias != "" {
		return fmt.Sprintf(" as %s", self.Alias)
	}
	return ""
}

type TableName struct {
	Name  *Value
	Alias string
}

type FromClause struct {
	Type  FromClauseType
	Names []*TableName
	Regex *regexp.Regexp
}

func (self *FromClause) GetString() string {
	buffer := bytes.NewBufferString("")
	switch self.Type {
	case FromClauseMerge:
		fmt.Fprintf(buffer, "merge(")
		for i, n := range self.Names {
			if i > 0 {
				buffer.WriteRune(',')
			}
			buffer.WriteString(n.Name.GetString())
		}
		buffer.WriteString(")")
	case FromClauseInnerJoin:
		fmt.Fprintf(buffer, "%s%s inner join %s%s", self.Names[0].Name.GetString(), self.Names[0].GetAliasString(),
			self.Names[1].Name.GetString(), self.Names[1].GetAliasString())
	default:
		names := make([]string, 0, len(self.Names))
		for _, t := range self.Names {
			alias := ""
			if t.Alias != "" {
				alias = fmt.Sprintf(" as %s", t.Alias)
			}
			if t.Name.Type == ValueRegex {
				names = append(names, fmt.Sprintf(`%s%s`, t.Name.GetString(), alias))
				continue
			}
			names = append(names, fmt.Sprintf(`"%s"%s`, t.Name.GetString(), alias))
		}
		buffer.WriteString(strings.Join(names, ","))
	}
	return buffer.String()
}
