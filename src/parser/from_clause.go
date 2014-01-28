package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"
import (
	"bytes"
	"strings"
)
import "fmt"

type FromClauseType int

const (
	FromClauseArray     FromClauseType = C.FROM_ARRAY
	FromClauseMerge     FromClauseType = C.FROM_MERGE
	FromClauseInnerJoin FromClauseType = C.FROM_INNER_JOIN
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
}

func (self *FromClause) GetString() string {
	buffer := bytes.NewBufferString("")
	switch self.Type {
	case FromClauseMerge:
		fmt.Fprintf(buffer, "%s%s merge %s %s", self.Names[0].Name.GetString(), self.Names[1].GetAliasString(),
			self.Names[1].Name.GetString(), self.Names[1].GetAliasString())
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
			names = append(names, fmt.Sprintf("%s%s", t.Name.GetString(), alias))
		}
		buffer.WriteString(strings.Join(names, ","))
	}
	return buffer.String()
}
