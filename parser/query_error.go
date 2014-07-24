package parser

import (
	"fmt"
	"strings"
)

type QueryError struct {
	queryString string
	firstLine   int
	firstColumn int
	lastLine    int
	lastColumn  int
	errorString string
}

func (self *QueryError) Error() string {
	return fmt.Sprintf("Error at %d:%d %d:%d. %s", self.firstLine, self.firstColumn, self.lastLine, self.lastColumn, self.errorString)
}
func (self *QueryError) PrettyPrint() string {
	return fmt.Sprintf("%s\n%s\n%s%s", self.errorString, self.queryString, strings.Repeat(" ", self.firstColumn), strings.Repeat("^", self.lastColumn-self.firstColumn))
}
