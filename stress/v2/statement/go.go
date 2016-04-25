package statement

import (
	"fmt"

	"github.com/influxdata/influxdb/stress/v2/ponyExpress"
)

// GoStatement is a Statement Implementation to allow other statements to be run concurrently
type GoStatement struct {
	Statement

	StatementID string
}

// SetID statisfies the Statement Interface
func (i *GoStatement) SetID(s string) {
	i.StatementID = s
}

// Run statisfies the Statement Interface
func (i *GoStatement) Run(s *ponyExpress.StoreFront) {
	s.Add(1)
	go func() {
		i.Statement.Run(s)
		s.Done()
	}()
}

// Report statisfies the Statement Interface
func (i *GoStatement) Report(s *ponyExpress.StoreFront) string {
	return fmt.Sprintf("Go %v", i.Statement.Report(s))
}
