package statement

import (
	"fmt"
	"strings"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/stress/v2/ponyExpress"
)

// SetStatement set state variables for the test
type SetStatement struct {
	Var   string
	Value string

	StatementID string

	Tracer *ponyExpress.Tracer
}

// SetID statisfies the Statement Interface
func (i *SetStatement) SetID(s string) {
	i.StatementID = s
}

// Run statisfies the Statement Interface
func (i *SetStatement) Run(s *ponyExpress.StoreFront) {

	// Set the Tracer
	i.Tracer = ponyExpress.NewTracer(make(map[string]string))

	// Create a new Directive
	d := ponyExpress.NewDirective(strings.ToLower(i.Var), strings.ToLower(i.Value), i.Tracer)

	switch d.Property {

	// Needs to be set on both StoreFront and ponyExpress
	// Set the write percison for points generated
	case "precision":
		s.Precision = d.Value

		// Increment the tracer
		i.Tracer.Add(1)
		s.SendDirective(d)

	// Lives on StoreFront
	// Set the date for the first point entered into the database
	case "startdate":
		s.Lock()
		s.StartDate = d.Value
		s.Unlock()

	// Lives on StoreFront
	// Set the BatchSize for writes
	case "batchsize":
		s.Lock()
		s.BatchSize = parseInt(d.Value)
		s.Unlock()

	// Lives on StoreFront
	// Reset the ResultsClient to have a new address
	case "resultsaddress":
		s.Lock()
		s.SetResultsClient(influx.HTTPConfig{Addr: fmt.Sprintf("http://%v/", d.Value)})
		s.Unlock()

	// TODO: Make TestName actually change the reporting DB
	// Lives on StoreFront
	// Set the TestName that controls reporting DB
	case "testname":
		s.Lock()
		s.TestName = d.Value
		s.Unlock()

	// All other variables live on ponyExpress
	default:
		// Increment the tracer
		i.Tracer.Add(1)
		s.SendDirective(d)
	}
	i.Tracer.Wait()
}

// Report statisfies the Statement Interface
func (i *SetStatement) Report(s *ponyExpress.StoreFront) string {
	return fmt.Sprintf("SET %v = '%v'", i.Var, i.Value)
}
