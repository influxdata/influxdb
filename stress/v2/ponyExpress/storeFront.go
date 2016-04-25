package ponyExpress

import (
	"fmt"
	"log"
	"sync"

	influx "github.com/influxdata/influxdb/client/v2"
)

// NewStoreFront creates the backend for the stress test
func NewStoreFront() *StoreFront {

	// Make the Package and Directive chans
	packageCh := make(chan Package, 0)
	directiveCh := make(chan Directive, 0)

	// Make the Response chan
	responseCh := make(chan Response, 0)

	s := &StoreFront{
		TestName:  "DefaultTestName",
		Precision: "s",
		StartDate: "2016-01-02",
		BatchSize: 5000,

		packageChan:   packageCh,
		directiveChan: directiveCh,

		ResultsChan: responseCh,
		communes:    make(map[string]*commune),
		TestID:      randStr(10),
	}

	// Set the results instance to localhost:8086 by default
	s.SetResultsClient(influx.HTTPConfig{
		Addr: fmt.Sprintf("http://%v/", "localhost:8086"),
	})

	// Start the client service
	startPonyExpress(packageCh, directiveCh, responseCh, s.TestID)

	// Listen for Results coming in
	s.resultsListen()

	return s
}

// NewTestStoreFront returns a StoreFront to be used for testing Statements
func NewTestStoreFront() (*StoreFront, chan Package, chan Directive) {

	packageCh := make(chan Package, 0)
	directiveCh := make(chan Directive, 0)

	s := &StoreFront{
		TestName:  "DefaultTestName",
		Precision: "s",
		StartDate: "2016-01-02",
		BatchSize: 5000,

		directiveChan: directiveCh,
		packageChan:   packageCh,

		communes: make(map[string]*commune),
		TestID:   randStr(10),
	}

	return s, packageCh, directiveCh
}

// The StoreFront is the Statement facing API that consume Statement output and coordinates the test results
type StoreFront struct {
	TestID   string
	TestName string

	Precision string
	StartDate string
	BatchSize int

	sync.WaitGroup
	sync.Mutex

	packageChan   chan<- Package
	directiveChan chan<- Directive

	ResultsChan   chan Response
	communes      map[string]*commune
	ResultsClient influx.Client
}

// SendPackage is the public facing API for to send Queries and Points
func (sf *StoreFront) SendPackage(p Package) {
	sf.packageChan <- p
}

// SendDirective is the public facing API to set state variables in the test
func (sf *StoreFront) SendDirective(d Directive) {
	sf.directiveChan <- d
}

// Starts a go routine that listens for Results
func (sf *StoreFront) resultsListen() {

	// Make sure databases for results are created
	sf.createDatabase(fmt.Sprintf("_%v", sf.TestName))
	sf.createDatabase(sf.TestName)

	// Listen for Responses
	go func() {

		// Prepare a BatchPointsConfig
		bpconf := influx.BatchPointsConfig{
			Database:  fmt.Sprintf("_%v", sf.TestName),
			Precision: "ns",
		}

		// Prepare the first batch of points
		bp, _ := influx.NewBatchPoints(bpconf)

		// TODO: Panics on resp.Tracer.Done() if there are too many 500s in a row
		// Loop over ResultsChan
		for resp := range sf.ResultsChan {
			switch resp.Point.Name() {
			// If the done point comes down the channel write the results
			case "done":
				sf.ResultsClient.Write(bp)
				// Decrement the tracer
				resp.Tracer.Done()
			// By default fall back to the batcher
			default:
				// Add the StoreFront tags
				pt := resp.AddTags(sf.tags())
				// Add the point to the batch
				bp = sf.batcher(pt, bp, bpconf)
				// Decrement the tracer
				resp.Tracer.Done()
			}
		}

	}()
}

// Batches incoming Result.Point and sends them if the batch reaches 5k in sizes
func (sf *StoreFront) batcher(pt *influx.Point, bp influx.BatchPoints, bpconf influx.BatchPointsConfig) influx.BatchPoints {
	// If fewer than 5k add point and return
	if len(bp.Points()) <= 5000 {
		bp.AddPoint(pt)
	} else {
		// Otherwise send the batch
		err := sf.ResultsClient.Write(bp)

		// Check error
		if err != nil {
			log.Fatalf("Error writing performance stats\n  error: %v\n", err)
		}

		// Reset the batch of points
		bp, _ = influx.NewBatchPoints(bpconf)
	}
	return bp
}

// SetResultsClient is the utility for reseting the address of the ResultsClient
func (sf *StoreFront) SetResultsClient(conf influx.HTTPConfig) {
	clnt, err := influx.NewHTTPClient(conf)
	if err != nil {
		log.Fatalf("Error resetting results clien\n  error: %v\n", err)
	}
	sf.ResultsClient = clnt
}

// Convinence database creation function
func (sf *StoreFront) createDatabase(db string) {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %v", db)
	sf.ResultsClient.Query(influx.Query{Command: query})
}

// GetStatementResults is a convinence function for fetching all results given a StatementID
func (sf *StoreFront) GetStatementResults(sID, t string) (res []influx.Result) {
	// Make the template string
	qryStr := fmt.Sprintf(`SELECT * FROM "%v" WHERE statement_id = '%v'`, t, sID)
	// Make the query and return the results
	return sf.queryTestResults(qryStr)
}

//  Runs given qry on the test results database and returns the results or nil in case of error
func (sf *StoreFront) queryTestResults(qry string) (res []influx.Result) {
	q := influx.Query{
		Command:  qry,
		Database: fmt.Sprintf("_%v", sf.TestName),
	}

	response, err := sf.ResultsClient.Query(q)

	if err == nil {
		if response.Error() != nil {
			log.Fatalf("Error sending results query\n  error: %v\n", response.Error())
		}
	}

	res = response.Results

	// If there are no results this indicates some kind of error
	if res[0].Series == nil {
		return nil
	}

	return res
}
