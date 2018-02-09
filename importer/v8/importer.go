// Package v8 contains code for importing data from 0.8 instances of InfluxDB.
package v8 // import "github.com/influxdata/influxdb/importer/v8"

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client"
)

const batchSize = 5000

// Config is the config used to initialize a Importer importer
type Config struct {
	Path       string // Path to import data.
	Version    string
	Compressed bool // Whether import data is gzipped.
	PPS        int  // points per second importer imports with.

	client.Config
}

// NewConfig returns an initialized *Config
func NewConfig() Config {
	return Config{Config: client.NewConfig()}
}

// Importer is the importer used for importing 0.8 data
type Importer struct {
	client                *client.Client
	database              string
	retentionPolicy       string
	config                Config
	batch                 []string
	totalInserts          int
	failedInserts         int
	totalCommands         int
	throttlePointsWritten int
	startTime             time.Time
	lastWrite             time.Time
	throttle              *time.Ticker

	stderrLogger *log.Logger
	stdoutLogger *log.Logger
}

// NewImporter will return an intialized Importer struct
func NewImporter(config Config) *Importer {
	config.UserAgent = fmt.Sprintf("influxDB importer/%s", config.Version)
	return &Importer{
		config:       config,
		batch:        make([]string, 0, batchSize),
		stdoutLogger: log.New(os.Stdout, "", log.LstdFlags),
		stderrLogger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Import processes the specified file in the Config and writes the data to the databases in chunks specified by batchSize
func (i *Importer) Import() error {
	// Create a client and try to connect.
	cl, err := client.NewClient(i.config.Config)
	if err != nil {
		return fmt.Errorf("could not create client %s", err)
	}
	i.client = cl
	if _, _, e := i.client.Ping(); e != nil {
		return fmt.Errorf("failed to connect to %s\n", i.client.Addr())
	}

	// Validate args
	if i.config.Path == "" {
		return fmt.Errorf("file argument required")
	}

	defer func() {
		if i.totalInserts > 0 {
			i.stdoutLogger.Printf("Processed %d commands\n", i.totalCommands)
			i.stdoutLogger.Printf("Processed %d inserts\n", i.totalInserts)
			i.stdoutLogger.Printf("Failed %d inserts\n", i.failedInserts)
		}
	}()

	// Open the file
	f, err := os.Open(i.config.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader

	// If gzipped, wrap in a gzip reader
	if i.config.Compressed {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gr.Close()
		// Set the reader to the gzip reader
		r = gr
	} else {
		// Standard text file so our reader can just be the file
		r = f
	}

	// Get our reader
	scanner := bufio.NewReader(r)

	// Process the DDL
	if err := i.processDDL(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	// Set up our throttle channel.  Since there is effectively no other activity at this point
	// the smaller resolution gets us much closer to the requested PPS
	i.throttle = time.NewTicker(time.Microsecond)
	defer i.throttle.Stop()

	// Prime the last write
	i.lastWrite = time.Now()

	// Process the DML
	if err := i.processDML(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	// If there were any failed inserts then return an error so that a non-zero
	// exit code can be returned.
	if i.failedInserts > 0 {
		plural := " was"
		if i.failedInserts > 1 {
			plural = "s were"
		}

		return fmt.Errorf("%d point%s not inserted", i.failedInserts, plural)
	}

	return nil
}

func (i *Importer) processDDL(scanner *bufio.Reader) error {
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			return nil
		}
		// If we find the DML token, we are done with DDL
		if strings.HasPrefix(line, "# DML") {
			return nil
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		i.queryExecutor(line)
	}
}

func (i *Importer) processDML(scanner *bufio.Reader) error {
	i.startTime = time.Now()
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			// Call batchWrite one last time to flush anything out in the batch
			i.batchWrite()
			return nil
		}
		if strings.HasPrefix(line, "# CONTEXT-DATABASE:") {
			i.batchWrite()
			i.database = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "# CONTEXT-RETENTION-POLICY:") {
			i.batchWrite()
			i.retentionPolicy = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		i.batchAccumulator(line)
	}
}

func (i *Importer) execute(command string) {
	response, err := i.client.Query(client.Query{Command: command, Database: i.database})
	if err != nil {
		i.stderrLogger.Printf("error: %s\n", err)
		return
	}
	if err := response.Error(); err != nil {
		i.stderrLogger.Printf("error: %s\n", response.Error())
	}
}

func (i *Importer) queryExecutor(command string) {
	i.totalCommands++
	i.execute(command)
}

func (i *Importer) batchAccumulator(line string) {
	i.batch = append(i.batch, line)
	if len(i.batch) == batchSize {
		i.batchWrite()
	}
}

func (i *Importer) batchWrite() {
	// Exit early if there are no points in the batch.
	if len(i.batch) == 0 {
		return
	}

	// Accumulate the batch size to see how many points we have written this second
	i.throttlePointsWritten += len(i.batch)

	// Find out when we last wrote data
	since := time.Since(i.lastWrite)

	// Check to see if we've exceeded our points per second for the current timeframe
	var currentPPS int
	if since.Seconds() > 0 {
		currentPPS = int(float64(i.throttlePointsWritten) / since.Seconds())
	} else {
		currentPPS = i.throttlePointsWritten
	}

	// If our currentPPS is greater than the PPS specified, then we wait and retry
	if int(currentPPS) > i.config.PPS && i.config.PPS != 0 {
		// Wait for the next tick
		<-i.throttle.C

		// Decrement the batch size back out as it is going to get called again
		i.throttlePointsWritten -= len(i.batch)
		i.batchWrite()
		return
	}

	_, e := i.client.WriteLineProtocol(strings.Join(i.batch, "\n"), i.database, i.retentionPolicy, i.config.Precision, i.config.WriteConsistency)
	if e != nil {
		i.stderrLogger.Println("error writing batch: ", e)
		i.stderrLogger.Println(strings.Join(i.batch, "\n"))
		i.failedInserts += len(i.batch)
	} else {
		i.totalInserts += len(i.batch)
	}
	i.throttlePointsWritten = 0
	i.lastWrite = time.Now()

	// Clear the batch and record the number of processed points.
	i.batch = i.batch[:0]
	// Give some status feedback every 100000 lines processed
	processed := i.totalInserts + i.failedInserts
	if processed%100000 == 0 {
		since := time.Since(i.startTime)
		pps := float64(processed) / since.Seconds()
		i.stdoutLogger.Printf("Processed %d lines.  Time elapsed: %s.  Points per second (PPS): %d", processed, since.String(), int64(pps))
	}
}
