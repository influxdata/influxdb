package v8

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/influxdb/influxdb/client"
)

const batchSize = 5000

// Config is the config used to initialize a Importer importer
type Config struct {
	Username         string
	Password         string
	URL              url.URL
	Precision        string
	WriteConsistency string
	Path             string
	Version          string
	Compressed       bool
}

// NewConfig returns an initialized *Config
func NewConfig() *Config {
	return &Config{}
}

// Importer is the importer used for importing 0.8 data
type Importer struct {
	client          *client.Client
	database        string
	retentionPolicy string
	config          *Config
	batch           []string
	totalInserts    int
	failedInserts   int
	totalCommands   int
}

// NewImporter will return an intialized Importer struct
func NewImporter(config *Config) *Importer {
	return &Importer{
		config: config,
		batch:  make([]string, 0, batchSize),
	}
}

// Import processes the specified file in the Config and writes the data to the databases in chunks specified by batchSize
func (i *Importer) Import() error {
	// Create a client and try to connect
	config := client.NewConfig()
	config.URL = i.config.URL
	config.Username = i.config.Username
	config.Password = i.config.Password
	config.UserAgent = fmt.Sprintf("influxDB importer/%s", i.config.Version)
	cl, err := client.NewClient(config)
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
			log.Printf("Processed %d commands\n", i.totalCommands)
			log.Printf("Processed %d inserts\n", i.totalInserts)
			log.Printf("Failed %d inserts\n", i.failedInserts)
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
	scanner := bufio.NewScanner(r)

	// Process the scanner
	i.processDDL(scanner)
	i.processDML(scanner)

	// Check if we had any errors scanning the file
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	return nil
}

func (i *Importer) processDDL(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Text()
		// If we find the DML token, we are done with DDL
		if strings.HasPrefix(line, "# DML") {
			return
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		i.queryExecutor(line)
	}
}

func (i *Importer) processDML(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "# CONTEXT-DATABASE:") {
			i.database = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "# CONTEXT-RETENTION-POLICY:") {
			i.retentionPolicy = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		i.batchAccumulator(line)
	}
}

func (i *Importer) execute(command string) {
	response, err := i.client.Query(client.Query{Command: command, Database: i.database})
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if err := response.Error(); err != nil {
		log.Printf("error: %s\n", response.Error())
	}
}

func (i *Importer) queryExecutor(command string) {
	i.totalCommands++
	i.execute(command)
}

func (i *Importer) batchAccumulator(line string) {
	i.batch = append(i.batch, line)
	if len(i.batch) == batchSize {
		if e := i.batchWrite(); e != nil {
			log.Println("error writing batch: ", e)
			// Output failed lines to STDOUT so users can capture lines that failed to import
			fmt.Println(strings.Join(i.batch, "\n"))
			i.failedInserts += len(i.batch)
		} else {
			i.totalInserts += len(i.batch)
		}
		i.batch = i.batch[:0]
	}
}

func (i *Importer) batchWrite() error {
	_, e := i.client.WriteLineProtocol(strings.Join(i.batch, "\n"), i.database, i.retentionPolicy, i.config.Precision, i.config.WriteConsistency)
	return e
}
