package importer

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/influxdb/influxdb/client"
)

const batchSize = 5000

type V8Config struct {
	username, password string
	url                url.URL
	precision          string
	writeConsistency   string
	file, version      string
	compressed         bool
}

func NewV8Config(username, password, precision, writeConsistency, file, version string, u url.URL, compressed bool) *V8Config {
	return &V8Config{
		username:         username,
		password:         password,
		precision:        precision,
		writeConsistency: writeConsistency,
		file:             file,
		version:          version,
		url:              u,
		compressed:       compressed,
	}
}

type V8 struct {
	client                      *client.Client
	database                    string
	retentionPolicy             string
	config                      *V8Config
	wg                          sync.WaitGroup
	line, command               chan string
	done                        chan struct{}
	batch                       []string
	totalInserts, totalCommands int
}

func NewV8(config *V8Config) *V8 {
	return &V8{
		config:  config,
		done:    make(chan struct{}),
		line:    make(chan string),
		command: make(chan string),
		batch:   make([]string, 0, batchSize),
	}
}

func (v8 *V8) Import() error {
	// Create a client and try to connect
	config := client.NewConfig(v8.config.url, v8.config.username, v8.config.password, v8.config.version, client.DEFAULT_TIMEOUT)
	cl, err := client.NewClient(config)
	if err != nil {
		return fmt.Errorf("could not create client %s", err)
	}
	v8.client = cl
	if _, _, e := v8.client.Ping(); e != nil {
		return fmt.Errorf("failed to connect to %s\n", v8.client.Addr())
	}

	// Validate args
	if v8.config.file == "" {
		return fmt.Errorf("file argument required")
	}

	defer func() {
		v8.wg.Wait()
		if v8.totalInserts > 0 {
			log.Printf("Processed %d commands\n", v8.totalCommands)
			log.Printf("Processed %d inserts\n", v8.totalInserts)
		}
	}()

	// Open the file
	f, err := os.Open(v8.config.file)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader

	// If gzipped, wrap in a gzip reader
	if v8.config.compressed {
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

	// start our accumulator
	go v8.batchAccumulator()

	// start our command executor
	go v8.queryExecutor()

	// Get our reader
	scanner := bufio.NewScanner(r)

	// Process the scanner
	v8.processDDL(scanner)
	v8.processDML(scanner)

	// Signal go routines we are done
	close(v8.done)

	// Check if we had any errors scanning the file
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	return nil
}

func (v8 *V8) processDDL(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Text()
		// If we find the DML token, we are done with DDL
		if strings.HasPrefix(line, "# DML") {
			return
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		v8.command <- line
	}
}

func (v8 *V8) processDML(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		v8.line <- line
	}
}

func (v8 *V8) execute(command string) {
	response, err := v8.client.Query(client.Query{Command: command, Database: v8.database})
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if err := response.Error(); err != nil {
		log.Printf("error: %s\n", response.Error())
	}
}

func (v8 *V8) queryExecutor() {
	v8.wg.Add(1)
	defer v8.wg.Done()
	for {
		select {
		case c := <-v8.command:
			v8.totalCommands++
			v8.execute(c)
		case <-v8.done:
			return
		}
	}
}

func (v8 *V8) batchAccumulator() {
	v8.wg.Add(1)
	defer v8.wg.Done()
	for {
		select {
		case l := <-v8.line:
			v8.batch = append(v8.batch, l)
			if len(v8.batch) == batchSize {
				v8.batchWrite()
				v8.totalInserts += len(v8.batch)
				v8.batch = v8.batch[:0]
			}
		case <-v8.done:
			v8.totalInserts += len(v8.batch)
			return
		}
	}
}

func (v8 *V8) batchWrite() {
	_, e := v8.client.WriteLineProtocol(strings.Join(v8.batch, "\n"), v8.database, v8.retentionPolicy, v8.config.precision, v8.config.writeConsistency)
	if e != nil {
		log.Println("error writing batch: ", e)
	}
}
