package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/davecheney/profile"
	"github.com/influxdb/influxdb/raft"
)

const InitializationDuration = 1 * time.Second

func main() {
	// Parse flags.
	var (
		prof    = flag.Bool("profile", false, "enable profiling")
		addr    = flag.String("addr", "", "bind address")
		joinURL = flag.String("join-url", "", "cluster to join")
		sz      = flag.Int("size", 32, "command size")
	)
	flag.Parse()
	log.SetFlags(0)

	// Enable profiling.
	if *prof {
		defer profile.Start(&profile.Config{CPUProfile: true, MemProfile: true, BlockProfile: true}).Stop()
	}

	// Validate input.
	if *addr == "" {
		log.Fatal("bind address required")
	}

	// Create temporary log location.
	path := tempfile()
	if err := os.MkdirAll(path, 0700); err != nil {
		log.Fatal(err)
	}

	// Retrieve hostname.
	hostname, _ := os.Hostname()

	// Create state machine.
	fsm := &FSM{}

	// Create and open log.
	l := &raft.Log{
		FSM: fsm,
		URL: &url.URL{Scheme: "http", Host: hostname + *addr},
	}

	if err := l.Open(path); err != nil {
		log.Fatal(err)
	}

	// Initialize or join.
	if *joinURL == "" {
		if err := l.Initialize(); err != nil {
			log.Fatalf("initialize: %s", err)
		}
		log.Println("[initialized]")

		go generator(l, *sz)
	} else {
		u, err := url.Parse(*joinURL)
		if err != nil {
			log.Fatalf("invalid join url: %s", err)
		}
		if err := l.Join(u); err != nil {
			log.Fatalf("join: %s", err)
		}
		log.Println("[joined]")
	}

	// Print notice.
	log.Printf("Listening on http://%s%s", hostname, *addr)
	log.SetFlags(log.LstdFlags)

	// Create and start handler.
	h := raft.NewHTTPHandler(l)
	go func() { log.Fatal(http.ListenAndServe(*addr, h)) }()

	time.Sleep(InitializationDuration)

	startTime := time.Now()
	time.Sleep(5 * time.Second)

	entryN := fsm.EntryN
	log.Printf("[BENCH] ops=%d; %0.03f ops/sec\n\n", entryN, float64(entryN)/time.Since(startTime).Seconds())
}

// generator is run in a separate goroutine to generate data.
func generator(l *raft.Log, sz int) {
	time.Sleep(InitializationDuration)

	for {
		command := make([]byte, sz)
		if err := l.Apply(command); err != nil {
			log.Fatalf("generate: %s", err)
		}
	}
}

// FSM represents the application's finite state machine.
type FSM struct {
	EntryN int
}

func (fsm *FSM) Apply(e *raft.LogEntry) error {
	fsm.EntryN++
	return nil
}

func (fsm *FSM) Snapshot(w io.Writer) error { return nil }
func (fsm *FSM) Restore(r io.Reader) error  { return nil }

// tempfile returns the path to a non-existent file in the temp directory.
func tempfile() string {
	f, _ := ioutil.TempFile("", "raftbench-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
