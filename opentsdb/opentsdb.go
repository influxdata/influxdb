package opentsdb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
)

const (
	// DefaultPort represents the default OpenTSDB port.
	DefaultPort = 4242

	// DefaultDatabaseName is the default OpenTSDB database if none is specified.
	DefaultDatabaseName = "opentsdb"
)

// SeriesWriter defines the interface for the destination of the data.
type SeriesWriter interface {
	WriteSeries(database, retentionPolicy string, points []influxdb.Point) (uint64, error)
}

// Server is an InfluxDB input class to implement  OpenTSDB's input protocols.
type Server struct {
	writer SeriesWriter

	database        string
	retentionpolicy string

	listener *net.TCPListener
	tsdbhttp *tsdbHTTPListener
	wg       sync.WaitGroup

	addr net.Addr
	mu   sync.Mutex
}

func NewServer(w SeriesWriter, retpol string, db string) *Server {
	s := &Server{}

	s.writer = w
	s.retentionpolicy = retpol
	s.database = db
	s.tsdbhttp = makeTSDBHTTPListener()

	return s
}

func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// ListenAndServe start the OpenTSDB compatible server on the given
// ip and port.
func (s *Server) ListenAndServe(listenAddress string) {
	var err error

	addr, err := net.ResolveTCPAddr("tcp", listenAddress)
	if err != nil {
		log.Println("TSDBServer: ResolveTCPAddr: ", err)
		return
	}

	s.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		log.Println("TSDBServer: Listen: ", err)
		return
	}

	s.mu.Lock()
	s.addr = s.listener.Addr()
	s.mu.Unlock()

	s.wg.Add(1)

	// Set up the background HTTP server that we
	// will pass http request to via a channel
	mux := http.NewServeMux()
	mux.HandleFunc("/api/metadata/put", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/api/put", s)
	httpsrv := &http.Server{}
	httpsrv.Handler = mux
	go httpsrv.Serve(s.tsdbhttp)

	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				log.Println("openTSDB TCP listener closed")
				return
			}
			if err != nil {
				log.Println("error accepting openTSDB: ", err.Error())
				continue
			}
			s.wg.Add(1)
			go s.HandleConnection(conn)
		}
	}()
}

func (s *Server) Close() error {
	var err error
	if s.listener != nil {
		err = (*s.listener).Close()
	}
	s.wg.Wait()
	s.listener = nil
	return err
}

// HandleConnection takes each new connection and attempts to
// determine if it should be handled by the HTTP handler, if
// parsing as a HTTP request fails, we'll pass it to the
// telnet handler
func (s *Server) HandleConnection(conn net.Conn) {
	var peekbuf bytes.Buffer
	t := io.TeeReader(conn, &peekbuf)
	r := bufio.NewReader(t)

	_, httperr := http.ReadRequest(r)

	splice := io.MultiReader(&peekbuf, conn)
	bufsplice := bufio.NewReader(splice)
	newc := &tsdbConn{
		Conn: conn,
		rdr:  bufsplice,
	}

	if httperr == nil {
		s.tsdbhttp.acc <- tsdbHTTPReq{
			conn: newc,
		}
	} else {
		s.HandleTelnet(newc)
	}
}

// HandleTelnet accepts OpenTSDB's telnet protocol
// Each telnet command consists of a line of the form:
//   put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0
func (s *Server) HandleTelnet(conn net.Conn) {
	reader := bufio.NewReader(conn)
	tp := textproto.NewReader(reader)

	defer conn.Close()
	defer s.wg.Done()

	for {
		line, err := tp.ReadLine()
		if err != nil {
			log.Println("error reading from openTSDB connection", err.Error())
			return
		}

		inputStrs := strings.Fields(line)

		if len(inputStrs) == 1 && inputStrs[0] == "version" {
			conn.Write([]byte("InfluxDB TSDB proxy"))
			continue
		}

		if len(inputStrs) < 4 || inputStrs[0] != "put" {
			log.Println("TSDBServer: malformed line, skipping: ", line)
			continue
		}

		name := inputStrs[1]
		tsStr := inputStrs[2]
		valueStr := inputStrs[3]
		tagStrs := inputStrs[4:]

		var t time.Time
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			log.Println("TSDBServer: malformed time, skipping: ", tsStr)
		}

		switch len(tsStr) {
		case 10:
			t = time.Unix(ts, 0)
			break
		case 13:
			t = time.Unix(ts/1000, (ts%1000)*1000)
			break
		default:
			log.Println("TSDBServer: time must be 10 or 13 chars, skipping: ", tsStr)
			continue
		}

		tags := make(map[string]string)
		for t := range tagStrs {
			parts := strings.SplitN(tagStrs[t], "=", 2)
			if len(parts) != 2 {
				log.Println("TSDBServer: malformed tag data", tagStrs[t])
				continue
			}
			k := parts[0]

			tags[k] = parts[1]
		}

		fields := make(map[string]interface{})
		fields["value"], err = strconv.ParseFloat(valueStr, 64)
		if err != nil {
			log.Println("TSDBServer: could not parse value as float: ", valueStr)
			continue
		}

		p := influxdb.Point{
			Name:   name,
			Tags:   tags,
			Time:   t,
			Fields: fields,
		}

		_, err = s.writer.WriteSeries(s.database, s.retentionpolicy, []influxdb.Point{p})
		if err != nil {
			log.Println("TSDB cannot write data: ", err)
			continue
		}
	}
}

/*
 tsdbDP is a struct to unmarshal OpenTSDB /api/put data into
 Request is either tsdbDP, or a []tsdbDP
 {
   "metric": "sys.cpu.nice",
   "timestamp": 1346846400,
   "value": 18,
   "tags": {
     "host": "web01",
     "dc": "lga"
   }
 }
*/
type tsdbDP struct {
	Metric string            `json:"metric"`
	Time   int64             `json:"timestamp"`
	Value  float64           `json:"value"`
	Tags   map[string]string `json:"tags,omitempty"`
}

// ServeHTTP implements OpenTSDB's HTTP /api/put endpoint
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	defer s.wg.Done()

	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	dps := make([]tsdbDP, 1)
	var br *bufio.Reader

	// We need to peek and see if this is an array or a single
	// DP
	multi := false

	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "Could not read gzip, "+err.Error(), http.StatusBadRequest)
			return
		}

		br = bufio.NewReader(zr)
	} else {
		br = bufio.NewReader(r.Body)
	}

	f, err := br.Peek(1)

	if err != nil || len(f) != 1 {
		http.Error(w, "Could not peek at JSON data, "+err.Error(), http.StatusBadRequest)
		return
	}

	switch f[0] {
	case '{':
	case '[':
		multi = true
	default:
		http.Error(w, "Expected JSON array or hash", http.StatusBadRequest)
		return
	}

	dec := json.NewDecoder(br)

	if multi {
		err = dec.Decode(&dps)
	} else {
		err = dec.Decode(&dps[0])
	}

	if err != nil {
		http.Error(w, "Could not decode JSON as TSDB data", http.StatusBadRequest)
		return
	}

	var idps []influxdb.Point
	for dpi := range dps {
		dp := dps[dpi]

		var ts time.Time
		if dp.Time < 10000000000 {
			ts = time.Unix(dp.Time, 0)
		} else {
			ts = time.Unix(dp.Time/1000, (dp.Time%1000)*1000)
		}

		fields := make(map[string]interface{})
		fields["value"] = dp.Value
		if err != nil {
			continue
		}
		p := influxdb.Point{
			Name:   dp.Metric,
			Tags:   dp.Tags,
			Time:   ts,
			Fields: fields,
		}
		idps = append(idps, p)
	}
	_, err = s.writer.WriteSeries(s.database, s.retentionpolicy, idps)
	if err != nil {
		log.Println("TSDB cannot write data: ", err)
	}

	w.WriteHeader(http.StatusNoContent)
}

// tsdbHTTPListener is a listener that takes connects from a channel
// rather than directly from a network socket
type tsdbHTTPListener struct {
	addr net.Addr
	cls  chan struct{}
	acc  chan tsdbHTTPReq
}

// tsdbHTTPReq represents a incoming connection that we have established
// to be a valid http request.
type tsdbHTTPReq struct {
	conn net.Conn
	err  error
}

func (l *tsdbHTTPListener) Accept() (c net.Conn, err error) {
	select {
	case newc := <-l.acc:
		log.Println("TSDB listener accept ", newc)
		return newc.conn, newc.err
	case <-l.cls:
		close(l.cls)
		close(l.acc)
		return nil, nil
	}
}

func (l *tsdbHTTPListener) Close() error {
	l.cls <- struct{}{}
	return nil
}

func (l *tsdbHTTPListener) Addr() net.Addr {
	return l.addr
}

func makeTSDBHTTPListener() *tsdbHTTPListener {
	return &tsdbHTTPListener{
		acc: make(chan tsdbHTTPReq),
		cls: make(chan struct{}),
	}
}

// tsdbConn is a net.Conn implmentation used to splice peeked buffer content
// to the pre-existing net.Conn that was peeked into
type tsdbConn struct {
	rdr *bufio.Reader
	net.Conn
}

// Read implmeents the io.Reader interface
func (c *tsdbConn) Read(b []byte) (n int, err error) {
	return c.rdr.Read(b)
}
