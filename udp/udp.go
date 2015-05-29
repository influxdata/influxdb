package udp

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
)

const (
	udpBufferSize = 65536
)

// SeriesWriter defines the interface for the destination of the data.
type SeriesWriter interface {
	WriteSeries(database, retentionPolicy string, points []influxdb.Point) (uint64, error)
}

// UDPServer
type UDPServer struct {
	writer SeriesWriter
	conn   *net.UDPConn
	addr   *net.UDPAddr
	wg     sync.WaitGroup

	batchSize    int
	batchTimeout time.Duration

	Logger *log.Logger
}

// NewUDPServer returns a new instance of a UDPServer
func NewUDPServer(w SeriesWriter) *UDPServer {
	u := UDPServer{
		writer: w,
		Logger: log.New(os.Stderr, "[udp] ", log.LstdFlags),
	}
	return &u
}

// ListenAndServe instructs the UDPServer to start processing data
// on the given interface. iface must be in the form host:port.
func (u *UDPServer) ListenAndServe(iface string) (err error) {
	u.addr, err = net.ResolveUDPAddr("udp", iface)
	if err != nil {
		u.Logger.Printf("Failed to resolve UDP address %s: %s", iface, err)
		return err
	}

	u.conn, err = net.ListenUDP("udp", u.addr)
	if err != nil {
		u.Logger.Printf("Failed to set up UDP listener at address %s: %s", u.addr, err)
		return err
	}

	var bp client.BatchPoints
	buf := make([]byte, udpBufferSize)

	batcher := influxdb.NewPointBatcher(u.batchSize, u.batchTimeout)
	batcher.Start()

	// Start processing batches.
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case batch := <-batcher.Out():
				_, e := u.writer.WriteSeries(bp.Database, bp.RetentionPolicy, batch)
				if e != nil {
					u.Logger.Printf("Failed to write point batch to database %q: %s\n", bp.Database, e)
				}
			case <-done:
				return
			}
		}
	}()

	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		for {
			_, _, err := u.conn.ReadFromUDP(buf)
			if err != nil {
				u.Logger.Printf("Failed to read UDP message: %s.", err)
				batcher.Flush()
				close(done)
				wg.Wait()
				return
			}

			dec := json.NewDecoder(bytes.NewReader(buf))
			if err := dec.Decode(&bp); err != nil {
				u.Logger.Printf("Failed to decode JSON UDP message")
				continue
			}

			points, err := influxdb.NormalizeBatchPoints(bp)
			if err != nil {
				u.Logger.Printf("Failed to normalize batch points")
				continue
			}

			for _, point := range points {
				batcher.In() <- point
			}
		}
	}()
	return nil
}

func (u *UDPServer) SetBatchSize(sz int)             { u.batchSize = sz }
func (u *UDPServer) SetBatchTimeout(d time.Duration) { u.batchTimeout = d }
