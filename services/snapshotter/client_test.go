package snapshotter_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/services/snapshotter"
)

func TestClient_MetastoreBackup_InvalidMetadata(t *testing.T) {
	metaBlob, err := data.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	nodeBytes, err := json.Marshal(&influxdb.Node{ID: 1})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	var numBytes [24]byte

	// Write an invalid magic header.
	binary.BigEndian.PutUint64(numBytes[:8], snapshotter.BackupMagicHeader+1)
	binary.BigEndian.PutUint64(numBytes[8:16], uint64(len(metaBlob)))
	binary.BigEndian.PutUint64(numBytes[16:24], uint64(len(nodeBytes)))

	var buf bytes.Buffer
	buf.Write(numBytes[:16])
	buf.Write(metaBlob)
	buf.Write(numBytes[16:24])
	buf.Write(nodeBytes)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer l.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			t.Errorf("error accepting tcp connection: %s", err)
			return
		}
		defer conn.Close()

		var header [1]byte
		if _, err := conn.Read(header[:]); err != nil {
			t.Errorf("unable to read mux header: %s", err)
			return
		}

		var typ [1]byte
		if _, err := conn.Read(typ[:]); err != nil {
			t.Errorf("unable to read typ header: %s", err)
			return
		}

		var m map[string]interface{}
		dec := json.NewDecoder(conn)
		if err := dec.Decode(&m); err != nil {
			t.Errorf("invalid json request: %s", err)
			return
		}
		conn.Write(buf.Bytes())
	}()

	c := snapshotter.NewClient(l.Addr().String())
	_, err = c.MetastoreBackup()
	if err == nil || err.Error() != "invalid metadata received" {
		t.Errorf("unexpected error: got=%q want=%q", err, "invalid metadata received")
	}

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		t.Errorf("timeout while waiting for the goroutine")
	}
}
