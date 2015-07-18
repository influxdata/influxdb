package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/influxdb/influxdb/tsdb"
)

// remoteShardResponder implements the remoteShardConn interface.
type remoteShardResponder struct {
	t       *testing.T
	rxBytes []byte

	buffer *bytes.Buffer
}

func newRemoteShardResponder(resp *MapShardResponse) *remoteShardResponder {
	r := &remoteShardResponder{}

	a := make([]byte, 0, 256)
	r.buffer = bytes.NewBuffer(a)
	d, _ := resp.MarshalBinary()
	WriteTLV(r.buffer, mapShardResponseMessage, d)

	return r
}

func (r remoteShardResponder) MarkUnusable() { return }
func (r remoteShardResponder) Close() error  { return nil }
func (r remoteShardResponder) Read(p []byte) (n int, err error) {
	return io.ReadFull(r.buffer, p)
}

func (r remoteShardResponder) Write(p []byte) (n int, err error) {
	if r.rxBytes == nil {
		r.rxBytes = make([]byte, 0)
	}
	r.rxBytes = append(r.rxBytes, p...)
	return len(p), nil
}

// Ensure a RemoteMapper can process valid responses from a remote shard.
func TestShardWriter_RemoteMapper_Success(t *testing.T) {
	expTagSets := []string{"tagsetA"}
	expOutput := &tsdb.MapperOutput{
		Name: "cpu",
	}
	d, err := json.Marshal(expOutput)
	if err != nil {
		t.Fatalf("failed to marshal output: %s", err.Error())
	}

	resp := &MapShardResponse{}
	resp.SetCode(0)
	resp.SetData(d)
	resp.SetTagSets(expTagSets)

	c := newRemoteShardResponder(resp)

	r := NewRemoteMapper(c, 1234, "SELECT * FROM CPU", 10)
	if err := r.Open(); err != nil {
		t.Fatalf("failed to open remote mapper: %s", err.Error())
	}

	if r.TagSets()[0] != expTagSets[0] {
		t.Fatalf("incorrect tagsets received, exp %v, got %v", expTagSets, r.TagSets())
	}

	chunk, err := r.NextChunk()
	if err != nil {
		t.Fatalf("failed to get next chunk from mapper: %s", err.Error())
	}
	output, ok := chunk.(*tsdb.MapperOutput)
	if !ok {
		t.Fatal("chunk is not of expected type")
	}
	if output.Name != "cpu" {
		t.Fatalf("received output incorrect, exp: %v, got %v", expOutput, output)
	}
}
