package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestEncodingAndDecodingSpeed(t *testing.T) {
	entries := make([]*LogEntry, 2000)

	e := newLogEntry(nil, 1, 2, &joinCommand{Name: "localhost:1000"})

	for i := 0; i < 2000; i++ {
		entries[i] = e
	}

	req := newAppendEntriesRequest(1, "leader", 1, 1, entries, 1)

	var b bytes.Buffer

	startTime := time.Now()

	json.NewEncoder(&b).Encode(req)

	fmt.Println("Encoding ", b.Len()/1000, " kb took ", time.Now().Sub(startTime))

	startTime = time.Now()

	resp := &AppendEntriesResponse{}

	length := b.Len()

	json.NewDecoder(&b).Decode(&resp)

	fmt.Println("Decoding ", length/1000, " kb took ", time.Now().Sub(startTime))

}
