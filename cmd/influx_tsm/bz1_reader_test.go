package main

import (
	"fmt"
	"testing"
)

func Test_BZ1Reader(t *testing.T) {
	r := NewBZ1Reader("/home/philip/.influxdb/data/_internal/monitor/1")
	if err := r.Open(); err != nil {
		t.Fatalf("failed to open BZ1 shard: %s", err.Error())
	}

	for r.Next() {
		key, values, _ := r.Read()
		fmt.Println(key, values)
	}
}
