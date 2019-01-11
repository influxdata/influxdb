package influxdb_test

import (
	"context"
	"fmt"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
)

func ExampleKeyValueLog() {
	c := bolt.NewClient()
	c.Path = "example.bolt"
	ctx := context.Background()
	if err := c.Open(ctx); err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		if err := c.AddLogEntry(ctx, []byte("bucket_0_auditlog"), []byte(fmt.Sprintf("abc-%v", i)), time.Now()); err != nil {
			panic(err)
		}
	}

	opts := platform.FindOptions{Limit: 2, Offset: 1, Descending: false}
	if err := c.ForEachLogEntry(ctx, []byte("bucket_0_auditlog"), opts, func(v []byte, t time.Time) error {
		fmt.Println(t.UTC())
		fmt.Println(string(v))
		fmt.Println()
		return nil
	}); err != nil {
		panic(err)
	}

	v, t, err := c.LastLogEntry(ctx, []byte("bucket_0_auditlog"))
	if err != nil {
		panic(err)
	}
	fmt.Println(t.UTC())
	fmt.Println(string(v))
	fmt.Println()

	v, t, err = c.FirstLogEntry(ctx, []byte("bucket_0_auditlog"))
	if err != nil {
		panic(err)
	}
	fmt.Println(t.UTC())
	fmt.Println(string(v))
	fmt.Println()
}
