package bolt

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
)

// changeIntervalToDuration
// Before, we supported queries that included `GROUP BY :interval:`
// After, we only support queries with `GROUP BY time(:interval:)`
// thereby allowing non_negative_derivative(_____, :interval)
var changeIntervalToDuration = Migration{
	ID:   "59b0cda4fc7909ff84ee5c4f9cb4b655b6a26620",
	Up:   up,
	Down: down,
}

var up = func(db bolt.DB) error {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(DashboardsBucket)

		fmt.Println("We're in the transaction")

		err := bucket.ForEach(func(id, data []byte) error {
			board := &Dashboard{}

			fmt.Println("New dashboard!")

			err := proto.Unmarshal(data, board)
			if err != nil {
				log.Fatal("unmarshaling error: ", err)
			}

			fmt.Println("Parsed the dashboard!")

			for i, cell := range board.cells {
				for i, query := range cell.queries {
					query.Command = strings.Replace(query.Command, ":interval:", "time(:interval:)", -1)
					cell.queries[i] = query
				}
				board.cells[i] = cell
			}

			fmt.Println("Updated the dashboard!")

			data, err = proto.Marshal(board)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}

			fmt.Println("marshaled the dashboard!")

			err = bucket.Put(id, data)
			if err != nil {
				log.Fatal("error updating dashboard: ", err)
			}

			fmt.Println("Updated the dashboard!")

			return nil
		})

		if err != nil {
			log.Fatal("error updating dashboards: ", err)
		}
	})

	if err != nil {
		return err
	}

	return nil
}

var down = func(ctx context.Context, client bolt.Client) error {
	return nil
}

var DashboardsBucket = []byte("Dashoard")

type Dashboard struct {
	Cells []*DashboardCell `protobuf:"bytes,3,rep,name=cells" json:"cells,omitempty"`
}
type DashboardCell struct {
	Queries []*Query `protobuf:"bytes,5,rep,name=queries" json:"queries,omitempty"`
}
type Query struct {
	Command string `protobuf:"bytes,1,opt,name=Command,proto3" json:"Command,omitempty"`
}
