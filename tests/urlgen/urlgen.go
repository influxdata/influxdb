package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"
)

func main() {
	intervalN := flag.Int("interval", 10, "interval")
	seriesN := flag.Int("series", 1, "Number of unique series to generate.")
	clientN := flag.Int("clients", 10, "Number of clients to simulate.")
	startDate := flag.String("start", time.Now().Format(time.RFC3339), "Date to start with.")
	flag.Parse()

	t, _ := time.Parse(time.RFC3339, *startDate)
	oneSecond := 1 * time.Second

	for i := 0; i < *clientN; i++ {
		for j := 0; j < *seriesN; j++ {
			points := make([]*Point, 0)
			for k := 0; k < *intervalN; k++ {
				t = t.Add(oneSecond)
				points = append(points, &Point{
					Name:      "cpu",
					Timestamp: t,
					Tags:      map[string]string{"host": fmt.Sprintf("server%d", j+1)},
					Fields:    map[string]interface{}{"value": 100},
				})
			}
			batch := &Batch{
				Database:        "db",
				RetentionPolicy: "raw",
				Points:          points,
			}
			buf, _ := json.Marshal(batch)

			fmt.Printf("http://localhost:8086/write POST %s\n", buf)
		}
	}
}

type Batch struct {
	Database        string   `json:"database"`
	RetentionPolicy string   `json:"retentionPolicy"`
	Points          []*Point `json:"points"`
}

type Point struct {
	Name      string                 `json:"name"`
	Timestamp time.Time              `json:"timestamp"`
	Tags      map[string]string      `json:"tags"`
	Fields    map[string]interface{} `json:"fields"`
}
