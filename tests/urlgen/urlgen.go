package main

import (
	"flag"
	"fmt"
	"time"
)

var (
	i, s, c int
)

func init() {
	flag.IntVar(&i, "i", 10, "interval")
	flag.IntVar(&s, "s", 1, "Number of unique series to generate.")
	flag.IntVar(&c, "c", 10, "Number of clients to simulate.")
}

func main() {
	flag.Parse()

	var (
		pointsInSeries string
		j              int = 1
		k              int
		printComma     string = ","
		jsonTemplate   string = `{"name": "cpu", "tags": {"host": "server%d"}, "timestamp": "%s","fields": {"value": 100}}%s`
		urlTemplate    string = `http://localhost:8086/write POST {"database" : "db", "retentionPolicy" : "raw", "points": [%s]}`
	)

	t := time.Date(2010, time.January, 1, 8, 0, 0, 0, time.UTC)
	for ; c > 0; c-- {
		for ; j < s+1; j++ {
			for ; k < i; k++ {
				if k == i-1 {
					printComma = ""
				}
				pointsInSeries = pointsInSeries + fmt.Sprintf(jsonTemplate, j, t.Format(time.RFC3339), printComma)
				t = t.Add(1 * time.Second)
			}
			fmt.Printf(urlTemplate, pointsInSeries)
			fmt.Println()
			pointsInSeries = ""
			k = 0
			printComma = ","
		}
		j = 1
	}
}
