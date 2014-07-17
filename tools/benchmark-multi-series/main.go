package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	numberOfSeries := 50000
	if len(os.Args) > 1 {
		numberOfSeries, _ = strconv.Atoi(os.Args[1])
	}
	fmt.Printf("Benchmarking writing %d series\n", numberOfSeries)

	c, err := client.NewClient(&client.ClientConfig{})
	if err != nil {
		panic(err)
	}

	before := time.Now()
	c.DeleteDatabase("performance")
	fmt.Printf("Deleting took %s\n", time.Now().Sub(before))
	os.Exit(0)
	if err := c.CreateDatabase("performance"); err != nil {
		panic(err)
	}

	c, err = client.NewClient(&client.ClientConfig{
		Database: "performance",
	})
	if err != nil {
		panic(err)
	}

	before = time.Now()

	for i := 0; i < 10; i++ {
		series := []*client.Series{}
		for i := 0; i < numberOfSeries; i++ {
			name := fmt.Sprintf("series_%d", i+1)
			series = append(series, &client.Series{
				Name:    name,
				Columns: []string{"value"},
				Points: [][]interface{}{
					{rand.Float64()},
				},
			})
		}
		if err := c.WriteSeries(series); err != nil {
			panic(err)
		}
	}

	fmt.Printf("Writing took %s\n", time.Now().Sub(before))
}
