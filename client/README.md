# InfluxDB Client

## Description

A Go client library written and maintain by the **InfluxDB** team.
This package provides convenience functions to read and write time series data.
It uses the HTTP protocol to communicate with your **InfluxDB** cluster.


## Getting Started

### Connecting To Your Database

Connecting to an **InfluxDB** database is straight forward. You will need a host
name, a port and the cluster credentials. The default port is 8086 and the default
credentials is root for both username and password. You can customize these settings
to your specific installation via the **InfluxDB** configuration file.

```go
package main

import "github.com/influxdb/influxdb/client"

const (
	MyHost        = "localhost"
	MyPort        = 8086
	MyDB          = "square_holes"
	MyMeasurement = "shapes"
	MyUser        = "root"
	MyPwd         = "root"
)

func main() {
	u, err := url.Parse(fmt.Sprintf("http://%s:%d", MyHost, MyPort))
	if err != nil {
		log.Fatal(err)
	}
	conf := client.Config{
		URL:      *u,
		Username: MyUser,
		Password: MyPwd,
	}

	con, err := client.NewClient(conf)
	if err != nil {
		log.Fatal(err)
	}

	dur, ver, err := con.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Happy as a Hippo! %v, %s", dur, ver)
}

```

### Inserting Data

Time series data aka *points* are written to the database using batch inserts.
The mechanism is to create one or more points and then create a batch aka *batch points*
and write these to a given database and series. A series is a combination of a
measurement (time/values) and a set of tags.

In this sample we will create a batch of a 1k points. Each point as a timestamp and
a single value as well as 2 tags indicating a shape and color. We write these points
to a database called _square_holes_ using a measurement named _shapes_.

NOTE: In this example, we are specifically assigning time, tags and precision
to each point. Alternately, you can specify a timestamp, tags and precision at
the batch point level that coulld be used as defaults if an associated point
does not provide these metrics.

NOTE: You can specify a RetententionPolicy as part of the batch. If not
provided it will use the database _default_ retention policy.

```go
func writePoints(con *client.Client) {
	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green"}
		sampleSize = 1000
		pts        = make([]client.Point, sampleSize)
	)

	rand.Seed(42)
	for i := 0; i < sampleSize; i++ {
		pts[i] = client.Point{
			Name: "shapes",
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(sampleSize),
			},
			Timestamp: time.Now(),
			Precision: "s",
		}
	}

	bps := client.BatchPoints{
		Points:          pts,
		Database:        MyDB,
		RetentionPolicy: "default", // ie for evar!
	}
	_, err := con.Write(bps)
	if err != nil {
		log.Fatal(err)
	}
}
```


### Querying Data

One nice advantage of using **InfluxDB** is having the ability to query your data
using familiar SQL constructs. In this example we are providing a convenience
function to query the database as follows:

```go
// queryDB convenience to query the database
func queryDB(con *client.Client, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: MyDB,
	}
	if results, err := con.Query(q); err == nil {
		if results.Error() != nil {
			return res, results.Error()
		}
		res = results.Results
	}
	return
}
```

#### Creating A Database
```go
_, err := queryDB(con, fmt.Sprintf("create database %s", MyDB))
if err != nil {
	log.Fatal(err)
}
```

#### Count Records
```go
q := fmt.Sprintf("select count(%s) from %s", "value", MyMeasurement)
res, err := queryDB(con, q)
if err != nil {
	log.Fatal(err)
}
count := res[0].Series[0].Values[0][1]
log.Printf("Found a total of `%v records", count)

```

#### Find the last 10 _shapes_ records

```go
q := fmt.Sprintf("select * from %s limit %d", MyMeasurement, 20)
res, err = queryDB(con, q)
if err != nil {
	log.Fatal(err)
}

for i, row := range res[0].Series[0].Values {
	t, err := time.Parse(time.RFC3339, row[0].(string))
	if err != nil {
		log.Fatal(err)
	}
	val, err := row[1].(json.Number).Int64()
	log.Printf("[%2d] %s: %03d\n", i, t.Format(time.Stamp), val)
}
```


## Go Docs

Please refer to
[http://godoc.org/github.com/influxdb/influxdb/client](http://godoc.org/github.com/influxdb/influxdb/client)
for documentation.


## See Also

You can see a use of the client libray in the
[InfluxDB CLI](https://github.com/influxdb/influxdb/blob/master/cmd/influx/main.go).
