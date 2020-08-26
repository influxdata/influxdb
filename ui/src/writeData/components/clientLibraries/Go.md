For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-go)

##### Initialize the Client

```
package main

import (
  "context"
  "fmt"
  "github.com/influxdata/influxdb-client-go/v2"
  "time"
)

func main() {
  // You can generate a Token from the "Tokens Tab" in the UI
  const token = "<%= token %>"
  const bucket = "<%= bucket %>"
  const org = "<%= org %>"

  client := influxdb2.NewClient("<%= server %>", token)
  // always close client at the end
  defer client.Close()
}
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
// get non-blocking write client
writeAPI := client.WriteAPI(org, bucket)

// write line protocol
writeAPI.WriteRecord(fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0))
writeAPI.WriteRecord(fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 22.5, 45.0))
// Flush writes
writeAPI.Flush()
```

Option 2: Use a Data Point to write data

```
p := influxdb2.NewPoint("stat",
  map[string]string{"unit": "temperature"},
  map[string]interface{}{"avg": 24.5, "max": 45},
  time.Now())
// write point asynchronously
writeAPI.WritePoint(p)
// create point using fluent style
p = influxdb2.NewPointWithMeasurement("stat").
  AddTag("unit", "temperature").
  AddField("avg", 23.2).
  AddField("max", 45).
  SetTime(time.Now())
// write point asynchronously
writeAPI.WritePoint(p)
// Flush writes
writeAPI.Flush()
```

##### Execute a Flux query

```
query := fmt.Sprintf("from(bucket:\"%v\")|> range(start: -1h) |> filter(fn: (r) => r._measurement == \"stat\")", bucket)
// Get query client
queryAPI := client.QueryAPI(org)
// get QueryTableResult
result, err := queryAPI.Query(context.Background(), query)
if err == nil {
  // Iterate over query response
  for result.Next() {
    // Notice when group key has changed
    if result.TableChanged() {
      fmt.Printf("table: %s\n", result.TableMetadata().String())
    }
    // Access data
    fmt.Printf("value: %v\n", result.Record().Value())
  }
  // check for an error
  if result.Err() != nil {
    fmt.Printf("query parsing error: %\n", result.Err().Error())
  }
} else {
  panic(err)
}
```
