InfluxDB Go Client Library
============

# package client

```go
    import "github.com/influxdb/influxdb/client"
 ```


#FUNCTIONS

```go
func EpochToTime(epoch int64, precision string) (time.Time, error)
```

    EpochToTime takes a unix epoch time and uses precision to return back a
    time.Time

```go
func SetPrecision(t time.Time, precision string) time.Time
```
    SetPrecision will round a time to the specified precision

#TYPES

```go
type BatchPoints struct {
    Points          []Point           `json:"points,omitempty"`
    Database        string            `json:"database,omitempty"`
    RetentionPolicy string            `json:"retentionPolicy,omitempty"`
    Tags            map[string]string `json:"tags,omitempty"`
    Timestamp       time.Time         `json:"timestamp,omitempty"`
    Precision       string            `json:"precision,omitempty"`
}
```
BatchPoints is used to send batched data in a single write. Database and
Points are required If no retention policy is specified, it will use the
databases default retention policy. If tags are specified, they will be
"merged" with all points. If a point already has that tag, it is
ignored. If timestamp is specified, it will be applied to any point with
an empty timestamp. Precision can be specified if the timestamp is in
epoch format (integer). Valid values for Precision are n, u, ms, s, m,
and h

```go
func (bp *BatchPoints) UnmarshalJSON(b []byte) error
```
UnmarshalJSON decodes the data into the BatchPoints struct

```go
type Client struct {
    // contains filtered or unexported fields
}
```
Client is used to make calls to the server.

```go
func NewClient(c Config) (*Client, error)
```
NewClient will instantiate and return a connected client to issue
commands to the server.

```go
func (c *Client) Addr() string
```
Addr provides the current url as a string of the server the client is
connected to.

```go
func (c *Client) Ping() (time.Duration, string, error)
```
Ping will check to see if the server is up Ping returns how long the
requeset took, the version of the server it connected to, and an error
if one occured.

```go
func (c *Client) Query(q Query) (*Results, error)
```
Query sends a command to the server and returns the Results

```go
func (c *Client) Write(bp BatchPoints) (*Results, error)
```
Write takes BatchPoints and allows for writing of multiple points with
defaults If successful, error is nil and Results is nil If an error
occurs, Results may contain additional information if populated.

```go
type Config struct {
    URL       url.URL
    Username  string
    Password  string
    UserAgent string
}
```
Config is used to specify what server to connect to. URL: The URL of the
server connecting to. Username/Password are optional. They will be
passed via basic auth if provided. UserAgent: If not provided, will
default "InfluxDBClient/" + version,

```go
type Point struct {
    Name      string
    Tags      map[string]string
    Timestamp time.Time
    Fields    map[string]interface{}
    Precision string
}
```
Point defines the fields that will be written to the database Name,
Timestamp, and Fields are required Precision can be specified if the
timestamp is in epoch format (integer). Valid values for Precision are
n, u, ms, s, m, and h

```go
func (p *Point) MarshalJSON() ([]byte, error)
```
MarshalJSON will format the time in RFC3339Nano Precision is also
ignored as it is only used for writing, not reading Or another way to
say it is we always send back in nanosecond precision

```go
func (p *Point) UnmarshalJSON(b []byte) error
```
UnmarshalJSON decodes the data into the Point struct

```go
type Query struct {
    Command  string
    Database string
}
```
Query is used to send a command to the server. Both Command and Database
are required.

```go
type Result struct {
    Series []influxql.Row
    Err    error
}
```
Result represents a resultset returned from a single statement.

```go
func (r *Result) MarshalJSON() ([]byte, error)
```
MarshalJSON encodes the result into JSON.

```go
func (r *Result) UnmarshalJSON(b []byte) error
```
UnmarshalJSON decodes the data into the Result struct

```go
type Results struct {
    Results []Result
    Err     error
}
```
Results represents a list of statement results.

```go
func (a Results) Error() error
```
Error returns the first error from any statement. Returns nil if no
errors occurred on any statements.

```go
func (r *Results) MarshalJSON() ([]byte, error)
```
MarshalJSON encodes the result into JSON.

```go
func (r *Results) UnmarshalJSON(b []byte) error
```
UnmarshalJSON decodes the data into the Results struct

# Examples

You can see a use of the client libray in the [InfluxDB CLI](https://github.com/influxdb/influxdb/blob/master/cmd/influx/main.go).
