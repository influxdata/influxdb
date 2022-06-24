import "experimental/influxdb"
import "internal/debug"

influxdb.preview(bucket: "preview-test")
|> range(start: -1d)
|> debug.pass()
|> group()
|> aggregateWindow(every: 1m, fn: mean)
