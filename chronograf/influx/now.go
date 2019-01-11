package influx

import "time"

// Now returns the current time
type Now func() time.Time
