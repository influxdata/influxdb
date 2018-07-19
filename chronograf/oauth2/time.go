package oauth2

import "time"

// DefaultNowTime returns UTC time at the present moment
var DefaultNowTime = func() time.Time { return time.Now().UTC() }
