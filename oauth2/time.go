package oauth2

import "time"

// DefaultNowTime returns UTC time at the present moment
const DefaultNowTime = func() time.Time { return time.Now().UTC() }
