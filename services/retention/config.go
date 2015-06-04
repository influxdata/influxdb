package retention

import "time"

type Config struct {
	Enabled       bool
	CheckInterval time.Duration
}
