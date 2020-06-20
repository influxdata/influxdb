package influxdb

type Event struct {
	Type EventType
	Auth Authorization
}

type EventType uint

const (
	EventUnknown EventType = iota
	EventSetupComplete
	EventStartupComplete
)

func (s EventType) String() string {
	return [...]string{
		EventUnknown:         "unknown",
		EventSetupComplete:   "setup_complete",
		EventStartupComplete: "startup_complete",
	}[s]
}
