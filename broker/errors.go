package broker

import "errors"

var (
	// ErrSubscribed is returned when a stream is already subscribed to a topic.
	ErrSubscribed = errors.New("already subscribed")
)
