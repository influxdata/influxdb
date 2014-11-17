package messaging

import "errors"

var (
	// ErrPathRequired is returned when opening a broker without a path.
	ErrPathRequired = errors.New("path required")

	// ErrClosed is returned when closing a broker that's already closed.
	ErrClosed = errors.New("broker already closed")

	// ErrSubscribed is returned when a stream is already subscribed to a topic.
	ErrSubscribed = errors.New("already subscribed")

	// ErrTopicExists is returned when creating a duplicate topic.
	ErrTopicExists = errors.New("topic already exists")

	// ErrReplicaExists is returned when creating a duplicate replica.
	ErrReplicaExists = errors.New("replica already exists")

	// ErrReplicaNotFound is returned when referencing a replica that doesn't exist.
	ErrReplicaNotFound = errors.New("replica not found")

	// ErrReplicaNameRequired is returned when finding a replica without a name.
	ErrReplicaNameRequired = errors.New("replica name required")

	// errReplicaUnavailable is returned when writing bytes to a replica when
	// there is no writer attached to the replica.
	errReplicaUnavailable = errors.New("replica unavailable")

	// ErrClientOpen is returned when opening an already open client.
	ErrClientOpen = errors.New("client already open")

	// ErrClientClosed is returned when closing an already closed client.
	ErrClientClosed = errors.New("client closed")

	// ErrBrokerURLRequired is returned when opening a broker without URLs.
	ErrBrokerURLRequired = errors.New("broker url required")

	// ErrMessageTypeRequired is returned publishing a message without a type.
	ErrMessageTypeRequired = errors.New("message type required")

	// ErrTopicRequired is returned publishing a message without a topic ID.
	ErrTopicRequired = errors.New("topic required")
)
