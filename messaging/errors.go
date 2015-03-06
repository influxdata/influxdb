package messaging

import "errors"

var (
	// ErrPathRequired is returned when opening a broker without a path.
	ErrPathRequired = errors.New("path required")

	// ErrPathRequired is returned when opening a broker without a connection address.
	ErrConnectionAddressRequired = errors.New("connection address required")

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

	// ErrReplicaIDRequired is returned when creating a replica without an id.
	ErrReplicaIDRequired = errors.New("replica id required")

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

	// ErrNoLeader is returned when a leader cannot be reached.
	ErrNoLeader = errors.New("no leader")

	// ErrIndexRequired is returned when making a call without a valid index.
	ErrIndexRequired = errors.New("index required")

	// ErrTopicOpen is returned when opening an already open topic.
	ErrTopicOpen = errors.New("topic already open")

	// ErrSegmentReclaimed is returned when requesting a segment that has been deleted.
	ErrSegmentReclaimed = errors.New("segment reclaimed")

	// ErrStaleWrite is returned when writing a message with an old index to a topic.
	ErrStaleWrite = errors.New("stale write")
)
