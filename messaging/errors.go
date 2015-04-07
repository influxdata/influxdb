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

	// ErrClientOpen is returned when opening an already open client.
	ErrClientOpen = errors.New("client already open")

	// ErrClientClosed is returned when closing an already closed client.
	ErrClientClosed = errors.New("client closed")

	// ErrConnOpen is returned when opening an already open connection.
	ErrConnOpen = errors.New("connection already open")

	// ErrConnClosed is returned when closing an already closed connection.
	ErrConnClosed = errors.New("connection closed")

	// ErrConnCannotReuse is returned when opening a previously closed connection.
	ErrConnCannotReuse = errors.New("cannot reuse connection")

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

	// ErrReaderClosed is returned when reading from a closed topic reader.
	ErrReaderClosed = errors.New("reader closed")

	// ErrURLRequired is returned when making a call without a url parameter
	ErrURLRequired = errors.New("url required")

	// ErrMessageDataRequired is returned when publishing a message without data.
	ErrMessageDataRequired = errors.New("message data required")

	// ErrChecksum is returned when decoding a message with corrupt data.
	ErrChecksum = errors.New("checksum error")
)
