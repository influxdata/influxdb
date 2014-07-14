package storage

// A write/delete used by BatchPut. To delete data, set the Value to nil
type Write struct {
	Key   []byte
	Value []byte
}

// Iterator is used to scan a range of keys
type Iterator interface {
	// Seek to the given key, or a larger key if this one doesn't exist
	Seek(key []byte)
	// Get the key at the current position
	Key() []byte
	// Get the value at the current position
	Value() []byte
	// Move the next element
	Next()
	// Move the previous element
	Prev()
	// True if no error occured during scanning and if the predicate
	// used when calling GetRangePredicate is true . If GetRange is
	// used, returns GetKey() <= last where <= is the lexicographic
	// order.
	Valid() bool
	// Returns any errors occured during scannig
	Error() error
	// Close the iterator and free the resources used
	Close() error
}

// Interface to all storage engine backends
type Engine interface {
	Name() string
	Path() string
	// Add the given key/value pair, duplicates aren't allowed
	Put(key, value []byte) error
	// Get the value associated with the given key
	Get(key []byte) ([]byte, error)
	// More efficient put
	BatchPut(writes []Write) error
	// Delete the given range of keys [first, last]
	Del(first, last []byte) error
	// Get an iterator for the db
	Iterator() Iterator
	// Compact the db and reclaim unused space
	Compact()
	// Close the database
	Close()
}
