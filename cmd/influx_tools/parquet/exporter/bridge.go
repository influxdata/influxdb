package export

// ShardValues is a helper bridging data from v1 cursors
type ShardValues interface {
	Type() interface{}
	Timestamps() []int64
	Values() interface{}
	Len() int
	String() string
}
