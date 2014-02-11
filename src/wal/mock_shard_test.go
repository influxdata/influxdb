package wal

type MockShard struct {
	Shard
	id uint32
}

func (s *MockShard) Id() uint32 {
	return s.id
}
