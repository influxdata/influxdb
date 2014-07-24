package wal

type MockServer struct {
	Server
	id uint32
}

func (s *MockServer) Id() uint32 {
	return s.id
}
