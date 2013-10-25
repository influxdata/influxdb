package coordinator

type ClusterServer struct {
	Id       uint32
	RaftName string
	State    ServerState
}

type ServerState int

const (
	LoadingRingData ServerState = iota
	DeletingOldData
	Running
)
