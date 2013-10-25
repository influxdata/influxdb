package coordinator

type ClusterServer struct {
	Id                   uint32
	RaftName             string
	State                ServerState
	RaftConnectionString string
}

type ServerState int

const (
	LoadingRingData ServerState = iota
	SendingRingData
	DeletingOldData
	Running
	Potential
)
