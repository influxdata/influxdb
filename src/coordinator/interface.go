package coordinator

import (
	"datastore"
	"protocol"
	"query"
)

type Coordinator interface {
	DistributeQuery(query *query.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(series *protocol.Series) error
}

type ClusterServer interface {
	Datastore
	RingLocations() []*RingLocation
	AddRingLocation(ringLocation *RingLocation) error
	RemoveRingLocation(ringLocation *RingLocation) error
}

type ClusterConfiguration interface {
	JoinServer(seedServers []string, server string) error
	RemoveServer(server string)
	GetClusterServersForRingLocation(ringLocation int64) []*ClusterServer
	GetRingLocationsForQuery(query *query.Query) []int64

	getServers() []*ClusterServer
	splitSeriesIntoLocations(series *protocol.Series) []*SeriesAndRing
}

type ClusterServerImpl struct {
	host          string
	ringLocations []*RingLocation
}

type RingLocation struct {
	location      int64
	totalData     int64
	averageVolume int64
}

type SeriesAndRing struct {
	series       *protocol.Series
	ringLocation int64
}
