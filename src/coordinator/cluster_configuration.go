package coordinator

import (
	"sync"
)

type ClusterConfiguration struct {
	MaxRingLocation           int64
	RingLocationToServers     map[int64][]string
	ringLocationToServersLock sync.RWMutex
	ReadApiKeys               map[string]bool
	readApiKeysLock           sync.RWMutex
	WriteApiKeys              map[string]bool
	writeApiKeysLock          sync.RWMutex
}

type ApiKeyType int

const (
	ReadKey ApiKeyType = iota
	WriteKey
)

func NewClusterConfiguration(maxRingLocation int64) *ClusterConfiguration {
	return &ClusterConfiguration{
		MaxRingLocation:       maxRingLocation,
		RingLocationToServers: make(map[int64][]string),
		ReadApiKeys:           make(map[string]bool),
		WriteApiKeys:          make(map[string]bool),
	}
}

func (self *ClusterConfiguration) AddRingLocationToServer(hostnameAndPort string, ringLocation int64) {
	self.ringLocationToServersLock.Lock()
	defer self.ringLocationToServersLock.Unlock()
	self.RingLocationToServers[ringLocation] = append(self.RingLocationToServers[ringLocation], hostnameAndPort)
}

func (self *ClusterConfiguration) RemoveRingLocationFromServer(hostnameAndPort string, ringLocation int64) {
	self.ringLocationToServersLock.Lock()
	defer self.ringLocationToServersLock.Unlock()
	oldLocations := self.RingLocationToServers[ringLocation]
	newLocations := make([]string, 0, len(oldLocations))
	for _, l := range oldLocations {
		if l != hostnameAndPort {
			newLocations = append(newLocations, l)
		}
	}
	self.RingLocationToServers[ringLocation] = newLocations
}

func (self *ClusterConfiguration) AddApiKey(database, key string, apiKeyType ApiKeyType) {
	if apiKeyType == ReadKey {
		self.readApiKeysLock.Lock()
		defer self.readApiKeysLock.Unlock()
		self.ReadApiKeys[database+key] = true
	} else {
		self.writeApiKeysLock.Lock()
		defer self.writeApiKeysLock.Unlock()
		self.WriteApiKeys[database+key] = true
	}
}

func (self *ClusterConfiguration) DeleteApiKey(database, key string) {
	self.readApiKeysLock.Lock()
	self.writeApiKeysLock.Lock()
	defer self.readApiKeysLock.Unlock()
	defer self.writeApiKeysLock.Unlock()
	fullKey := database + key
	delete(self.ReadApiKeys, fullKey)
	delete(self.WriteApiKeys, fullKey)
}

func (self *ClusterConfiguration) IsValidReadKey(database, key string) bool {
	self.readApiKeysLock.RLock()
	defer self.readApiKeysLock.RUnlock()
	return self.ReadApiKeys[database+key]
}

func (self *ClusterConfiguration) IsValidWriteKey(database, key string) bool {
	self.writeApiKeysLock.RLock()
	defer self.writeApiKeysLock.RUnlock()
	return self.WriteApiKeys[database+key]
}
