package httpd

import (
	"container/list"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/services/meta"
)

type RequestInfo struct {
	IPAddr   string
	Username string
}

type RequestStats struct {
	Writes  int64 `json:"writes"`
	Queries int64 `json:"queries"`
}

func (r *RequestInfo) String() string {
	if r.Username != "" {
		return fmt.Sprintf("%s:%s", r.Username, r.IPAddr)
	}
	return r.IPAddr
}

type RequestProfile struct {
	tracker *RequestTracker
	elem    *list.Element

	mu       sync.RWMutex
	Requests map[RequestInfo]*RequestStats
}

func (p *RequestProfile) AddWrite(info RequestInfo) {
	p.add(info, p.addWrite)
}

func (p *RequestProfile) AddQuery(info RequestInfo) {
	p.add(info, p.addQuery)
}

func (p *RequestProfile) add(info RequestInfo, fn func(*RequestStats)) {
	// Look for a request entry for this request.
	p.mu.RLock()
	st := p.Requests[info]
	p.mu.RUnlock()
	if st != nil {
		fn(st)
		return
	}

	// There is no entry in the request tracker. Create one.
	p.mu.Lock()
	if st := p.Requests[info]; st != nil {
		// Something else created this entry while we were waiting for the lock.
		p.mu.Unlock()
		fn(st)
		return
	}

	st = &RequestStats{}
	p.Requests[info] = st
	p.mu.Unlock()
	fn(st)
}

func (p *RequestProfile) addWrite(st *RequestStats) {
	atomic.AddInt64(&st.Writes, 1)
}

func (p *RequestProfile) addQuery(st *RequestStats) {
	atomic.AddInt64(&st.Queries, 1)
}

// Stop informs the RequestTracker to stop collecting statistics for this
// profile.
func (p *RequestProfile) Stop() {
	p.tracker.mu.Lock()
	p.tracker.profiles.Remove(p.elem)
	p.tracker.mu.Unlock()
}

type RequestTracker struct {
	mu       sync.RWMutex
	profiles *list.List
}

func NewRequestTracker() *RequestTracker {
	return &RequestTracker{
		profiles: list.New(),
	}
}

func (rt *RequestTracker) TrackRequests() *RequestProfile {
	// Perform the memory allocation outside of the lock.
	profile := &RequestProfile{
		Requests: make(map[RequestInfo]*RequestStats),
		tracker:  rt,
	}

	rt.mu.Lock()
	profile.elem = rt.profiles.PushBack(profile)
	rt.mu.Unlock()
	return profile
}

func (rt *RequestTracker) Add(req *http.Request, user meta.User) {
	rt.mu.RLock()
	if rt.profiles.Len() == 0 {
		rt.mu.RUnlock()
		return
	}
	defer rt.mu.RUnlock()

	var info RequestInfo
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return
	}

	info.IPAddr = host
	if user != nil {
		info.Username = user.ID()
	}

	// Add the request info to the profiles.
	for p := rt.profiles.Front(); p != nil; p = p.Next() {
		profile := p.Value.(*RequestProfile)
		if req.URL.Path == "/query" {
			profile.AddQuery(info)
		} else if req.URL.Path == "/write" {
			profile.AddWrite(info)
		}
	}
}
