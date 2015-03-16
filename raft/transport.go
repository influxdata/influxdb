package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
)

// HTTPTransport represents a transport for sending RPCs over the HTTP protocol.
type HTTPTransport struct{}

// Join requests membership into a node's cluster.
func (t *HTTPTransport) Join(uri url.URL, nodeURL url.URL) (uint64, uint64, *Config, error) {
	// Construct URL.
	u := uri
	u.Path = path.Join(u.Path, "raft/join")
	u.RawQuery = (&url.Values{"url": {nodeURL.String()}}).Encode()

	// Send HTTP request.
	resp, err := http.Get(u.String())
	if err != nil {
		return 0, 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Parse returned error.
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		return 0, 0, nil, errors.New(s)
	}

	// Parse returned id.
	id, err := strconv.ParseUint(resp.Header.Get("X-Raft-ID"), 10, 64)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("invalid id: %q", resp.Header.Get("X-Raft-ID"))
	}

	// Parse returned id.
	leaderID, err := strconv.ParseUint(resp.Header.Get("X-Raft-Leader-ID"), 10, 64)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("invalid leader id: %q", resp.Header.Get("X-Raft-Leader-ID"))
	}

	// Unmarshal config.
	var config *Config
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return 0, 0, nil, fmt.Errorf("config unmarshal: %s", err)
	}

	return id, leaderID, config, nil
}

// Leave removes a node from a cluster's membership.
func (t *HTTPTransport) Leave(uri url.URL, id uint64) error {
	// Construct URL.
	u := uri
	u.Path = path.Join(u.Path, "raft/leave")
	u.RawQuery = (&url.Values{"id": {strconv.FormatUint(id, 10)}}).Encode()

	// Send HTTP request.
	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// Parse returned error.
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		return errors.New(s)
	}

	return nil
}

// Heartbeat checks the status of a follower.
func (t *HTTPTransport) Heartbeat(uri url.URL, term, commitIndex, leaderID uint64) (uint64, error) {
	// Construct URL.
	u := uri
	u.Path = path.Join(u.Path, "raft/heartbeat")

	// Set URL parameters.
	v := &url.Values{}
	v.Set("term", strconv.FormatUint(term, 10))
	v.Set("commitIndex", strconv.FormatUint(commitIndex, 10))
	v.Set("leaderID", strconv.FormatUint(leaderID, 10))
	u.RawQuery = v.Encode()

	// Send HTTP request.
	resp, err := http.Get(u.String())
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()

	// Parse returned index.
	newIndexString := resp.Header.Get("X-Raft-Index")
	newIndex, err := strconv.ParseUint(newIndexString, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid index: %q", newIndexString)
	}

	// Parse returned error.
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		return newIndex, errors.New(s)
	}

	return newIndex, nil
}

// ReadFrom streams the log from a leader.
func (t *HTTPTransport) ReadFrom(uri url.URL, id, term, index uint64) (io.ReadCloser, error) {
	// Construct URL.
	u := uri
	u.Path = path.Join(u.Path, "raft/stream")

	// Set URL parameters.
	v := &url.Values{}
	v.Set("id", strconv.FormatUint(id, 10))
	v.Set("term", strconv.FormatUint(term, 10))
	v.Set("index", strconv.FormatUint(index, 10))
	u.RawQuery = v.Encode()

	// Send HTTP request.
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	// Parse returned error.
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		_ = resp.Body.Close()
		return nil, errors.New(s)
	}

	return resp.Body, nil
}

// RequestVote requests a vote for a candidate in a given term.
func (t *HTTPTransport) RequestVote(uri url.URL, term, candidateID, lastLogIndex, lastLogTerm uint64) error {
	// Construct URL.
	u := uri
	u.Path = path.Join(u.Path, "raft/vote")

	// Set URL parameters.
	v := &url.Values{}
	v.Set("term", strconv.FormatUint(term, 10))
	v.Set("candidateID", strconv.FormatUint(candidateID, 10))
	v.Set("lastLogIndex", strconv.FormatUint(lastLogIndex, 10))
	v.Set("lastLogTerm", strconv.FormatUint(lastLogTerm, 10))
	u.RawQuery = v.Encode()

	// Send HTTP request.
	resp, err := http.Get(u.String())
	if err != nil {
		return err
	}
	_ = resp.Body.Close()

	// Parse returned error.
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		return errors.New(s)
	}

	return nil
}
