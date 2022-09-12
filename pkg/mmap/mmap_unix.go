//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd

// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mmap provides a way to memory-map a file.
package mmap

import (
	"os"
	"syscall"

	errors2 "github.com/influxdata/influxdb/v2/pkg/errors"
)

// Map memory-maps a file.
func Map(path string, sz int64) (data []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer errors2.Capture(&err, f.Close)()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	} else if fi.Size() == 0 {
		return nil, nil
	}

	// Use file size if map size is not passed in.
	if sz == 0 {
		sz = fi.Size()
	}

	data, err = syscall.Mmap(int(f.Fd()), 0, int(sz), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Unmap closes the memory-map.
func Unmap(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.Munmap(data)
}
