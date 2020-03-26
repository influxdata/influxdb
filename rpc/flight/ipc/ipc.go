// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ipc

import (
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/memory"
)

const (
	errInconsistentSchema = errString("arrow/ipc: tried to write record batch with different schema")
	errMaxRecursion       = errString("arrow/ipc: max recursion depth reached")
	errBigArray           = errString("arrow/ipc: array larger than 2^31-1 in length")

	kArrowAlignment    = 64 // buffers are padded to 64b boundaries (for SIMD)
	kArrowIPCAlignment = 8  // align on 8b boundaries in IPC
)

var (
	paddingBytes  [kArrowAlignment]byte
	kEOS                 = [8]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0} // end of stream message
	kIPCContToken uint32 = 0xFFFFFFFF                                  // 32b continuation indicator for FlatBuffers 8b alignment
)

func paddedLength(nbytes int64, alignment int32) int64 {
	align := int64(alignment)
	return ((nbytes + align - 1) / align) * align
}

type errString string

func (s errString) Error() string {
	return string(s)
}

type ReadAtSeeker interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

type config struct {
	alloc  memory.Allocator
	schema *arrow.Schema
}

func newConfig(opts ...Option) *config {
	cfg := &config{
		alloc: memory.NewGoAllocator(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// Option is a functional option to configure opening or creating Arrow files
// and streams.
type Option func(*config)

// WithAllocator specifies the Arrow memory allocator used while building records.
func WithAllocator(mem memory.Allocator) Option {
	return func(cfg *config) {
		cfg.alloc = mem
	}
}

// WithSchema specifies the Arrow schema to be used for reading or writing.
func WithSchema(schema *arrow.Schema) Option {
	return func(cfg *config) {
		cfg.schema = schema
	}
}

var (
	_ arrio.Writer = (*Writer)(nil)
)
