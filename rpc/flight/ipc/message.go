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
	"fmt"

	"github.com/influxdata/influxdb/rpc/flight/ipc/internal/flatbuf"
)

// MetadataVersion represents the Arrow metadata version.
type MetadataVersion flatbuf.MetadataVersion

const (
	MetadataV4 = MetadataVersion(flatbuf.MetadataVersionV4) // version for >= Arrow-0.8.0
)

func (m MetadataVersion) String() string {
	if v, ok := flatbuf.EnumNamesMetadataVersion[int16(m)]; ok {
		return v
	}
	return fmt.Sprintf("MetadataVersion(%d)", int16(m))
}
