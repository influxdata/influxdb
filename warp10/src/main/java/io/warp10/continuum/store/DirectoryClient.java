//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.continuum.store;

import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface DirectoryClient {
  public List<Metadata> find(DirectoryRequest request) throws IOException;
  
  public Map<String,Object> stats(DirectoryRequest request) throws IOException;
  
  public MetadataIterator iterator(DirectoryRequest request) throws IOException;
}
