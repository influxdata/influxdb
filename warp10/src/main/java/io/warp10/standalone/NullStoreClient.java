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

package io.warp10.standalone;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;

import java.io.IOException;

public class NullStoreClient implements StoreClient {
  
  @Override
  public GTSDecoderIterator fetch(ReadToken token, java.util.List<io.warp10.continuum.store.thrift.data.Metadata> metadatas, long now, long then, long count, long skip, double sample, boolean writeTimestamp, final int preBoundary, final int postBoundary) {
    return null;
  }
  
  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {}
  
  @Override
  public void store(GTSEncoder encoder) throws IOException {}
  
  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {}

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException { return 0L; }
}
