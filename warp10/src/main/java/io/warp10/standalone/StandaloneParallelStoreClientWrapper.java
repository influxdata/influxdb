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

import java.io.IOException;
import java.util.List;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;

public class StandaloneParallelStoreClientWrapper implements StoreClient {
  private final StoreClient parent;
  
  public StandaloneParallelStoreClientWrapper(StoreClient parent) {
    this.parent = parent;
  }
  
  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    parent.addPlasmaHandler(handler);
  }
  
  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    parent.archive(chunk, encoder);
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    return parent.delete(token, metadata, start, end);
  }
  
  @Override
  public GTSDecoderIterator fetch(ReadToken token, List<Metadata> metadatas, long now, long then, long count, long skip, double sample, boolean writeTimestamp, final int preBoundary, final int postBoundary) throws IOException {
    if (ParallelGTSDecoderIteratorWrapper.useParallelScanners()) {
      return new ParallelGTSDecoderIteratorWrapper(parent, token, now, then, count, skip, sample, metadatas, preBoundary, postBoundary);
    } else {
      return parent.fetch(token, metadatas, now, then, count, skip, sample, writeTimestamp, preBoundary, postBoundary);
    }
  }
  
  @Override
  public void store(GTSEncoder encoder) throws IOException {
    parent.store(encoder);
  }
}
