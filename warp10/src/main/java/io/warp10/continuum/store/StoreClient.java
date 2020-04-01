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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

import java.io.IOException;
import java.util.List;

public interface StoreClient {
  public void store(GTSEncoder encoder) throws IOException;
  public void archive(int chunk, GTSEncoder encoder) throws IOException;
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException;
  /**
   * 
   * @param token Read token to use for reading data
   * @param metadatas List of Metadata for the GTS to fetch
   * @param now End timestamp (included)
   * @param then Start timestamp (included)
   * @param count Number of datapoints to fetch. 0 is a valid value if you want to fetch only boundaries. Use -1 to specify you are not fetching by count.
   * @param skip Number of datapoints to skip before returning values
   * @param sample Double value representing the sampling rate. Use 1.0D for returning all values. Valid values are ] 0.0D, 1.0D ]
   * @param writeTimestamp Flag indicating we are interested in the HBase cell timestamp
   * @param preBoundary Size of the pre boundary in number of values
   * @param postBoundary Size of the post boundary in number of values
   * @return
   * @throws IOException
   */
  public GTSDecoderIterator fetch(ReadToken token, final List<Metadata> metadatas, final long now, final long then, long count, long skip, double sample, boolean writeTimestamp, final int preBoundary, final int postBoundary) throws IOException;
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler);
}
