//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10.warp.sdk;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;

public abstract class IngressPlugin {
  
  public void init() {}
  
  /**
   * Called when a line of GTS Input Format has been parsed.
   * 
   * @param ingress Instance which parsed the line (either Ingress or StandaloneIngressHandler)
   * @param token Write token used to push the data
   * @param line Text line in GTS Input Format
   * @param encoder Parsed encoder
   * @return true if the encoder can be processed further, false if it should be discarded
   */
  public boolean update(Object ingress, WriteToken token, String line, GTSEncoder encoder) {
    return true;
  }
  
  /**
   * Called when a line of the input to the /meta endpoint has been parsed
   * 
   * @param ingress Instance which parsed the line (either Ingress or StandaloneIngressHandler)
   * @param token Write token used to perform the metadata modification
   * @param line Line which was parsed
   * @param metadata Parsed Metadata
   * @return true if the Metadata can be further processed, false if the change should be ignored
   */
  public boolean meta(Object ingress, WriteToken token, String line, Metadata metadata) {
    return true;
  }
  
  /**
   * Called for each GTS identified as a deletion candidate
   * 
   * @param ingress Instance which processes the delete (either Ingress or StandaloneDeleteHandler)
   * @param token Write token used for the delete
   * @param metadata Metadata of the GTS to delete
   * @return true if deletion of this GTS should continue, false otherwise
   */
  public boolean delete(Object ingress, WriteToken token, Metadata metadata) {
    return true;
  }
  
  /**
   * Called when the processing of an update, meta or delete operation ends
   * @param ingress Instance
   */
  public void flush(Object ingress) { }   
}
