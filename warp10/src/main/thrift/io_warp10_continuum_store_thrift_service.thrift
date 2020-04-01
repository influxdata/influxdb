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

include "io_warp10_continuum_store_thrift_data.thrift"
namespace java io.warp10.continuum.store.thrift.service

service DirectoryService {
  /**
   * Find Metadatas of matching GTS instances
   */
  io_warp10_continuum_store_thrift_data.DirectoryFindResponse find(1:io_warp10_continuum_store_thrift_data.DirectoryFindRequest request);
  
  /**
   * Retrieve Metadata associated with a given classId/labelsId
   */
  io_warp10_continuum_store_thrift_data.DirectoryGetResponse get(1:io_warp10_continuum_store_thrift_data.DirectoryGetRequest request);
  
  /**
   * Retrieve statistics associated with selected GTS instances
   */
  io_warp10_continuum_store_thrift_data.DirectoryStatsResponse stats(1:io_warp10_continuum_store_thrift_data.DirectoryStatsRequest request);    
}
