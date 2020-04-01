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
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for keeping track of GTS classId + labelsId
 * to metadata association and related registration/search features.
 *
 */
public interface GTSDirectory {
  
  /**
   * Retrieve classId for a Geo Time Serie
   * 
   * @param gts GTS instance for which to retrieve the classId
   * @return the computed classId
   */
  public long getClassId(String name);
  
  /**
   * Retrieve labelsId for a Geo Time Serie
   * 
   * @param gts GTS instance for which to retrieve the labelsId
   * @return the computed labelsId
   */
  public long getLabelsId(Map<String,String> labels);
  
  /**
   * Register the given Geo Time Serie under the specified classId/labelsId.
   * 
   * classId and labelsId are passed as parameters so as to leave the responsibility
   * of choosing hash keys to external entities or to avoid recomputing them
   * if they have been previously computed.
   * 
   * @param classId classId under which to register the GTS
   * @param labelsId labelsId under which to register the GTS
   * @param name Name associated with classId
   * @param labels Labels associated with labelsId
   * 
   * @throws IOException if something goes wrong
   */
  public void register(long classId, long labelsId, String name, Map<String,String> labels) throws IOException;
  
  /**
   * Register the given GTS using classId/labelsId returned by get{Class,Labels}Id.
   * 
   * @param gts GTS instance to register
   * @throws IOException if something goes wrong
   */
  public void register(GeoTimeSerie gts) throws IOException;
  
  /**
   * Register the GTS which this encoder encodes.
   * 
   * @param encoder GTSEncoder instance whose GTS is to be registered
   * 
   * @throws IOException
   */
  public void register(GTSEncoder encoder) throws IOException;
  
  /**
   * Retrieve Metadata by classId/labelsId.
   * 
   * @param classId
   * @param labelsId
   * 
   * @return The matching Metadata or null if none were found.
   */
  public Metadata getMetadata(long classId, long labelsId);
  
  /**
   * Retrieve matching geo time series metadata from class and labels expressions.
   * 
   * Expressions can either be
   * 
   * EXACT_MATCH
   * =EXACT_MATCH
   * ~REGULAR_EXPRESSION
   * 
   * The names of labels must match exactly and should not use the above syntax.
   * 
   * @param classExpr
   * @param labelsExpr
   * @return
   */
  public List<Metadata> find(String classExpr, Map<String,String> labelsExpr);
  
  /**
   * Update metadata for a GTS
   * 
   * @param classId classId of the GTS to update
   * @param labelsId labelsId of the GTS to update
   * @param metadata new Metadata
   */
  public void updateMetadata(long classId, long labelsId, Metadata metadata) throws IOException;
}

