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

package io.warp10.script;

import io.warp10.continuum.gts.GeoTimeSerie;

import java.util.List;
import java.util.Map;

/**
 * Interface implemented by all WarpScript filters.
 *
 */
public interface WarpScriptFilterFunction {
  /**
   * Return the subset of 'series' which satisfies the filter.
   * 
   * @param labels Labels of the partition being filtered
   * @param series GTS instances to filter, one list from each original GTS list, all parts of the same partition
   * @return A list of GTS to retain. All retained GTS MUST be in 'series'
   * 
   * @throws WarpScriptException
   */
  public List<GeoTimeSerie> filter(Map<String,String> labels, List<GeoTimeSerie>... series) throws WarpScriptException;
}
