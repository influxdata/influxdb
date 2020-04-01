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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

/**
 * Downsamples a GTS using LTTB (Largest Triangle Three Bucket)
 * @see http://skemman.is/stream/get/1946/15343/37285/3/SS_MSthesis.pdf
 */
public class LTTB extends GTSStackFunction {
  
  private static final String THRESHOLD = "threshold";
  
  private final boolean timebased;
  
  public LTTB(String name, boolean timebased) {
    super(name);
    this.timebased = timebased;
  }
  
  @Override
  protected GeoTimeSerie gtsOp(Map<String,Object> params, GeoTimeSerie gts) throws WarpScriptException {
    int threshold = (int) params.get(THRESHOLD);
    
    return GTSHelper.lttb(gts, threshold, this.timebased);
  }
  
  @Override
  protected Map<String,Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a threshold on top of the stack.");
    }
    
    int threshold = ((Number) top).intValue();
   
    Map<String,Object> params = new HashMap<String, Object>();
    params.put(THRESHOLD, threshold);

    return params;
  }
}
