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

import io.warp10.continuum.gts.GTSOutliersHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

/**
 * Normalize GTS instances with their Z-score. 
 * Replace X by (X-mean)/std or (X-median)/mad if useMedian is true.
 */
public class ZSCORE extends GTSStackFunction {
  
  private static final String MODIFIED_PARAM = "USE_MEDIAN";
  
  public ZSCORE(String name) {
    super(name);
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String, Object> params = new HashMap<String, Object>();
    
    Object top = stack.pop();
    
    if (!(top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a flag (a BOOLEAN) on top of the stack indicating whether to normalize by the mean (F), or by the median (T).");
    }
    
    params.put(MODIFIED_PARAM, ((Boolean) top).booleanValue());
    
    return params;
  }
  
  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    boolean useMedian = (boolean) params.get(MODIFIED_PARAM);
    
    return GTSOutliersHelper.zScore(gts, useMedian, false);
  }  
}
