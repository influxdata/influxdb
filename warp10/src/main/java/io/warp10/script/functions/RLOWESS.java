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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Apply fast and robust LOWESS smoothing
 * @see http://en.wikipedia.org/wiki/Local_regression
 * @see http://www.stat.washington.edu/courses/stat527/s14/readings/Cleveland_JASA_1979.pdf
 * @see http://streaming.stat.iastate.edu/~stat416/LectureNotes/handout_LOWESS.pdf
 * @see http://slendermeans.org/lowess-speed.html
 * @see http://www.itl.nist.gov/div898/handbook/pmd/section1/pmd144.htm
 */
public class RLOWESS extends GTSStackFunction {
  
  private static final String BANDWIDTH_PARAM = "q";
  private static final String ROBUSTNESS_PARAM = "r";
  private static final String DELTA_PARAM = "d";
  private static final String DEGREE_PARAM = "p";
  
  public RLOWESS(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String,Object>();
    
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a degree (a LONG) for the polynomial fit on top of the stack.");
    }
    
    params.put(DEGREE_PARAM, ((Number) top).intValue());
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a delta radius (a LONG in µs) within which LOWESS is computed only once, in 2nd position from the top of the stack.");
    }
    
    params.put(DELTA_PARAM, ((Number) top).longValue());
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of robustness iteration (a LONG) in 3rd position from the top of the stack.");
    }
    
    params.put(ROBUSTNESS_PARAM, ((Number) top).intValue());
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a bandwidth (a LONG) in 4th position from the top of the stack.");
    }
    
    params.put(BANDWIDTH_PARAM, ((Number) top).intValue());
    

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    int q = (int) params.get(BANDWIDTH_PARAM);
    int r = (int) params.get(ROBUSTNESS_PARAM);
    long d = (long) params.get(DELTA_PARAM);
    int p = (int) params.get(DEGREE_PARAM);
    
    // if d is negative, set it as a 10^d fraction of lastick-firstick
    if (d < 0){
      d = (long) Math.pow(10,d) * (GTSHelper.lasttick(gts) - GTSHelper.firsttick(gts));
    }

    return GTSHelper.rlowess(gts, q, r, d, p);

  }
}