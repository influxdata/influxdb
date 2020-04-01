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
 * Apply LOWESS smoothing
 * @see http://en.wikipedia.org/wiki/Local_regression
 * @see http://streaming.stat.iastate.edu/~stat416/LectureNotes/handout_LOWESS.pdf
 * @see http://www.itl.nist.gov/div898/handbook/pmd/section1/pmd144.htm
 * @see http://www.itl.nist.gov/div898/handbook/pmd/section1/dep/dep144.htm
 */
public class LOWESS extends GTSStackFunction {
  
  private static final String BANDWIDTH_PARAM = "q";
  
  public LOWESS(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String,Object>();
    
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a bandwidth (a LONG) on top of the stack.");
    }
    
    params.put(BANDWIDTH_PARAM, ((Number) top).intValue());

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    int q = (int) params.get(BANDWIDTH_PARAM);
    return GTSHelper.rlowess(gts, q, 0, 0, 1);

  }
}
