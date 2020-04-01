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

public class SINGLEEXPONENTIALSMOOTHING extends GTSStackFunction {
  
  private static final String ALPHA = "alpha";
  
  public SINGLEEXPONENTIALSMOOTHING(String name) {
    super(name);
  }
  
  @Override
  protected GeoTimeSerie gtsOp(Map<String,Object> params, GeoTimeSerie gts) throws WarpScriptException {
    return GTSHelper.singleExponentialSmoothing(gts, (double) params.get(ALPHA));
  }
  
  @Override
  protected Map<String,Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the 'alpha' parameter (smoothing factor) on the top of the stack.");
    }
    
  
    double alpha = ((Number) top).doubleValue();
    
    if (alpha <= 0.0D || alpha >= 1.0D) {
      throw new WarpScriptException("'alpha' (smoothing factor) should be strictly between 0.0 and 1.0.");
    }
    
    Map<String,Object> params = new HashMap<String, Object>();
    params.put(ALPHA, alpha);

    return params;
  }
}
