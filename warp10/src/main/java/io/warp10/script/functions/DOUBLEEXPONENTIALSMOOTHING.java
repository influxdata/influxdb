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
import java.util.List;
import java.util.Map;

public class DOUBLEEXPONENTIALSMOOTHING extends GTSStackFunction {
  
  private static final String ALPHA = "alpha";
  private static final String BETA = "beta";
  
  public DOUBLEEXPONENTIALSMOOTHING(String name) {
    super(name);
  }

  @Override
  protected List<GeoTimeSerie> gtsOp(Map<String,Object> params, GeoTimeSerie gts) throws WarpScriptException {
    return GTSHelper.doubleExponentialSmoothing(gts, (double) params.get(ALPHA), (double) params.get(BETA));
  }
  
  @Override
  protected Map<String,Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the 'beta' parameter (trend factor) on the top of the stack.");
    }

    double beta = ((Number) top).doubleValue();

    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects the 'alpha' parameter (smoothing factor) at the second level of the stack.");
    }

    double alpha = ((Number) top).doubleValue();
    
    if (alpha <= 0.0D || alpha >= 1.0D) {
      throw new WarpScriptException("'alpha' (smoothing factor) should be strictly between 0.0 and 1.0.");
    }
    
    if (beta <= 0.0D || beta >= 1.0D) {
      throw new WarpScriptException("'beta' (trend factor) should be strictly between 0.0 and 1.0.");
    }
    
    Map<String,Object> params = new HashMap<String, Object>();
    params.put(ALPHA, alpha);
    params.put(BETA, beta);
    
    return params;
  }
}
