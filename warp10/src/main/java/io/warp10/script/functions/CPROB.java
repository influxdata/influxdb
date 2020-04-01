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
 * Compute the probability of each value or its conditional probability.
 * If the separator is non null then the value is expected to be of type STRING
 * and with the following syntax:
 * 
 * {given1,given2,....,givenN}<SEP>event
 * 
 * Then the probability p(event|given1,given2,...,givenN) will be emitted
 * 
 */
public class CPROB extends GTSStackFunction {
  
  private static final String SEP_PARAM = "sep";
  
  public CPROB(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String,Object>();
    
    Object top = stack.pop();
    
    if (null != top && !(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects null or a String separator on top of the stack.");
    }
    
    params.put(SEP_PARAM, top);

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    String sep = (String) params.get(SEP_PARAM);
    return GTSHelper.cprob(gts, sep);
  }
}
