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
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.math.BigDecimal;

/**
 * Extract the number of values and the sum of values and sum of square of values.
 * 
 * This is used to compute MU/SIGMA in a distributed way for a large number of similar GTS
 * 
 * Computation uses BigDecimal to prevent overflowing for long series, this will lead to somewhat slower performance.
 */
public class NSUMSUMSQ extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public NSUMSUMSQ(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
        
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series instance on top of the stack.");
    }
    
    GeoTimeSerie gts = (GeoTimeSerie) o;
    
    int n = GTSHelper.nvalues(gts);
    
    if (TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) {
      throw new WarpScriptException(getName() + " can only compute mu and sigma for numerical series.");
    }
    
    BigDecimal sum = BigDecimal.valueOf(0.0D);
    BigDecimal sumsq = BigDecimal.valueOf(0.0D);
    
    for (int i = 0; i < n; i++) {
      BigDecimal bd;
      if (TYPE.DOUBLE == gts.getType()) {
        bd = BigDecimal.valueOf(((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue()); 
      } else {
        bd = BigDecimal.valueOf(((Number) GTSHelper.valueAtIndex(gts, i)).longValue()); 
      }
      sum = sum.add(bd);
      sumsq = sumsq.add(bd.multiply(bd));
    }

    stack.push(n);
    stack.push(sum.doubleValue());
    stack.push(sumsq.doubleValue());

    return stack;
  }
}
