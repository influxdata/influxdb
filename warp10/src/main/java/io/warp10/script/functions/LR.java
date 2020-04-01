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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Compute the simple linear regression parameters alpha and beta for the given numeric GTS
 * 
 * Computation uses BigDecimal to prevent overflowing for long series, this will lead to somewhat slower performance.
 */
public class LR extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LR(String name) {
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
      throw new WarpScriptException(getName() + " can only compute simple linear regression parameters for numerical series.");
    }
    
    BigDecimal zero = new BigDecimal(0.0D, MathContext.UNLIMITED).setScale(16);
    BigDecimal sumx = zero;
    BigDecimal sumy = zero;
    BigDecimal sumxx = zero;
    BigDecimal sumxy = zero;
    
    for (int i = 0; i < n; i++) {
      BigDecimal bd;
      if (TYPE.DOUBLE == gts.getType()) {
        bd = BigDecimal.valueOf(((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue()); 
      } else {
        bd = BigDecimal.valueOf(((Number) GTSHelper.valueAtIndex(gts, i)).longValue()); 
      }
      BigDecimal tick = BigDecimal.valueOf(GTSHelper.tickAtIndex(gts, i)); 
      sumx = sumx.add(tick);
      sumy = sumy.add(bd);
      sumxx = sumxx.add(tick.multiply(tick));
      sumxy = sumxy.add(bd.multiply(tick));
    }

    BigDecimal N = BigDecimal.valueOf((double) GTSHelper.nvalues(gts));
        
    BigDecimal xybar = sumxy.divide(N, RoundingMode.HALF_UP);
    BigDecimal xbar = sumx.divide(N, RoundingMode.HALF_UP);
    BigDecimal ybar = sumy.divide(N, RoundingMode.HALF_UP);
    BigDecimal xxbar = sumxx.divide(N, RoundingMode.HALF_UP);
    
    BigDecimal betaHat = xybar.subtract(xbar.multiply(ybar)).divide(xxbar.subtract(xbar.multiply(xbar)), RoundingMode.HALF_UP);
   
    BigDecimal alphaHat = ybar.subtract(betaHat.multiply(xbar));
    
    stack.push(alphaHat.doubleValue());
    stack.push(betaHat.doubleValue());

    return stack;
  }
}
