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
 * Extract mu and sigma from a numerical GTS.
 * 
 * The computed variance has the Bessel correction applied.
 * 
 * Mu and Sigma are pushed onto the stack
 *
 * Computation uses BigDecimal to prevent overflowing for long series, this will lead to somewhat slower performance.
 */
public class MUSIGMA extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public MUSIGMA(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a boolean on top of the stack to determine if Bessel's correction should be applied or not.");
    }
    
    boolean applyBessel = Boolean.TRUE.equals(o);
    
    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series instance below the top of the stack.");
    }
    
    GeoTimeSerie gts = (GeoTimeSerie) o;
    
    int n = GTSHelper.nvalues(gts);
    
    if (0 == n) {
      throw new WarpScriptException(getName() + " can only compute mu and sigma for non empty series.");
    }
    
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
      sum = sum.add(BigDecimal.valueOf(((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue()));
      sumsq = sumsq.add(bd.multiply(bd));
    }
    
    // Push MU
    
    stack.push(sum.divide(BigDecimal.valueOf(n), BigDecimal.ROUND_HALF_UP).doubleValue());
    
    BigDecimal bdn = BigDecimal.valueOf(n);
    
    double variance = sumsq.divide(bdn, BigDecimal.ROUND_HALF_UP).subtract(sum.multiply(sum).divide(bdn.multiply(bdn), BigDecimal.ROUND_HALF_UP)).doubleValue();

    //
    // Apply Bessel's correction
    // @see http://en.wikipedia.org/wiki/Bessel's_correction
    //
        
    if (applyBessel && n > 1) {
      variance = variance * (n / (n - 1.0D));
    }
 
    // Push SIGMA
    
    stack.push(Math.sqrt(variance));
    
    return stack;
  }
}
