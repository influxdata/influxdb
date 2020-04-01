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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * Pushes onto the stack a list of M-1 bounds defining M intervals whose area under
 * the bell curve N(mu,sigma) are all the same. 
 * 
 * Expects on the stack
 * 
 * 3: MU
 * 2: SIGMA
 * 1: M
 *
 */
public class NBOUNDS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public NBOUNDS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of intervals on top of the stack.");
    }
    
    int n = ((Number) top).intValue();
    
    if (n < 1) {
      throw new WarpScriptException(getName() + " cannot generate bounds for less than 2 intervals.");
    }
    
    if (n > 65536) {
      throw new WarpScriptException(getName() + " cannot generate bounds for more than 65536 intervals.");      
    }
    
    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a standard deviation below the number of intervals.");
    }
    
    double sigma = ((Number) top).doubleValue();
    
    if (sigma <= 0.0D) {
      throw new WarpScriptException(getName() + " expects a standard deviation strictly positive.");
    }
    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a mean below the standard deviation.");
    }
    
    double mu = ((Number) top).doubleValue();
    
    NormalDistribution nd = new org.apache.commons.math3.distribution.NormalDistribution(mu, sigma);
    
    List<Object> bounds = new ArrayList<Object>(n - 1);
    
    double area = 1.0D / n;
    
    for (int i = 1; i <= n - 1; i++) {
      bounds.add(nd.inverseCumulativeProbability(i * area));
    }
    
    stack.push(bounds);
    
    return stack;
  }
}
