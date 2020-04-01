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
 * Pushes onto the stack a list of M-1 bounds defining M + 2 intervals between 'a' and 'b'
 * plus the two intervals before 'a' and after 'b'
 * 
 * Expects on the stack
 * 
 * 3: a
 * 2: b
 * 1: M
 *
 */
public class LBOUNDS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LBOUNDS(String name) {
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
      throw new WarpScriptException(getName() + " cannot generate bounds for less than 1 interval.");
    }
    
    if (n > 65536) {
      throw new WarpScriptException(getName() + " cannot generate bounds for more than 65536 intervals.");      
    }
    
    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects an upper bound below the number of intervals.");
    }
    
    double upper = ((Number) top).doubleValue();
    
    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a lower bounds below the upper bound.");
    }
    
    double lower = ((Number) top).doubleValue();
    
    if (lower >= upper) {
      throw new WarpScriptException(getName() + " expects the lower bound to be strictly less than the upper bound.");
    }
    
    List<Object> bounds = new ArrayList<Object>(n - 1);
    
    double bound = lower;
    double step = (upper - lower) / n;
    
    if (0.0 == step) {
      throw new WarpScriptException(getName() + " cannot operate if bounds are too close.");
    }
    
    bounds.add(lower);
    
    for (int i = 1; i < n; i++) {
      bounds.add(lower + i * step);
    }
    
    bounds.add(upper);
    
    stack.push(bounds);
    
    return stack;
  }
}
