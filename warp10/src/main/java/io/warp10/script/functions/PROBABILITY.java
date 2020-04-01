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
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Probability Function from a value histogram
 */
public class PROBABILITY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final Map<Object,Double> probabilities;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object histogram = stack.pop();
      
      if (!(histogram instanceof Map)) {
        throw new WarpScriptException(getName() + " expects a value histogram on top of the stack.");
      }
      
      Map<Object, Double> probabilities = new HashMap<Object, Double>();
      
      double total = 0.0D;
      
      for (Number count: ((Map<Object,Number>) histogram).values()) {
        total += ((Number) count).doubleValue();
      }
      
      for (Entry<Object,Number> entry: ((Map<Object,Number>) histogram).entrySet()) {
        probabilities.put(entry.getKey(), entry.getValue().doubleValue() / total);
      }

      stack.push(new PROBABILITY(getName(), probabilities));
      
      return stack;
    }
  }

  public PROBABILITY(String name, Map<Object,Double> probabilities) {
    super(name);
    this.probabilities = probabilities;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
      
    if (!this.probabilities.containsKey(o)) {
      stack.push(0.0D);
    } else {
      stack.push(this.probabilities.get(o));
    }

    return stack;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(WarpScriptLib.MAP_START);
    sb.append(" ");
    for (Entry<Object,Double> entry: this.probabilities.entrySet()) {
      sb.append(StackUtils.toString(entry.getKey()));
      sb.append(" ");
      sb.append(StackUtils.toString(entry.getValue()));
      sb.append(" ");
    }
    sb.append(" ");
    sb.append(WarpScriptLib.MAP_END);
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
