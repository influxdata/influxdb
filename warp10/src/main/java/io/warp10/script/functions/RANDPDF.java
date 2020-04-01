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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

/**
 * Produces a function which emits a random number according to a given distribution
 * specified by a histogram
 */
public class RANDPDF extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final SecureRandom sr = new SecureRandom();
  
  private final Object[] values;
  private final double[] cumulativeProbability;
  private Map<Object,Double> probabilities;
  private final boolean seeded;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    private final boolean seeded;

    public Builder(String name, boolean seeded) {
      super(name);
      this.seeded = seeded;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object histogram = stack.pop();
      
      if (!(histogram instanceof Map)) {
        throw new WarpScriptException(getName() + " expects a value histogram on top of the stack.");
      }
      
      Map<Object, Double> probabilities = new TreeMap<Object, Double>();
      
      double total = 0.0D;
      
      for (Number count: ((Map<Object,Number>) histogram).values()) {
        total += ((Number) count).doubleValue();
      }
      
      for (Entry<Object,Number> entry: ((Map<Object,Number>) histogram).entrySet()) {
        probabilities.put(entry.getKey(), entry.getValue().doubleValue() / total);
      }

      stack.push(new RANDPDF(getName(), probabilities, seeded));
      
      return stack;
    }
  }

  public RANDPDF(String name, Map<Object,Double> probabilities, boolean seeded) {
    super(name);
    
    this.probabilities = probabilities;
    this.values = new Object[probabilities.size()];
    this.cumulativeProbability = new double[probabilities.size()];
    this.seeded = seeded;
    
    double cumulative = 0.0D;
    
    int idx = 0;
    
    for (Entry<Object, Double> entry: probabilities.entrySet()) {
      cumulative += entry.getValue();
      this.values[idx] = entry.getKey();
      this.cumulativeProbability[idx++] = cumulative;
    }
    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    //
    // Generate a random number
    //

    double rand;
    if (seeded) {
      Random prng = (Random) stack.getAttribute(PRNG.ATTRIBUTE_SEEDED_PRNG);

      if (null == prng) {
        throw new WarpScriptException(getName() + " seeded PRNG was not initialized.");
      }

      rand = prng.nextDouble();

    } else {
      rand = sr.nextDouble();
    }
    
    //
    // Check where rand would be inserted in the 'cumulativeProbability' array
    //
    
    int pos = Arrays.binarySearch(this.cumulativeProbability, rand);
    
    if (pos >= 0) {
      stack.push(this.values[pos]);
    } else {
      pos = -pos - 1;
      if (this.cumulativeProbability.length == pos) {
        stack.push(this.values[this.values.length - 1]);
      } else {
        stack.push(this.values[pos]);
      }
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
