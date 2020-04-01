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

package io.warp10.script.aggregator;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

/**
 * Compute a Shannon Entropy, considering GTS values to be number of occurrences of the underlying symbol.
 * 
 * The nature of the underlying symbol is not important.
 * 
 * The returned entropy is normalized by log(N) where N is the sum of occurrences.
 */
public class ShannonEntropy extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  private final boolean invert;
  
  public ShannonEntropy(String name, boolean invert) {
    super(name);
    this.invert = invert;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    double sum = 0.0D;
    
    //
    // Compute sum of occurrence counts
    //
    
    int n = 0;
    
    for (int i = 0; i < values.length; i++) {
      if (null == values[i]) {
        continue;
      }
      if (!(values[i] instanceof Long)) {
        throw new WarpScriptException(getName() + " can only operate on long values.");
      }
      n++;
      sum += (long) values[i];
    }
    
    //
    // Compute the Shannon Entropy
    //
    
    double H = 0.0D;
    
    if (1 != n) {
      for (int i = 0; i < values.length; i++) {
        if (null == values[i]) {
          continue;
        }
        
        long xi = (long) values[i];
        
        if (0 == xi) {
          continue;
        }
        
        double pi = ((double) xi) / sum;
        double logpi = Math.log(pi);
    
        H = H - pi * logpi;
      }
      
      //
      // Normalize
      //

      H = H / Math.log(n);
    } else {
      if (this.invert) {
        H = 1.0D;
      }
    }
    
    return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, H };
  }
}
