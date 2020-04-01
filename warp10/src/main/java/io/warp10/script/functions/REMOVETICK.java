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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Remove a tick from a GTS
 * 
 */
public class REMOVETICK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public REMOVETICK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
    
    if (!(top instanceof Long) && !(top instanceof Collection)) {
      throw new WarpScriptException(getName() + " expects a tick (LONG) or a collection thereof on top of the stack.");
    }
    
    Set<Long> ticks = new HashSet<Long>();

    if (top instanceof Long) {
      long tick = ((Number) top).longValue();
      ticks.add(tick);
    } else {
      for (Object o: (Collection) top) {
        if (!(o instanceof Long)) {
          throw new WarpScriptException(getName() + " expects a tick (LONG) or a collection thereof on top of the stack.");
        }
        ticks.add(((Number) o).longValue());
      }
    }
    
    top = stack.pop();
    
    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on a Geo Time Series™.");
    }
    
    GeoTimeSerie gts = (GeoTimeSerie) top;
    
    int n = GTSHelper.nvalues(gts);
    GeoTimeSerie pruned = gts.cloneEmpty(n);
    
    for (int i = 0; i < n; i++) {
      long ts = GTSHelper.tickAtIndex(gts, i);
      
      if (ticks.contains(ts)) {
        continue;
      }
      
      GTSHelper.setValue(pruned, ts, GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
    }
    
    stack.push(pruned);
    
    return stack;
  }
}
