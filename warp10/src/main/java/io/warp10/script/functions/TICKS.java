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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extract the ticks of a GTS and push them onto the stack.
 * 
 * Only the ticks with actual values are returned
 */
public class TICKS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TICKS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie) && !(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series or a list thereof on top of the stack.");
    }

    Set<Long> ticks = new HashSet<Long>();
    
    if (o instanceof GeoTimeSerie) {
      int nvalues = GTSHelper.nvalues((GeoTimeSerie) o);
      
      for (int i = 0; i < nvalues; i++) {
        ticks.add(GTSHelper.tickAtIndex((GeoTimeSerie) o, i));
      }      
    } else {
      for (Object oo: (List) o) {
        if (!(oo instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a Geo Time Series or a list thereof on top of the stack.");
        }
        
        int nvalues = GTSHelper.nvalues((GeoTimeSerie) oo);
        
        for (int i = 0; i < nvalues; i++) {
          ticks.add(GTSHelper.tickAtIndex((GeoTimeSerie) oo, i));
        }
      }
    }
    
    List<Long> lticks = new ArrayList<Long>();
    lticks.addAll(ticks);
    
    Collections.sort(lticks);
    
    stack.push(lticks);
    
    return stack;
  }
}
