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
import java.util.List;

/**
 * Replace a list of GTS by a list of new GTS containing only the ticks which have
 * values in all input GTS
 */
public class COMMONTICKS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public COMMONTICKS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of GTS.");
    }
    
    // Check that list elements are GTS
    
    List<GeoTimeSerie> input = new ArrayList<GeoTimeSerie>();
    
    for (Object o: (List) top) {
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " operates on a list of GTS.");
      }
      input.add((GeoTimeSerie) o);
    }
    
    stack.push(GTSHelper.commonTicks(input));
    return stack;
  }
}
