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

/**
 * Shrinks the number of values of a GTS.
 * 
 * This function has the side effect of sorting the GTS.
 */
public class SHRINK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SHRINK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {        
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a size on top of the stack.");
    }

    long shrinkto = (long) top;
    
    top = stack.pop();
    
    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on a Geo Time Series.");
    }
    
    GeoTimeSerie gts = (GeoTimeSerie) top;
    
    if (GTSHelper.nvalues(gts) < Math.abs(shrinkto)) {
      throw new WarpScriptException(getName() + " cannot shrink a GTS to a size larger than its actual size!");
    }
    
    if (GTSHelper.nvalues(gts) > Math.abs(shrinkto)) {
      if (shrinkto < 0) {
        GTSHelper.sort(gts, true);
      } else {
        GTSHelper.sort(gts, false);
      }
      
      GTSHelper.shrinkTo(gts, (int) Math.abs(shrinkto));
    }

    stack.push(gts);
    
    return stack;
  }
}
