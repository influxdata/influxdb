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

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.geoxp.geo.Coverage;
import com.geoxp.geo.CoverageHelper;
import com.geoxp.geo.HHCodeHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Modify a GeoXPShape so it has cells of at most a threshold resolution
 * 
 */
public class GEOOPTIMIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GEOOPTIMIZE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a maximum resolution on top of the stack.");
    }
    
    int maxres = ((Number) o).intValue();
    
    if (0 != maxres && (maxres < 2 || maxres > 30 || (0 != (maxres & 1)))) {
      throw new WarpScriptException(getName() + " expects a maximum resolution which is an even number between 2 and 30 or 0.");
    }
    
    o = stack.pop();
    
    if (!(o instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " operates on a shape.");
    }
    
    GeoXPShape shape = (GeoXPShape) o;
    
    if (0 != maxres) {
      shape = GeoXPLib.limitResolution(shape, maxres, HHCodeHelper.MAX_RESOLUTION);
    } else {
      Coverage c = CoverageHelper.fromGeoCells(GeoXPLib.getCells(shape));
      long thresholds = 0x0L;
      c.optimize(thresholds);
      shape = GeoXPLib.fromCells(c.toGeoCells(30), false);
    }
    
    stack.push(shape);
    
    return stack;
  }
}
