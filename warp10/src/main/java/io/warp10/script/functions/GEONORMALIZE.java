//
//   Copyright 2020  SenX S.A.S.
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
import com.geoxp.geo.HHCodeHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class GEONORMALIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GEONORMALIZE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " operates on two GEOSHAPE instances.");
    }
    
    GeoXPShape bshape = (GeoXPShape) top;
    
    top = stack.pop();
    
    if (!(top instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " operates on two GEOSHAPE instances.");
    }
    
    GeoXPShape ashape = (GeoXPShape) top;
    
    Coverage cova = new Coverage(GeoXPLib.getCells(ashape));
    Coverage covb = new Coverage(GeoXPLib.getCells(bshape));
    
    Coverage.normalize(cova, covb);
    
    stack.push(GeoXPLib.fromCells(cova.toGeoCells(HHCodeHelper.MAX_RESOLUTION), false));
    stack.push(GeoXPLib.fromCells(covb.toGeoCells(HHCodeHelper.MAX_RESOLUTION), false));
    
    return stack;
  }
}
