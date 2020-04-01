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

import java.util.ArrayList;
import java.util.List;

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
 * Splits a GEOSHAPE into connex shapes (corner contact does not count towards connexity)
 */
public class GEOSPLIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GEOSPLIT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();

    if (!(top instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " operates on a GEOSHAPE.");
    }
    
    GeoXPShape shape = (GeoXPShape) top;
    
    Coverage coverage = new Coverage(GeoXPLib.getCells(shape));
    
    List<Coverage> clusters = CoverageHelper.clusters(coverage);
    
    List<Object> shapes = new ArrayList<Object>();
    
    for (Coverage cluster: clusters) {
      shapes.add(GeoXPLib.fromCells(cluster.toGeoCells(HHCodeHelper.MAX_RESOLUTION), false));
    }
    
    stack.push(shapes);
    
    return stack;
  }
}
