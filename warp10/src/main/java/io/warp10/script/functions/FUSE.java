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

import java.util.List;

/**
 * Apply fuse on GTS instances
 * 
 */
public class FUSE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public FUSE(String name) {
    super(name);
  }
  
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of GTS as input.");
    }
    
    for (Object o: (List<Object>) top) {
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " expects a list of GTS as input.");
      }
    }
    
    stack.push(GTSHelper.fuse((List<GeoTimeSerie>) top));
    
    return stack;
  }
}
