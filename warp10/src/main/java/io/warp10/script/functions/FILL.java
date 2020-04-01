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

import java.util.List;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class FILL extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public FILL(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof WarpScriptFillerFunction)) {
      throw new WarpScriptException(getName() + " expects a FILLER on top of the stack.");
    }
    
    WarpScriptFillerFunction filler = (WarpScriptFillerFunction) top;
    
    top = stack.pop();
    
    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on two Geo Time Series™.");
    }
    
    GeoTimeSerie gtsb = (GeoTimeSerie) top;

    top = stack.pop();
    
    if (!(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on two Geo Time Series™.");
    }
    
    GeoTimeSerie gtsa = (GeoTimeSerie) top;

    List<GeoTimeSerie> gts = GTSHelper.fill(gtsa, gtsb, filler);
    
    stack.push(gts.get(0));
    stack.push(gts.get(1));
    
    return stack;
  }
}
