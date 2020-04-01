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
 * Replace multiple contiguous identical value/location/elevation by a single one
 */
public class COMPACT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public COMPACT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (top instanceof GeoTimeSerie) {
      stack.push(GTSHelper.compact((GeoTimeSerie) top, false));
    } else if (top instanceof List) {
      List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
      
      for (Object o: (List<Object>) top) {
        if (! (o instanceof GeoTimeSerie)) {
          stack.push(top);
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
        }
        series.add(GTSHelper.compact((GeoTimeSerie) o, false));
      }
      stack.push(series);
    } else {
      stack.push(top);
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
    }
    
    return stack;
  }
}
