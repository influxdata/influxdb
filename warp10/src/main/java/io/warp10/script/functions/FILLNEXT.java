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
 * Apply fillnext on GTS instances
 */
public class FILLNEXT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public FILLNEXT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof List) {
      List<Object> params = (List<Object>) top;

      List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

      for (int i = 0; i < params.size(); i++) {
        if (params.get(i) instanceof GeoTimeSerie) {
          series.add((GeoTimeSerie) params.get(i));
        } else if (params.get(i) instanceof List) {
          for (Object o: (List) params.get(i)) {
            if (!(o instanceof GeoTimeSerie)) {
              throw new WarpScriptException(getName() + " expects a LIST or a Geo Time Series™ as input.");
            }
            series.add((GeoTimeSerie) o);
          }
        }
      }

      List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();

      for (GeoTimeSerie gts: series) {
        result.add(GTSHelper.fillnext(gts));
      }
      stack.push(result);
    } else if (top instanceof GeoTimeSerie) {
      stack.push(GTSHelper.fillnext((GeoTimeSerie) top));
    } else {
      throw new WarpScriptException(getName() + " expects a LIST or a Geo Time Series™ as input.");
    }

    return stack;
  }
}
