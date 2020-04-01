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

import com.geoxp.GeoXPLib;

/**
 * Fill ticks with no values
 */
public class FILLVALUE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public FILLVALUE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a LIST of 4 parameters: latitude longitude elevation value.");
    }

    List<Object> params = (List<Object>) top;

    if(4 != params.size()) {
      throw new WarpScriptException(getName() + " expects a LIST of 4 parameters: latitude longitude elevation value.");
    }

    double lat = (double) params.get(0);
    double lon = (double) params.get(1);

    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;

    if (!((Double) params.get(0)).isNaN() && !((Double) params.get(1)).isNaN()) {
      location = GeoXPLib.toGeoXPPoint(lat, lon);
    }

    if (!(params.get(2) instanceof Double && ((Double) params.get(2)).isNaN())) {
      elevation = (long) params.get(2);
    }

    Object value = params.get(3);

    top = stack.pop();

    if (top instanceof List) {
      List<Object> gtsList = (List<Object>) top;

      List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

      for (int i = 0; i < gtsList.size(); i++) {
        if (gtsList.get(i) instanceof GeoTimeSerie) {
          series.add((GeoTimeSerie) gtsList.get(i));
        } else if (gtsList.get(i) instanceof List) {
          for (Object o: (List) gtsList.get(i)) {
            if (!(o instanceof GeoTimeSerie)) {
              throw new WarpScriptException(getName() + " expects a LIST or a Geo Time Series™ as input.");
            }
            series.add((GeoTimeSerie) o);
          }
        }
      }

      List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();

      for (GeoTimeSerie gts: series) {
        result.add(GTSHelper.fillvalue(gts, location, elevation, value));
      }
      stack.push(result);
    } else if (top instanceof GeoTimeSerie) {
      stack.push(GTSHelper.fillvalue((GeoTimeSerie) top, location, elevation, value));
    } else {
      throw new WarpScriptException(getName() + " expects a LIST or a Geo Time Series™ as input.");
    }

    return stack;
  }
}
