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
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.geoxp.GeoXPLib;

/**
 * Add values to the GTS for ticks in the given list for which it does not yet have values
 *
 * FILLTICKS expects the following parameters on the stack:
 * 1: a list of five parameters: lat, lon, elevation, value, ticks
 *    where ticks is a list of ticks for which to add the data point (tick, lat, lon, elevation and value) * 
 */
public class FILLTICKS extends GTSStackFunction {
  
  public FILLTICKS(String name) {
    super(name);
  }
  
  private static final String LOCATION = "location";
  private static final String ELEVATION = "elevation";
  private static final String VALUE = "value";
  private static final String TICKS = "ticks";

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of 5 parameters (lat, lon, elevation, value, ticks).");
    }

    List<Object> params = (List<Object>) top;
    
    if(5 != params.size()) {
      throw new WarpScriptException(getName() + " expects 5 parameters (lat, lon, elevation, value, ticks).");
    }
    double lat = (double) params.get(0);
    double lon = (double) params.get(1);

    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;

    if (!((Double) params.get(0)).isNaN() && !((Double) params.get(1)).isNaN()) {
      location = GeoXPLib.toGeoXPPoint(lat, lon);
    }
    
    if (params.get(2) instanceof Double && ((Double) params.get(2)).isNaN()) {
    } else {
      elevation = (long) params.get(2);
    }

    Object value = params.get(3);
    Object ticks = params.get(4);

    if (!(ticks instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of ticks as the fifth parameter of its parameter list.");
    }
    
    long[]  aticks = new long[((List) ticks).size()];
    
    int idx = 0;
    
    for (Object o: (List) ticks) {
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a list of ticks as the fifth parameter of its parameter list.");
      }
      aticks[idx++] = (long) o;
    }

    Map<String,Object> parameters = new HashMap<String, Object>();
    parameters.put(LOCATION, (long) location);
    parameters.put(ELEVATION, (long) elevation);
    parameters.put(VALUE, value);
    parameters.put(TICKS, (long[]) aticks);
    return parameters;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    long location = (long) params.get(LOCATION);
    long elevation = (long) params.get(ELEVATION);
    Object value = params.get(VALUE);
    long[] ticks = (long[]) params.get(TICKS);

    GeoTimeSerie result = GTSHelper.fillticks(gts, location, elevation, value, ticks);

    return result;
  }
}
