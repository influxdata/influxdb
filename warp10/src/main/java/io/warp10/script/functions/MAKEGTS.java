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

import com.geoxp.GeoXPLib;

/**
 * Push onto the stack a GTS built from arrays
 */
public class MAKEGTS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public MAKEGTS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of values on top of the stack.");
    }
    
    List<Object> values = (List<Object>) o;
    
    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of elevations below the list of values.");
    }
    
    List<Object> elevations = (List<Object>) o;

    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of longitudes below the list of elevations.");
    }
    
    List<Object> longitudes = (List<Object>) o;

    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of latitudes below the list of longitudes.");
    }
    
    List<Object> latitudes = (List<Object>) o;

    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of ticks below the list of latitudes.");
    }
    
    List<Object> ticks = (List<Object>) o;

    int i = 0;
    
    int len = Math.max(values.size(), Math.max(elevations.size(), Math.max(longitudes.size(), Math.max(latitudes.size(), ticks.size()))));
    
    GeoTimeSerie gts = new GeoTimeSerie(len);

    long lasttick = -1;
    
    while (i < len) { 
      Object value = i < values.size() ? values.get(i) : values.get(values.size() - 1);
      long elevation = i < elevations.size() ? ((Number) elevations.get(i)).longValue() : GeoTimeSerie.NO_ELEVATION;
      
      long location = GeoTimeSerie.NO_LOCATION;
      
      if (i < latitudes.size() && i < longitudes.size()) {
        location = GeoXPLib.toGeoXPPoint(((Number) latitudes.get(i)).doubleValue(), ((Number) longitudes.get(i)).doubleValue());
      }
      
      long tick = ++lasttick;
      
      if (i < ticks.size()) {
        tick = ((Number) ticks.get(i)).longValue();
        lasttick = tick;
      }
      
      GTSHelper.setValue(gts, tick, location, elevation, value, false);
      i++;
    }
    
    // Set empty name
    gts.setName("");
    
    stack.push(gts);
    
    return stack;
  }
}
