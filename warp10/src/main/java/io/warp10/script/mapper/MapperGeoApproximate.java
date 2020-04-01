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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.functions.GEOPACK;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;

/**
 * Mapper which approximates a location at the given resolution (even number from 2 to 32)
 */
public class MapperGeoApproximate extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects an even resolution between 2 (coarsest) and 32 (finest).");
      }
      
      int resolution = ((Number) o).intValue();
      
      stack.push(new MapperGeoApproximate(getName(), resolution));
      return stack;
    }
  }

  private final int resolution;
  
  /**
   * Default constructor
   */
  public MapperGeoApproximate(String name, int resolution) throws WarpScriptException {
    super(name);
    
    if (resolution < 2 || resolution > 32 || 0 != (resolution & 1)) {
      throw new WarpScriptException(getName() + " expects an even resolution between 2 (coarsest) and 32 (finest).");      
    }
    
    this.resolution = resolution;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    long location = locations[0];
    long elevation = elevations[0];
    
    //
    // If there is less than one value or if there is no location associated with the
    // value, return null as the tick value.
    //
   
    if (values.length < 1 || GeoTimeSerie.NO_LOCATION == location) {
      return new Object[] { tick, location, elevation, null };
    } else {
      location = GeoXPLib.centerGeoXPPoint(location, this.resolution);
      return new Object[] { tick, location, elevation, values[0] };
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.resolution);
    sb.append(" ");
    sb.append(this.getName());
    
    return sb.toString();
  }

}
