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
 * Mapper which returns a value for a tick if the value has a location
 * contained in the given Geo Shape.
 */
public class MapperGeoWithin extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object shape = stack.pop();
      
      if (!(shape instanceof GeoXPShape)) {
        throw new WarpScriptException(getName() + " expects a Geo Shape on top of the stack.");
      }
      
      stack.push(new MapperGeoWithin(getName(), (GeoXPShape) shape));
      return stack;
    }
  }

  private final GeoXPShape shape;
  
  /**
   * Default constructor, the timezone will be UTC
   */
  public MapperGeoWithin(String name, GeoXPShape shape) {
    super(name);
    this.shape = shape;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (values.length < 1) {
      return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    long location = locations[0];
    long elevation = elevations[0];
    
    //
    // If there is less than one value or if there is no location associated with the
    // value, return null as the tick value.
    //
   
    if (null == values[0] || GeoTimeSerie.NO_LOCATION == location || !GeoXPLib.isGeoXPPointInGeoXPShape(location, this.shape)) {
      return new Object[] { tick, location, elevation, null };
    } else {
      return new Object[] { tick, location, elevation, values[0] };
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append(StackUtils.toString(GEOPACK.pack(this.shape)));
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
    sb.append(" ");
    sb.append(WarpScriptLib.GEOUNPACK);
    sb.append(" ");
    sb.append(this.getName());
    
    return sb.toString();
  }
}
