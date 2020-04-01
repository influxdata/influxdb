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

package io.warp10.script.aggregator;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

/**
 * Return the sum of measures with elevation and location from
 * the latest measure.
 */
public class Sum extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  private final boolean ignoreNulls;
  
  public Sum(String name, boolean ignoreNulls) {
    super(name);
    this.ignoreNulls = ignoreNulls;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    if (0 == ticks.length) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    long suml = 0L;
    double sumd = 0.0D;
    
    TYPE sumType = TYPE.UNDEFINED;
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    long timestamp = Long.MIN_VALUE;
    
    boolean hasNulls = false;
    
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
    
      if (ticks[i] > timestamp) {
        location = locations[i];
        elevation = elevations[i];
        timestamp = ticks[i];
      }
    
      if (null == value) {
        hasNulls = true;
        continue;
      }
      
      if (TYPE.LONG == sumType) {
        suml = suml + ((Number) value).longValue();
      } else if (TYPE.DOUBLE == sumType) {
        sumd = sumd + ((Number) value).doubleValue();
      } else {
        // No type detected yet,
        // check value
        
        if (value instanceof Long) {
          suml = ((Number) value).longValue();
          sumType = TYPE.LONG;
        } else if (value instanceof Double) {
          sumd = ((Number) value).doubleValue();
          sumType = TYPE.DOUBLE;
        } else {
          //
          // Sum of String or Boolean has no meaning
          //
          return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
        }
      }
    }
    
    Object sum = null;
    
    if (TYPE.LONG == sumType) {
      sum = suml;
    } else if (TYPE.DOUBLE == sumType) {
      sum = sumd;
    }
    
    if (hasNulls && !this.ignoreNulls) {
      sum = null;
    }
    
    return new Object[] { 0L, location, elevation, sum };    
  }
}
