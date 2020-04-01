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
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Compute the vertical distance traveled between ticks, in m
 * 
 * Returned location and elevation are those of the tick being computed.
 */
public class VDist extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  public VDist(String name) {
    super(name);
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    final long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    
    if (0 == ticks.length) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }

    //
    // The GTS is considered sorted. It is the case for MAP, REDUCE and BUCKETIZE.
    // Let's compute the total distance traveled
    //
    
    long lastelevation = GeoTimeSerie.NO_ELEVATION;
    
    int i = 0;
    
    long totaldistance = 0L;
    
    while(i < elevations.length) {
      // Skip measurements with no elevation
      if (GeoTimeSerie.NO_ELEVATION == elevations[i]) {
        i++;
        continue;
      }
      
      // This is the first valid elevation we encounter
      if (GeoTimeSerie.NO_ELEVATION == lastelevation) {
        lastelevation = elevations[i];
        i++;
        continue;
      }
      
      //
      // Compute the distance from last elevation
      // and add it to the total distance.
      //
      
      totaldistance += Math.abs(lastelevation - elevations[i]);
      
      //
      // Update last elevation
      //
      
      lastelevation = elevations[i];
      
      i++;
    }
    
    //
    // Determine location/elevation at 'tick' if known
    //
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    for (i = 0; i < ticks.length; i++) {
      if (tick == ticks[i]) {
        location = locations[i];
        elevation = elevations[i];
        break;
      }
    }

    return new Object[] { tick, location, elevation, totaldistance / ((double) Constants.ELEVATION_UNITS_PER_M)};
  }
}
