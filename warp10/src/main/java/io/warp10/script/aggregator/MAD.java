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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import com.geoxp.GeoXPLib;

/**
 * Return the Median Absolute Deviation
 * @see https://en.wikipedia.org/wiki/Median_absolute_deviation
 * 
 * The returned location will be the median of all locations.
 * The returned elevation will be the median of all elevations.
 */
public class MAD extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  public MAD(String name) {
    super(name);
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    //
    // Sort locations, elevations
    //
    
    Arrays.sort(locations);
    Arrays.sort(elevations);
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    //
    // If start and end locations are identical, set median to that value
    //
    
    if (locations[0] == locations[locations.length - 1]) {
      location = locations[0];
    } else {
      // Remove NO_LOCATION
      int idx = Arrays.binarySearch(locations, GeoTimeSerie.NO_LOCATION);
      int len = locations.length;
      
      if (idx >= 0) {
        int i = idx + 1;
        while (i < locations.length && GeoTimeSerie.NO_LOCATION == locations[i]) {
          i++;
        }
        // Remove the NO_LOCATION values from the array
        if (i < locations.length) {
          System.arraycopy(locations, i, locations, idx, locations.length - i);
          len -= (i - idx);
        }
      }
      
      // Compute median of location
      
      if (0 == len % 2) {
        double[] high = GeoXPLib.fromGeoXPPoint(locations[len / 2]);
        double[] low = GeoXPLib.fromGeoXPPoint(locations[(len / 2) - 1]);
        location = GeoXPLib.toGeoXPPoint((high[0] + low[0])/2.0D, (high[1] + low[1])/2.0D);
      } else {
        location = locations[len / 2];
      }      
    }

    //
    // If start and end elevations are identical, set median to that value
    //
    
    if (elevations[0] == elevations[elevations.length - 1]) {
      elevation = elevations[0];
    } else {
      // Remove NO_elevation
      int idx = Arrays.binarySearch(elevations, GeoTimeSerie.NO_ELEVATION);
      int len = elevations.length;
      
      if (idx >= 0) {
        int i = idx + 1;
        while (i < elevations.length && GeoTimeSerie.NO_ELEVATION == elevations[i]) {
          i++;
        }
        // Remove the NO_elevation values from the array
        if (i < elevations.length) {
          System.arraycopy(elevations, i, elevations, idx, elevations.length - i);
          len -= (i - idx);
        }
      }
      
      // Compute median of elevation
      
      if (0 == len % 2) {
        elevation = (elevations[len / 2] + elevations[(len / 2) - 1]) / 2L;
      } else {
        elevation = elevations[len / 2];
      }      
    }

    //
    // Remove nulls
    //

    for (int i = 0; i < values.length; i++) {
      //fast if there is no null values (mapper or bucketizer use)
      if (null == values[i]) {
        //if one null is detected, rebuild an array without nulls and breaks.
        List nonnullvalues = new ArrayList<Object>(values.length - 1);
        for (int n = 0; n < values.length; n++) {
          if (null != values[n]) {
            nonnullvalues.add(values[n]);
          }
        }
        values = nonnullvalues.toArray();
        break;
      }
    }


    //
    // Sort nonnullvalues
    //
        
    Arrays.sort(values);

    //
    // If extrema are identical, use this as the median, otherwise remove nulls
    //
    
    Object median = null;
    
    if (0 != values.length) {
      if (values[0].equals(values[values.length - 1])) {
        median = values[0];
      } else {      
        int len = values.length;
        
        // Compute median
        if (0 == len % 2) {
          Object low = values[(len / 2) - 1];
          Object high = values[len / 2];
          
          if (low instanceof Long && high instanceof Long) {
            median = ((long) low + (long) high) / 2L;
          } else if (low instanceof Double && high instanceof Double) {
            median = ((double) low + (double) high) / 2.0D;
          } else {
            throw new WarpScriptException("Unable to compute median on an even number of non numeric values.");
          }
        } else {
          median = values[len / 2];
        }
      }      
    }
   
    //
    // Now compute the residuals
    //
    
    Object[] residuals = new Object[values.length];
    
    for (int i = 0; i < values.length; i++) {
      if (median instanceof Long) {
        residuals[i] = (long) Math.abs((long) median - (long) values[i]); 
      } else {
        residuals[i] = (double) Math.abs((double) median - (double) values[i]); 
      }
    }
    
    // Sort residuals
    
    Arrays.sort(residuals);
    
    // Compute median of residuals
    
    Object mad = null;
    
    if (0 != residuals.length) {
      if (residuals[0].equals(residuals[residuals.length - 1])) {
        mad = residuals[0];
      } else {
        int len = residuals.length;
        
        // Compute median
        if (0 == len % 2) {
          Object low = residuals[(len / 2) - 1];
          Object high = residuals[len / 2];
          
          if (low instanceof Long && high instanceof Long) {
            mad = ((long) low + (long) high) / 2L;
          } else if (low instanceof Double && high instanceof Double) {
            mad = ((double) low + (double) high) / 2.0D;
          } else {
            throw new WarpScriptException("Unable to compute median absolute deviation on an even number of non numeric values.");
          }
        } else {
          mad = residuals[len / 2];
        }
      }      
    }

    return new Object[] { tick, location, elevation, mad };
  }
}
