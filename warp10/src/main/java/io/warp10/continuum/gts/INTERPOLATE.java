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

package io.warp10.continuum.gts;

import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.geoxp.GeoXPLib;

/**
 * Fills the gaps in a GTS by interpolating linearly.
 */
public class INTERPOLATE extends GTSStackFunction {

  public INTERPOLATE(String name) {
    super(name);
  }

  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    return null;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    return interpolate(gts);
  }

  public static GeoTimeSerie interpolate(GeoTimeSerie gts) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized, do nothing
    //
        
    if (!GTSHelper.isBucketized(filled)) {
      return filled;
    }
    
    //
    // Sort GTS
    //
    
    GTSHelper.sort(filled);
    //filled = GTSHelper.dedup(filled);    
    
    //
    // If there is less than two values, we cannot interpolate, return the filled GTS now
    //
    
    if (filled.values < 2) {
      return filled;
    }
    
    //
    // Extract initial number of values
    //
    
    int nvalues = filled.values;
    
    //
    // Compute oldest bucket
    //
    
    long bucket = filled.lastbucket - filled.bucketcount * filled.bucketspan;
    
    int nElevations = 0;
    int nLocations = 0;
    
    if ((null != filled.elevations) && (GeoTimeSerie.NO_ELEVATION != filled.elevations[0])) {
      nElevations++;
    }

    if ((null != filled.locations) && (GeoTimeSerie.NO_LOCATION != filled.locations[0])) {
      nLocations++;
    }
    
    for (int i = 1; i < nvalues; i++) {
      if ((null != filled.elevations) && (GeoTimeSerie.NO_ELEVATION != filled.elevations[i])) {
        nElevations++;
      }
      if ((null != filled.locations) && (GeoTimeSerie.NO_LOCATION != filled.locations[i])) {
        nLocations++;
      }
      
      //
      // Move bucket passed the last tick encountered
      //
      while(bucket < filled.lastbucket && bucket <= filled.ticks[i-1]) {
        bucket += filled.bucketspan;
      }
      
      //
      // If bucket is on the current tick, advance tick
      //      
      if (bucket == filled.ticks[i]) {
        continue;
      }
      
      //
      // Determine rate of change for value
      //
      
      long tickDelta = GTSHelper.tickAtIndex(filled, i) - GTSHelper.tickAtIndex(filled, i - 1);
      
      double vDelta = ((Number) GTSHelper.valueAtIndex(filled, i)).doubleValue() - ((Number) GTSHelper.valueAtIndex(filled, i - 1)).doubleValue();
      double vRate = vDelta / tickDelta;
      
      //
      // Fill missing values until bucket passes the current tick
      //
      
      while(bucket < filled.ticks[i]) {
        long tDelta = bucket - GTSHelper.tickAtIndex(filled, i-1);
        GTSHelper.setValue(filled, bucket, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, ((Number) GTSHelper.valueAtIndex(filled, i-1)).doubleValue() + tDelta * vRate, false);
        bucket += filled.bucketspan;
      }
    }
    
    //
    // Take care of elevation if we have at least two valid values
    //
    
    if (nElevations >= 2) {
      bucket = filled.lastbucket - filled.bucketcount * filled.bucketspan;

      //
      // Sort ticks
      //
      
      GTSHelper.sort(filled);
      
      //
      // Advance 'idx' to the first tick with a valid elevation
      //
      
      int idx = 0;
      
      while (GeoTimeSerie.NO_ELEVATION == filled.elevations[idx]) {
        idx++;
      }
      
      while (idx < filled.values) {
        int i = idx + 1;
        
        // Advance 'i' to the next tick with no elevation
        while (i < filled.values && GeoTimeSerie.NO_ELEVATION != filled.elevations[i]) {
          i++;
        }
        
        // Move 'idx' to 'i' - 1, the last tick with an elevation before one without one
        idx = i - 1;
        
        // 'i' now points to a tick with no elevation, advance it to the next one with an elevation.
        while (i < filled.values && GeoTimeSerie.NO_ELEVATION == filled.elevations[i]) {
          i++;
        }
        
        // Fill all ticks between 'idx' and 'i' with an interpolated elevation
        if (i < filled.values) {
          double eRate = (filled.elevations[i] - filled.elevations[idx])/(filled.ticks[i] - filled.ticks[idx]);
          for (int j = idx + 1; j < i; j++) {
            filled.elevations[j] = (long) (filled.elevations[idx] + eRate * (filled.ticks[j] - filled.ticks[idx]));
          }
        }
      
        // Advance idx
        idx = i;
      }
    }
    
    //
    // Take care of location if we have at least two valid locations
    //
    
    if (nLocations >= 2) {
      bucket = filled.lastbucket - filled.bucketcount * filled.bucketspan;

      //
      // Sort ticks
      //
      
      GTSHelper.sort(filled);
      
      //
      // Advance 'idx' to the first tick with a valid location
      //
      
      int idx = 0;
      
      // nLocations > 0, this means locations is non null
      while (GeoTimeSerie.NO_LOCATION == filled.locations[idx]) {
        idx++;
      }
      
      while (idx < filled.values) {
        int i = idx + 1;
        
        // Advance 'i' to the next tick with no location
        while (i < filled.values && GeoTimeSerie.NO_LOCATION != filled.locations[i]) {
          i++;
        }
        
        // Move 'idx' to 'i' - 1, the last tick with a location before one without one
        idx = i - 1;
        
        // 'i' now points to a tick with no location, advance it to the next one with a location.
        while (i < filled.values && GeoTimeSerie.NO_LOCATION == filled.locations[i]) {
          i++;
        }
        
        // Fill all ticks between 'idx' and 'i' with an interpolated location
        if (i < filled.values) {
          double[] latlon_i = GeoXPLib.fromGeoXPPoint(filled.locations[i]);
          double[] latlon_idx = GeoXPLib.fromGeoXPPoint(filled.locations[idx]);
          
          double latRate = (latlon_i[0] - latlon_idx[0])/(filled.ticks[i] - filled.ticks[idx]);
          double lonRate = (latlon_i[1] - latlon_idx[1])/(filled.ticks[i] - filled.ticks[idx]);
          
          for (int j = idx + 1; j < i; j++) {
            double lat = latlon_idx[0] + latRate * (filled.ticks[j] - filled.ticks[idx]);
            double lon = latlon_idx[1] + lonRate * (filled.ticks[j] - filled.ticks[idx]);
            filled.locations[j] = GeoXPLib.toGeoXPPoint(lat, lon);
          }
        }
      
        // Advance idx
        idx = i;
      }      
    }
    
    return filled;
  }
}
