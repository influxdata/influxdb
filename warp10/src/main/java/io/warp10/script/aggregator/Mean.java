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

import com.geoxp.GeoXPLib;

/**
 * Return the mean of the values on the interval.
 * The returned location will be the centroid of all locations.
 * The returned elevation will be the average of all elevations.
 */
public class Mean extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  private final boolean ignoreNulls;
  
  public Mean(String name, boolean ignoreNulls) {
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
    
    TYPE sumType = TYPE.UNDEFINED;
    long suml = 0L;
    double sumd = 0.0D;
    long ticksum = 0L;
    long latitudes = 0L;
    long longitudes = 0L;
    int locationcount = 0;
    long elev = 0L;
    int elevationcount = 0;

    int nulls = 0;
    
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];

      if (null == value) {
        nulls++;
        continue;
        //return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
      } else {

        ticksum += ticks[i] - ticks[0];

        if (GeoTimeSerie.NO_LOCATION != locations[i]) {
          long[] xy = GeoXPLib.xyFromGeoXPPoint(locations[i]);
          latitudes += xy[0];
          longitudes += xy[1];
          locationcount++;
        }

        if (GeoTimeSerie.NO_ELEVATION != elevations[i]) {
          elev += elevations[i];
          elevationcount++;
        }

        if (TYPE.LONG == sumType) {
          suml = suml + ((Number) value).longValue();
        } else if (TYPE.DOUBLE == sumType) {
          sumd = sumd + ((Number) value).doubleValue();
        } else {
          // No type detected yet,
          // check value

          if (value instanceof Long) {
            sumType = TYPE.LONG;
            suml = ((Number) value).longValue();
          } else if (value instanceof Double) {
            sumType = TYPE.DOUBLE;
            sumd = ((Number) value).doubleValue();
          } else {
            //
            // Mean of String or Boolean has no meaning
            //
            return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
          }
        }
      }
    }
    
    long meanlocation = GeoTimeSerie.NO_LOCATION;
    long meanelevation = GeoTimeSerie.NO_ELEVATION;
    
    if (locationcount > 0) {
      latitudes = latitudes / locationcount;
      longitudes = longitudes / locationcount;
      meanlocation = GeoXPLib.toGeoXPPoint(latitudes, longitudes);
    }
    
    if (elevationcount > 0) {
      meanelevation = elev / elevationcount;
    }
    
    Object meanvalue = null;
    
    //
    // If we should not ignore nulls and there were some nulls, return null
    //
    
    if (!ignoreNulls && nulls > 0) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    if (TYPE.LONG == sumType) {
      meanvalue = suml / (double) (values.length - nulls);
    } else if (TYPE.DOUBLE == sumType) {
      meanvalue = sumd / (values.length - nulls);
    }
    
    return new Object[] { ticks[0] + (ticksum / ticks.length), meanlocation, meanelevation, meanvalue };
  }
}
