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
 * returns the root mean square of an interval
 * returns elevation and location from the latest measure.
 * Computation is done with doubles. If longs, result is casted to long at the end.
 */
public class RMS extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {

  private final boolean ignoreNulls;

  public RMS(String name, boolean ignoreNulls) {
    super(name);
    this.ignoreNulls = ignoreNulls;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == ticks.length) {
      return new Object[]{Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }

    TYPE sumType = TYPE.UNDEFINED;
    double sqsumd = 0.0D;
    double rms = 0.0D;
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    long timestamp = Long.MIN_VALUE;
    int nulls = 0;

    for (int i = 0; i < values.length; i++) {
      Object value = values[i];

      if (ticks[i] > timestamp) {
        location = locations[i];
        elevation = elevations[i];
        timestamp = ticks[i];
      }

      if (null == value) {
        nulls++;
        continue;
      } else {
        if (TYPE.UNDEFINED == sumType) {
          if (value instanceof Long) {
            sumType = TYPE.LONG;
          } else if (value instanceof Double) {
            sumType = TYPE.DOUBLE;
          } else {
            //
            // RMS of String or Boolean has no meaning
            //
            return new Object[]{Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
          }
        }
        //
        //Math.pow(x,2) optimized to x*x at runtime
        //
        sqsumd += Math.pow(((Number) value).doubleValue(), 2);
      }
    }

    Object meanvalue = null;

    //
    // If we should not ignore nulls and there were some nulls, return null
    //

    if (!ignoreNulls && nulls > 0) {
      return new Object[]{Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }

    // If all values were null, avoid divide by 0
    if (values.length == nulls) {
      return new Object[]{Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }

    rms = Math.sqrt(sqsumd / (values.length - nulls));

    if (TYPE.LONG == sumType) {
      meanvalue = ((long) rms);
    } else if (TYPE.DOUBLE == sumType) {
      meanvalue = rms;
    }

    return new Object[]{tick, location, elevation, meanvalue};
  }

}
