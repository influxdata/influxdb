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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

/**
 * Returns the rate of change (per second) between the first and last measures of the
 * interval.
 * Location and elevation returned are those of the latest measure.
 */
public class Rate extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  public Rate(String name) {
    super(name);
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

    int[] firstlast = GTSHelper.getFirstLastTicks(ticks);
    
    int firstidx = firstlast[0];
    int lastidx = firstlast[1];
    
    long firsttick = ticks[firstidx];
    long lasttick = ticks[lastidx];
    
    if (values[lastidx] instanceof Long) {
      long value = ((Number) values[lastidx]).longValue() - ((Number) values[firstidx]).longValue();
      
      if (0L == value || ticks[firstidx] == ticks[lastidx]) {
        // We need to cas to a double in case 0L is the first value returned, otherwise the GTS will be of type LONG
        return new Object[] { lasttick - firsttick, locations[lastidx], elevations[lastidx], (double) value };        
      } else {
        return new Object[] { lasttick - firsttick, locations[lastidx], elevations[lastidx], ((double) value) / ((ticks[lastidx] - ticks[firstidx]) / ((double) Constants.TIME_UNITS_PER_S))};
      }
    } else if (values[lastidx] instanceof Double) {
      double value = ((Number) values[lastidx]).doubleValue() - ((Number) values[firstidx]).doubleValue();
      if (0.0D == value || ticks[firstidx] == ticks[lastidx]) {
        return new Object[] { lasttick - firsttick, locations[lastidx], elevations[lastidx], value };        
      } else {
        return new Object[] { lasttick - firsttick, locations[lastidx], elevations[lastidx], value / ((ticks[lastidx] - ticks[firstidx]) / ((double) Constants.TIME_UNITS_PER_S))};
      }
    } else {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
  }
}
