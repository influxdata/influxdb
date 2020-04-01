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

/**
 * Returns the delta between the first and last measures of the
 * interval.
 * Location and elevation returned are those of the latest measure.
 */
public class Delta extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  public Delta(String name) {
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

    //
    // Determine first and last ticks
    //
    
    long firsttick = Long.MAX_VALUE;
    long lasttick = Long.MIN_VALUE;
    
    int firstidx = -1;
    int lastidx = -1;
    
    for (int i = 0; i < ticks.length; i++) {
      if (ticks[i] < firsttick) {
        firstidx = i;
        firsttick = ticks[i];
      }
      if (ticks[i] > lasttick) {
        lastidx = i;
        lasttick = ticks[i];
      }
    }
    
    if (values[lastidx] instanceof Long) {
      long value = ((Number) values[lastidx]).longValue() - ((Number) values[firstidx]).longValue();
      return new Object[] { lasttick - firsttick, locations[lastidx], elevations[lastidx], value };
    } else if (values[lastidx] instanceof Double) {
      double value = ((Number) values[lastidx]).doubleValue() - ((Number) values[firstidx]).doubleValue();
      return new Object[] { lasttick - firsttick, locations[lastidx], elevations[lastidx], value };      
    } else {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
  }
}
