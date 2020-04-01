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

package io.warp10.script.op;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Checks that values from N time series are <= to each other (in the order they are passed). The elevation and location are cleared.
 */
public class OpLE extends NamedWarpScriptFunction implements WarpScriptNAryFunction {
  
  public OpLE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    String[] names = (String[]) args[1];
    Map<String,String>[] labels = (Map<String,String>[]) args[2];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    //
    // The type of result is determined by the first non null value
    //
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (null == values[0]) {
      return new Object[] { tick, location, elevation, false };        
    }

    for (int i = 1; i < values.length; i++) {
      if (null == values[i]) {
        return new Object[] { tick, location, elevation, false };        
      }
      if ((values[i - 1] instanceof Long) && (values[i] instanceof Long)) {
        if (((Number) values[i - 1]).longValue() > ((Number) values[i]).longValue()) {
          return new Object[] { tick, location, elevation, false };
        }
      } else if ((values[i - 1] instanceof Double) && (values[i] instanceof Double)) {
        if (((Number) values[i - 1]).doubleValue() > ((Number) values[i]).doubleValue()) {
          return new Object[] { tick, location, elevation, false };
        }
      } else if ((values[i - 1] instanceof String) && (values[i] instanceof String)) {
        if (((String) values[i - 1]).compareTo((String) values[i]) > 0) {
          return new Object[] { tick, location, elevation, false };
        }
      } else {
        throw new WarpScriptException(getName() + " can only be applied to Geo Time Series of the same type.");
      }
    }
    
    return new Object[] { tick, location, elevation, true };
  }
}
