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

import java.util.Map;

/**
 * Add values from multiple time series. The elevation and location are cleared.
 */
public class OpAdd extends NamedWarpScriptFunction implements WarpScriptNAryFunction {
  
  private final boolean forbidNulls;
  
  public OpAdd(String name, boolean forbidNulls) {
    super(name);
    this.forbidNulls = forbidNulls;
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
    
    Object sum = null;
        
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    for (int i = 0; i < values.length; i++) {      
      // If one of the values is 'null' (absent), return null as the value
      if (null == values[i]) {
        if (this.forbidNulls) {
          return new Object[] { tick, location, elevation, null };
        } else {
          // Simply ignore nulls
          continue;
        }
      }      
      if (null == sum) {
        sum = values[i];        
      } else {
        if (sum instanceof Long) {
          if (values[i] instanceof Long) {
            sum = ((Long) sum) + ((Number) values[i]).longValue();
          } else if (values[i] instanceof Double) {
            sum = ((Double) sum) + ((Number) values[i]).doubleValue();
          } else if (values[i] instanceof Boolean) {            
            sum = ((Long) sum) + (Boolean.TRUE.equals(values[i]) ? 1L : 0L);
          } else if (values[i] instanceof String) {
            throw new WarpScriptException("Cannot add strings and longs.");
          }
        } else if (sum instanceof Double) {
          if (values[i] instanceof Long) {
            sum = ((Double) sum) + ((Number) values[i]).doubleValue();
          } else if (values[i] instanceof Double) {
            sum = ((Double) sum) + ((Number) values[i]).doubleValue();
          } else if (values[i] instanceof Boolean) {            
            sum = ((Double) sum) + (Boolean.TRUE.equals(values[i]) ? 1.0D : 0.0D);
          } else if (values[i] instanceof String) {            
            throw new WarpScriptException("Cannot add strings and doubles.");
          }
        } else if (sum instanceof Boolean) {
          if (values[i] instanceof Long) {
            sum = (Boolean.TRUE.equals(sum) ? 1L : 0L) + (long) values[i];
          } else if (values[i] instanceof Double) {
            sum = (Boolean.TRUE.equals(sum) ? 1.0D : 0.0D) + (double) values[i];
          } else if (values[i] instanceof Boolean) {            
            sum = (Boolean.TRUE.equals(sum) ? 1L : 0L) + (Boolean.TRUE.equals(values[i]) ? 1L : 0L);
          } else if (values[i] instanceof String) {
            throw new WarpScriptException("Cannot add strings and booleans.");
          }
        } else if (sum instanceof String) {
          sum = (String) sum + values[i].toString();
        }
      }
    }
    
    return new Object[] { tick, location, elevation, sum };
  }
}
