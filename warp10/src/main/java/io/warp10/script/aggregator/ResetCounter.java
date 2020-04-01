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
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptException;

/**
 * Count the number of times a GTS resets, eg:
 * 
 * 1 2 3 1 4 5 2 8 9 4
 *      ^     ^     ^
 *      
 * has 3 resets
 *
 */
public class ResetCounter extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction {
  
  /**
   * Flag indicating if a reset is a higher value instead of a lower one.
   */
  private final boolean resetHigher;
  
  /**
   * Create a reset counter, forcing the type of resets.
   * 
   * @param resetHigher If true, a value higher than the previous one will count as a reset.
   *                    If false, a value lower than the previous one will count as a reset.
   */
  public ResetCounter(String name, boolean resetHigher) {
    super(name);
    this.resetHigher = resetHigher;
  }
  
  /**
   * Create a reset counter which considers a lower value than the previous one to
   * be a reset.
   */
  public ResetCounter(String name) {
    super(name);
    this.resetHigher = false;
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
    // Sort ticks in ascending order
    //
    
    int[] indices = GTSHelper.sortIndices(ticks, false);
    
    //
    // Determine the type of values
    //
    
    TYPE type = GTSHelper.getValueType(values[0]);
    
    int resets = 0;
    
    Object lastvalue = values[0];

    for (int i = 1; i < values.length; i++) {
      if (this.resetHigher) {
        switch(type) {
          case LONG:
            if (((Number) values[indices[i]]).longValue() > (long) lastvalue) {
              resets++;
            }
            break;
          case DOUBLE:
            if (((Number) values[indices[i]]).doubleValue() > (double) lastvalue) {
              resets++;
            }
            break;
          case STRING:
            if (((String) values[indices[i]]).compareTo((String) lastvalue) > 0) {
              resets++;
            }
            break;
          case BOOLEAN:
            if (Boolean.FALSE.equals(lastvalue) && Boolean.TRUE.equals(values[indices[i]])) {
              resets++;
            }
            break;
        }
      } else {
        switch(type) {
          case LONG:
            if (((Number) values[indices[i]]).longValue() < (long) lastvalue) {
              resets++;
            }
            break;
          case DOUBLE:
            if (((Number) values[indices[i]]).doubleValue() < (double) lastvalue) {
              resets++;
            }
            break;
          case STRING:
            if (((String) values[indices[i]]).compareTo((String) lastvalue) < 0) {
              resets++;
            }
            break;
          case BOOLEAN:
            if (Boolean.TRUE.equals(lastvalue) && Boolean.FALSE.equals(values[indices[i]])) {
              resets++;
            }
            break;
        }        
      }
      lastvalue = values[indices[i]];
    }
    
    return new Object[] { ticks[indices[indices.length - 1]], locations[indices[indices.length - 1]], elevations[indices[indices.length - 1]], resets };
  }

}
