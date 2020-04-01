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
 * Retain the measurement with the minimum value and timestamp
 */
public class Min extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  private final boolean ignoreNulls;
  
  public Min(String name, boolean ignoreNulls) {
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

    long tick = Long.MAX_VALUE;
    int idx = -1;
    Long minl = null;
    Double mind = null; 
    String mins = null;
    Boolean minb = null;
    
    boolean hasNulls = false;
    
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      
      if (null == value) {
        hasNulls = true;
        continue;
      }
      
      if (null != minl) {
        if (!(value instanceof Number)) {
          throw new WarpScriptException(getName() + " cannot determine min of numeric and non numeric values.");
        }
        if (minl.compareTo(((Number) value).longValue()) > 0 || (minl.compareTo(((Number) value).longValue()) == 0 && ticks[i] < tick)) {
          tick = ticks[i];
          minl = ((Number) value).longValue();
          idx = i;
        }
      } else if (null != mind) {
        if (!(value instanceof Number)) {
          throw new WarpScriptException(getName() + " cannot determine min of numeric and non numeric values.");
        }
        if (mind.compareTo(((Number) value).doubleValue()) > 0 || (mind.compareTo(((Number) value).doubleValue()) == 0 && ticks[i] < tick)) {
          tick = ticks[i];
          mind = ((Number) value).doubleValue();
          idx = i;
        }        
      } else if (null != mins) {
        if (mins.compareTo(value.toString()) > 0  || (mins.compareTo(value.toString()) == 0 && ticks[i] < tick)) {
          tick = ticks[i];
          mins = value.toString();
          idx = i;
        }
      } else if (null != minb) {
        if (Boolean.FALSE.equals(value)) {
          idx = i;
          tick = ticks[i];
          minb = Boolean.FALSE;
        }
      } else {
        // No type detected yet,
        // check value
        
        if (value instanceof Long) {          
          tick = ticks[i];
          minl = (Long) value;
          idx = i;
        } else if (value instanceof Double) {
          tick = ticks[i];
          mind = (Double) value;
          idx = i;
        } else if (value instanceof String) {
          tick = ticks[i];
          mins = (String) value;
          idx = i;
        } else if (value instanceof Boolean) {
          tick = ticks[i];
          minb = (Boolean) value;
          idx = i;
        }        
      }
    }

    Object value;
    
    if (hasNulls && !ignoreNulls) {
      value = null;
    } else {
      if (null != minl) {
        value = minl;
      } else if (null != mind) {
        value = mind;
      } else if (null != mins) {
        value = mins;
      } else if (null != minb) {
        value = minb;
      } else {
        value = null;
      }      
    }
    
    return new Object[] { ticks[idx], locations[idx], elevations[idx], value };
  }
}
