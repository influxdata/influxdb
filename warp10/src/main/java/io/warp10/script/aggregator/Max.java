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
 * Retain the measurement with the highest value and timestamp
 */
public class Max extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  private final boolean ignoreNulls;
  
  public Max(String name, boolean ignoreNulls) {
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
    Long maxl = null;
    Double maxd = null; 
    String maxs = null;
    Boolean maxb = null;
    
    boolean hasNulls = false;
    
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
    
      if (null == value) {
        hasNulls = true;
        continue;
      }
      
      if (null != maxl) {
        if (!(value instanceof Number)) {
          throw new WarpScriptException(getName() + " cannot compare non numeric values to numeric ones.");
        }
        if (maxl.compareTo(((Number) value).longValue()) < 0 || (maxl.compareTo(((Number) value).longValue()) == 0 && ticks[i] < tick)) {
          tick = ticks[i];
          maxl = ((Number) value).longValue();
          idx = i;
        }
      } else if (null != maxd) {
        if (!(value instanceof Number)) {
          throw new WarpScriptException(getName() + " cannot compare non numeric values to numeric ones.");
        }
        if (maxd.compareTo(((Number) value).doubleValue()) < 0 || (maxd.compareTo(((Number) value).doubleValue()) == 0 && ticks[i] < tick)) {
          tick = ticks[i];
          maxd = ((Number) value).doubleValue();
          idx = i;
        }        
      } else if (null != maxs) {
        if (maxs.compareTo(value.toString()) < 0  || (maxs.compareTo(value.toString()) == 0 && ticks[i] < tick)) {
          tick = ticks[i];
          maxs = value.toString();
          idx = i;
        }
      } else if (null != maxb) {
        if (Boolean.TRUE.equals(value)) {
          idx = i;
          tick = ticks[i];
          maxb = Boolean.TRUE;
        }
      } else {
        // No type detected yet,
        // check value
        
        if (value instanceof Long) {          
          tick = ticks[i];
          maxl = (Long) value;
          idx = i;
        } else if (value instanceof Double) {
          tick = ticks[i];
          maxd = (Double) value;
          idx = i;
        } else if (value instanceof String) {
          tick = ticks[i];
          maxs = (String) value;
          idx = i;
        } else if (value instanceof Boolean) {
          tick = ticks[i];
          maxb = (Boolean) value;
          idx = i;
        }        
      }
    }

    Object value;
    
    if (hasNulls && !this.ignoreNulls) {
      value = null;
    } else {      
      if (null != maxl) {
        value = maxl;
      } else if (null != maxd) {
        value = maxd;
      } else if (null != maxs) {
        value = maxs;
      } else if (null != maxb) {
        value = maxb;
      } else {
        value = null;
      }
    }
    
    return new Object[] { ticks[idx], locations[idx], elevations[idx], value };
  }
}
