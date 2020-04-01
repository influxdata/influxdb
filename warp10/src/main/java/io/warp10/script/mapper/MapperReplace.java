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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.DateTimeZone;

/**
 * Replaces windows with at least one value with a constant.
 */
public class MapperReplace extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {
  
  public final Object value;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();
      stack.push(new MapperReplace(getName(), value));
      return stack;
    }
  }

  private final DateTimeZone dtz;
  
  /**
   * 
   */
  public MapperReplace(String name, Object value) throws WarpScriptException {
    super(name);
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte
        || value instanceof BigInteger) {
      this.value = ((Number) value).longValue();
    } else if (value instanceof Double
               || value instanceof Float
               || value instanceof BigDecimal) {
      this.value = ((Number) value).doubleValue();
    } else if (value instanceof Boolean) {
      this.value = value;
    } else if (value instanceof String) {
      this.value = value;
    } else {
      throw new WarpScriptException("Invalid value type.");
    }
    
    
    this.dtz = DateTimeZone.UTC; 
  }
    
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    if (0 == values.length) {
      return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };      
    } else {
      return new Object[] { tick, locations[0], elevations[0], this.value };
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(value));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
