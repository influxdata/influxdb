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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Mapper which returns the hour of the tick for which it
 * is computed.
 */
public class MapperHourOfDay extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object timezone = stack.pop();
      stack.push(new MapperHourOfDay(getName(), timezone));
      return stack;
    }
  }

  private final DateTimeZone dtz;
  
  /**
   * Default constructor, the timezone will be UTC
   */
  public MapperHourOfDay(String name) {
    super(name);
    this.dtz = DateTimeZone.UTC; 
  }
  
  public MapperHourOfDay(String name, Object timezone) {
    super(name);
    if (timezone instanceof String) {
      this.dtz = DateTimeZone.forID(timezone.toString());
    } else if (timezone instanceof Number) {
      this.dtz = DateTimeZone.forOffsetMillis(((Number) timezone).intValue());
    } else {
      this.dtz = DateTimeZone.UTC;
    }
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];

    long location = locations[0];
    long elevation = elevations[0];

    DateTime dt = new DateTime(tick / Constants.TIME_UNITS_PER_MS, this.dtz);
        
    return new Object[] { tick, location, elevation, dt.getHourOfDay() };
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.dtz.getID()));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
