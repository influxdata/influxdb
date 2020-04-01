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
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Wrap a Mapper so we return no value when the number of values in the
 * window is outside a given range
 */
public class STRICTMAPPER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public STRICTMAPPER(String name) {
    super(name);
  }


  private static final class StringentMapper extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {

    private final long min;
    private final long max;
    private final WarpScriptMapperFunction mapper;

    public StringentMapper(String name, long min, long max, WarpScriptMapperFunction mapper) {
      super(name);
      this.min = min;
      this.max = max;
      this.mapper = mapper;
    }

    @Override
    public Object apply(Object[] args) throws WarpScriptException {

      long[] ticks = (long[]) args[3];

      long timespan;
      if (0 == ticks.length) {
        timespan = 0;
      } else {
        timespan = ticks[ticks.length - 1] - ticks[0] + 1;
      }

      if (min > 0 && ticks.length < min || max > 0 && ticks.length > max || min < 0 && timespan < -min || max < 0 && timespan > -max) {
        return new Object[]{args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
      }

      return mapper.apply(args);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(mapper.toString());
      sb.append(" ");
      sb.append(min);
      sb.append(" ");
      sb.append(max);
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop(); // maxpoints or -maxspan

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a maximum (inclusive) number of values or a minimum timespan on top of the stack.");
    }


    long max = ((Number) o).longValue();

    o = stack.pop(); // minpoints or -minspan

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a minimum (inclusive) number of values or a minimum timespan below the top of the stack.");
    }

    long min = ((Number) o).longValue();

    // Safeguard over long overflow when negating
    if (Long.MIN_VALUE == min) {
      min++;
    }
    if (Long.MIN_VALUE == max) {
      max++;
    }

    if(min > 0 && max >= 0 || min < 0 && max <= 0) {
      if(Math.abs(min) > Math.abs(max)){
        throw new WarpScriptException(getName() + " expects abs(min) <= abs(max) when min and max both express a count or both express a duration");
      }
    }

    o = stack.pop(); // mapper

    if (!(o instanceof WarpScriptMapperFunction)) {
      throw new WarpScriptException(getName() + " expects a mapper below the extrema defining the value count range or timespan.");
    }

    WarpScriptMapperFunction mapper = (WarpScriptMapperFunction) o;

    stack.push(new StringentMapper(getName(), min, max, mapper));

    return stack;
  }
}
