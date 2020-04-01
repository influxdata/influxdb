//
//   Copyright 2019  SenX S.A.S.
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

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;

import java.util.function.BiFunction;

/**
 * Keep the first element (tick, lat, lon, hhcode, elevation or value) which pass a comparison test (>, >=, ==, =<, < or !=) versus a given value.
 */
public class CompareTo extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptBucketizerFunction {

  private final BiFunction<Object[], Integer, Boolean> tester;

  private final Object reference;

  public enum Comparison {
    GT,
    GE,
    EQ,
    LE,
    LT,
    NE
  }

  public enum Compared {
    TICK,
    HHCODE,
    LAT,
    LON,
    ELEV,
    VALUE
  }

  public CompareTo(String name, Object threshold, Compared compared, Comparison comparison) throws WarpScriptException {
    super(name);

    this.reference = threshold;

    //
    // Check threshold / comparison compatibility
    //
    if (threshold instanceof Boolean && !(comparison == Comparison.EQ || comparison == Comparison.NE)) {
      throw new WarpScriptException(name + " cannot compare with a BOOLEAN.");
    }

    //
    // Check threshold / compared compatibility
    //
    if (Compared.TICK == compared || Compared.HHCODE == compared || Compared.ELEV == compared) {
      if (!(threshold instanceof Long)) {
        throw new WarpScriptException(name + " can only compare with a LONG." + comparison);
      }
    } else if (Compared.LAT == compared || Compared.LON == compared) {
      if (!(threshold instanceof Double || threshold instanceof Long)) {
        throw new WarpScriptException(name + " can only compare with a DOUBLE or a LONG." + comparison);
      }
    } else { // Compared.VALUE == compared
      if (!(threshold instanceof String || threshold instanceof Long || threshold instanceof Double || threshold instanceof Boolean)) {
        throw new WarpScriptException(name + " can only compare with a STRING, a DOUBLE, a LONG or a BOOLEAN." + comparison);
      }
    }

    this.tester = generateTester(compared, comparison);
  }

  private BiFunction<Object[], Integer, Boolean> generateTester(Compared compared, Comparison comparison) {
    final int compareEqualsTo;
    final boolean invertComparison;

    // Set the compareEqualsTo and invertComparison variables to simplify and optimize the parametrization of the comparator.
    switch (comparison) {
      case GT:
        compareEqualsTo = 1;
        invertComparison = false;
        break;
      case GE:
        compareEqualsTo = -1;
        invertComparison = true;
        break;
      case EQ:
        compareEqualsTo = 0;
        invertComparison = false;
        break;
      case LE:
        compareEqualsTo = 1;
        invertComparison = true;
        break;
      case LT:
        compareEqualsTo = -1;
        invertComparison = false;
        break;
      case NE:
        compareEqualsTo = 0;
        invertComparison = true;
        break;
      default:
        // Can't happen if Comparison enum is not modified
        throw new RuntimeException(getName() + " was given an invalid comparison value.");
    }

    // In all the comparisons below, we apply Interger.signum to make sure to result it -1, 0 or 1. Long, Double and Boolean
    // comparisons are already returning these values but it it NOT in the contract of the functions. That why we take precaution
    // and apply Interger.signum.

    switch (compared) {
      case TICK:
        return new BiFunction<Object[], Integer, Boolean>() {
          @Override
          public Boolean apply(Object[] args, Integer index) {
            return (Integer.signum(Long.compare(((long[]) args[3])[index], (long) reference)) == compareEqualsTo) ^ invertComparison;
          }
        };
      case HHCODE:
        return new BiFunction<Object[], Integer, Boolean>() {
          @Override
          public Boolean apply(Object[] args, Integer index) {
            return (Integer.signum(Long.compare(((long[]) args[4])[index], (long) reference)) == compareEqualsTo) ^ invertComparison;
          }
        };
      case ELEV:
        return new BiFunction<Object[], Integer, Boolean>() {
          @Override
          public Boolean apply(Object[] args, Integer index) {
            return (Integer.signum(Long.compare(((long[]) args[5])[index], (long) reference)) == compareEqualsTo) ^ invertComparison;
          }
        };
      case LAT:
        return new BiFunction<Object[], Integer, Boolean>() {
          @Override
          public Boolean apply(Object[] args, Integer index) {
            double[] latlon1 = GeoXPLib.fromGeoXPPoint(((long[]) args[4])[index]);
            return (Integer.signum(Double.compare(latlon1[0], ((Number) reference).doubleValue())) == compareEqualsTo) ^ invertComparison;
          }
        };
      case LON:
        return new BiFunction<Object[], Integer, Boolean>() {
          @Override
          public Boolean apply(Object[] args, Integer index) {
            double[] latlon1 = GeoXPLib.fromGeoXPPoint(((long[]) args[4])[index]);
            return (Integer.signum(Double.compare(latlon1[1], ((Number) reference).doubleValue())) == compareEqualsTo) ^ invertComparison;
          }
        };
      case VALUE:
        if (reference instanceof Long) {
          return new BiFunction<Object[], Integer, Boolean>() {
            @Override
            public Boolean apply(Object[] args, Integer index) {
              Object value = ((Object[]) args[6])[index];
              if (value instanceof Number) {
                return (Integer.signum(Long.compare(((Number) value).longValue(), (long) reference)) == compareEqualsTo) ^ invertComparison;
              } else {
                return false;
              }
            }
          };
        } else if (reference instanceof Double) {
          return new BiFunction<Object[], Integer, Boolean>() {
            @Override
            public Boolean apply(Object[] args, Integer index) {
              Object value = ((Object[]) args[6])[index];
              if (value instanceof Number) {
                return (Integer.signum(Double.compare(((Number) value).doubleValue(), (double) reference)) == compareEqualsTo) ^ invertComparison;
              } else {
                return false;
              }
            }
          };
        } else if (reference instanceof String) {
          return new BiFunction<Object[], Integer, Boolean>() {
            @Override
            public Boolean apply(Object[] args, Integer index) {
              Object value = ((Object[]) args[6])[index];
              return (Integer.signum(value.toString().compareTo(((String) reference))) == compareEqualsTo) ^ invertComparison;
            }
          };
        } else if (reference instanceof Boolean) {
          return new BiFunction<Object[], Integer, Boolean>() {
            @Override
            public Boolean apply(Object[] args, Integer index) {
              Object value = ((Object[]) args[6])[index];
              if (value instanceof Boolean) {
                return (Integer.signum(Boolean.compare((boolean) value, (boolean) reference)) == compareEqualsTo) ^ invertComparison;
              } else {
                return false;
              }
            }
          };
        } else {
          // Can't happen because it was tested in the constructor that threshold is a Long, Double, String or Boolean.
          throw new RuntimeException(getName() + " was given a threshold of invalid type.");
        }
      default:
        // Can't happen if Compared enum is not modified
        throw new RuntimeException(getName() + " was given an invalid compared value.");
    }
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = Long.MAX_VALUE;

    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    int idx = -1;

    // Ticks are not considered to be sorted.
    for (int i = 0; i < values.length; i++) {
      // skip ticks older than the one we already identified
      if (-1 != idx && ticks[i] >= tick) {
        continue;
      }

      if (tester.apply(args, i)) {
        idx = i;
        tick = ticks[i];
      }
    }

    if (-1 != idx) {
      return new Object[]{ticks[idx], locations[idx], elevations[idx], values[idx]};
    } else {
      return new Object[]{args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null};
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(reference));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
