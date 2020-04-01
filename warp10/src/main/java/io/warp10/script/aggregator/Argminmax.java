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

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Map;

/**
 * For each tick return the tick and the concatenation of the values of the labels
 * for which the value is the maximum or minimum of Geo Time Series which are in the same equivalence class.
 * <p>
 * It operates on LONG and DOUBLE.
 * There is no location and elevation returned.
 * <p>
 * This reducer takes an additional LONG parameter to choose the maximum to report (use 0 to report them all),
 * and a String parameter to choose on which label it operates.
 */
public class Argminmax extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {

  /**
   * Label to report
   */
  private final String label;

  /**
   * Maximum number of maxima or minima to report
   */
  private final int count;

  /**
   * Is this method doing Argmin. If not, does Argmax.
   */
  private final boolean isArgmin;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    private final boolean isArgmin;

    public Builder(String name, boolean isArgmin) {
      super(name);
      this.isArgmin = isArgmin;
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();

      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects an integer number of maxima to report (use 0 to report them all).");
      }

      int count = ((Number) o).intValue();

      o = stack.pop();
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " expects the name of the label to report on the second level of the stack.");
      }

      String label = o.toString();

      stack.push(new Argminmax(getName(), label, count, isArgmin));
      return stack;
    }
  }

  public Argminmax(String name, String label, int count, boolean isArgmin) {
    super(name);
    this.label = label;
    this.count = count;
    this.isArgmin = isArgmin;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    Map<String, String>[] labels = (Map<String, String>[]) args[2];
    long[] ticks = (long[]) args[3];
    Object[] values = (Object[]) args[6];

    BitSet bitset = new BitSet(ticks.length);

    TYPE type = TYPE.LONG;

    long lmax = isArgmin ? Long.MAX_VALUE : Long.MIN_VALUE;
    double dmax = isArgmin ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;

    //
    // Iterate over all values
    // If we encounter a DOUBLE value, then type switches to DOUBLE, otherwise, stick with LONG
    //

    for (int i = 0; i < ticks.length; i++) {
      //
      // If there is no value for the ith element, continue to the next
      //

      if (null == values[i]) {
        continue;
      }

      //
      // If the labels for the ith element do not contain 'label', bail out
      //

      if (!labels[i].containsKey(this.label)) {
        throw new WarpScriptException(getName() + " expects all labels to contain label '" + this.label + "'.");
      }

      if (!(values[i] instanceof Number)) {
        throw new WarpScriptException(getName() + " can only operate on numerical values.");
      }

      //
      // Adapt the type of the max according to that of the current value,
      // DOUBLE rulez...
      //

      if (values[i] instanceof Long && TYPE.DOUBLE == type) {
        values[i] = ((Long) values[i]).doubleValue();
      } else if (values[i] instanceof Double && TYPE.LONG == type) {
        if (bitset.length() > 0) { // Only if a max has already been found
          dmax = (double) lmax;
        }
        type = TYPE.DOUBLE;
      }

      switch (type) {
        case LONG:
          if ((long) values[i] == lmax) {
            bitset.set(i);
          } else if (((long) values[i] > lmax) ^ isArgmin) { // XOR to inverse test in case of Argmin
            bitset.clear();
            lmax = (long) values[i];
            bitset.set(i);
          }
          break;
        case DOUBLE:
          if ((double) values[i] == dmax) {
            bitset.set(i);
          } else if (((double) values[i] > dmax) ^ isArgmin) { // XOR to inverse test in case of Argmin
            bitset.clear();
            dmax = (double) values[i];
            bitset.set(i);
          }
          break;
        default:
          throw new WarpScriptException(getName() + " encountered an incoherent case, call the coherency police!");
      }
    }

    //
    // Build result string of labels, URL encoding label values if they contain ','
    //

    StringBuilder sb = new StringBuilder();

    int rescount = 0;

    for (int i = 0; i < bitset.length(); i++) {
      if (bitset.get(i)) {
        rescount++;
        if (sb.length() > 0) {
          sb.append(",");
        }
        String lval = labels[i].get(this.label);
        try {
          sb.append(WarpURLEncoder.encode(lval, StandardCharsets.UTF_8));
        } catch (UnsupportedEncodingException uee) {
          // Can't happen, we're using UTF-8 which is one of the 6 standard encodings of the JVM
        }
        if (this.count > 0 && rescount == this.count) {
          break;
        }
      }
    }

    return new Object[]{tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, sb.toString()};
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.label));
    sb.append(" ");
    sb.append(StackUtils.toString(this.count));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }

}
