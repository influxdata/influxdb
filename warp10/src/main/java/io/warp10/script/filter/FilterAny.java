//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.script.filter;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterAny extends NamedWarpScriptFunction implements WarpScriptFilterFunction {

  public enum Comparator {
    EQ,
    GE,
    GT,
    LE,
    LT,
    NE
  }

  private final TYPE type;
  private final Object threshold;
  private final boolean complementSet; // if true, the filter does the opposite (so filter.all.* can be built using the same builder)
  private final Comparator comparator;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    private final boolean complementSet;
    private final Comparator comparator;

    public Builder(String name, Comparator comparator, boolean complement) {
      super(name);
      this.comparator =comparator;
      this.complementSet = complement;
    }

    public Builder(String name, Comparator comparator) {
      super(name);
      this.comparator = comparator;
      this.complementSet = false;
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object threshold = stack.pop();
      stack.push(new FilterAny(getName(), threshold, this.comparator, this.complementSet));
      return stack;
    }
  }

  public FilterAny(String name, Object threshold, Comparator comparator, boolean complementSet) throws WarpScriptException {
    super(name);
    this.comparator = comparator;
    this.complementSet = complementSet;

    boolean allowBooleanThreshold = comparator == Comparator.EQ || comparator == Comparator.NE;

    if (threshold instanceof Long) {
      this.type = TYPE.LONG;
      this.threshold = threshold;
    } else if (threshold instanceof Double) {
      this.type = TYPE.DOUBLE;
      this.threshold = threshold;
    } else if (threshold instanceof String) {
      this.type = TYPE.STRING;
      this.threshold = threshold;
    } else if (allowBooleanThreshold && threshold instanceof Boolean) {
      this.type = TYPE.BOOLEAN;
      this.threshold = threshold;
    } else {
      throw new WarpScriptException(getName() + " threshold type is invalid.");
    }
  }

  private boolean verify(int i) throws WarpScriptException {
    switch (this.comparator) {
      case EQ:
        return i == 0;
      case GE:
        return i <= 0;
      case GT:
        return i < 0;
      case LE:
        return i >= 0;
      case LT:
        return i > 0;
      case NE:
        return i != 0;
      default:
        throw new WarpScriptException(getName() + " has been implemented with an unknown comparator.");
    }
  }

  @Override
  public List<GeoTimeSerie> filter(Map<String,String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {
    List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();

    for (List<GeoTimeSerie> gtsinstances: series) {
      for (GeoTimeSerie serie: gtsinstances) {
        boolean found = false;
        int i = 0;

        while(!found && i < serie.size()) {
          Object val = GTSHelper.valueAtIndex(serie, i++);

          switch (type) {
            case LONG:
              found = verify(((Long) threshold).compareTo(((Number) val).longValue()));
              break;
            case DOUBLE:
              found = verify(((Double) threshold).compareTo(((Number) val).doubleValue()));
              break;
            case STRING:
              found = verify(((String) threshold).compareTo(val.toString()));
              break;
            case BOOLEAN:
              found = ((Boolean) threshold).equals(val) ^ this.comparator == Comparator.NE;
              break;
          }
        }

        //
        // The gts is retained if we found any point that satisfies the comparison.
        // OTOH that would also mean that not all points satisfy the opposite comparison.
        // So if we want to implement a filter that retains a gts if all its points satisfy the opposite comparison,
        // we can just retain the complement set of what we retain here.
        //
        // In case of wanting the complementSet, the logic is reversed.
        // A ^ false = A
        // A ^ true = ~A
        //
        
        if (found ^ this.complementSet) {
          retained.add(serie);
        }
      }
    }

    return retained;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.threshold));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
