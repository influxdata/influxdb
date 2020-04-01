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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.List;

/**
 * Generate a quantified version of GTS
 * <p>
 * QUANTIZE expects the following parameters on the stack:
 * <p>
 * 2: bounds a list of N bounds defining N + 1 intervals for quantification
 * 1: values a list of N+1 values or an empty list
 */
public class QUANTIZE extends ElementOrListStackFunction {

  public QUANTIZE(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of target values on top of the stack.");
    }

    List<Object> rankToValue = (List) top;

    top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of bounds under the top of the stack.");
    }

    // Create array to copy bounds into.
    final double[] bounds = new double[((List) top).size()];

    //
    // Check that we have enough values according to the number of bounds
    //

    if (!rankToValue.isEmpty() && (rankToValue.size() != bounds.length + 1)) {
      throw new WarpScriptException(getName() + " expected " + (bounds.length + 1) + " values but got " + rankToValue.size());
    }

    //
    // Put bounds into an array and make sure bounds are sorted and finite
    //

    for (int i = 0; i < bounds.length; i++) {
      bounds[i] = ((Number) ((List) top).get(i)).doubleValue();
      if (!Double.isFinite(bounds[i])) {
        throw new WarpScriptException(getName() + " expects the bounds to be finite.");
      } else if (i > 0 && bounds[i] <= bounds[i - 1]) {
        throw new WarpScriptException(getName() + " identified unordered or duplicate bounds.");
      }
    }

    final Object[] rankToValueArray;

    if (rankToValue.isEmpty()) {
      // The the array to null for GTSHelper.quantize to fall back to rank
      rankToValueArray = null;
    } else {
      rankToValueArray = rankToValue.toArray();
    }

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) element;
          return GTSHelper.quantize(gts, bounds, rankToValueArray);
        } else {
          throw new WarpScriptException(getName() + " expects a Geo Time Series instance or a list thereof.");
        }
      }
    };
  }
}
