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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Clip Geo Time Series, GTSEncoder or a list thereof according to a series of limits
 */
public class CLIP extends ElementOrListStackFunction {

  public CLIP(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of limit pairs on top of the stack.");
    }

    final List<Object> limits = (List<Object>) top;

    // Check that limits contains pair of ordered numeric bounds
    for (Object o: limits) {
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of limit pairs on top of the stack.");
      }

      List<Object> pair = (List<Object>) o;

      if (2 != pair.size()) {
        throw new WarpScriptException(getName() + " expects a list of limit pairs on top of the stack.");
      }

      if (!(pair.get(0) instanceof Number) || !(pair.get(1) instanceof Number)) {
        throw new WarpScriptException(getName() + " expects the limits to be numeric.");
      }

      long lower = ((Number) pair.get(0)).longValue();
      long upper = ((Number) pair.get(1)).longValue();

      if (lower > upper) {
        Collections.swap(pair, 0, 1);
      }
    }

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {

        List<Object> clipped = new ArrayList<Object>();

        if (element instanceof GeoTimeSerie) {
          for (Object o: limits) {
            List<Object> pair = (List<Object>) o;

            long lower = ((Number) pair.get(0)).longValue();
            long upper = ((Number) pair.get(1)).longValue();

            clipped.add(GTSHelper.timeclip((GeoTimeSerie) element, lower, upper));
          }
        } else if (element instanceof GTSEncoder) {
          for (Object o: limits) {
            List<Object> pair = (List<Object>) o;

            long lower = ((Number) pair.get(0)).longValue();
            long upper = ((Number) pair.get(1)).longValue();

            clipped.add(GTSHelper.timeclip((GTSEncoder) element, lower, upper));
          }
        } else {
          throw new WarpScriptException(getName() + " expects a GeoTimeSeries, a GTSEncoder or a list thereof under the list of limit.");
        }

        return clipped;
      }
    };
  }
}
