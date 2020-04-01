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

import java.util.HashMap;
import java.util.Map;

/**
 * Apply relabel on GTS instances
 * <p>
 * RELABEL expects the following parameters on the stack:
 * <p>
 * 1: labels A map of labels
 */
public class RELABEL extends ElementOrListStackFunction {

  public RELABEL(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map of labels as parameter.");
    }

    final Map<String, String> labels = (Map<String, String>) top;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          return GTSHelper.relabel((GeoTimeSerie) element, labels);
        } else if (element instanceof GTSEncoder) {
          return GTSHelper.relabel((GTSEncoder) element, labels);
        } else {
          throw new WarpScriptException(getName() + " expects a Geo Time Series, a GTSEncoder or a list thereof under the labels parameter.");
        }
      }
    };
  }
}
