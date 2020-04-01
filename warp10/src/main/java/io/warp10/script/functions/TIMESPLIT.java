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
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLoopContinueException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Apply timesplit on GTS instances
 *
 * TIMESPLIT expects the following parameters on the stack:
 *
 * 3: quiet A quiet period in microseconds
 * 2: minValues A minimum number of values
 * 1: label The name of the label which will hold the sequence
 */
public class TIMESPLIT extends ElementOrListStackFunction {

  public TIMESPLIT(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {Object top = stack.pop();
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a label on top of the stack.");
    }

    final String label = (String) top;

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a minimum number of values under the label.");
    }

    final int minValues = ((Number) top).intValue();

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a quiet period under the minimum number of values.");
    }

    final long quietPeriod = (long) top;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if(element instanceof GeoTimeSerie){
          return GTSHelper.timesplit((GeoTimeSerie)element, quietPeriod, minValues, label);
        } else {
          throw new WarpScriptException(getName()+ " expects a Geo Time Series or a list thereof under the label, minimum values and the quiet period parameters.");
        }
      }
    };
  }
}
