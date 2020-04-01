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
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Produce an empty clone of the parameter GTS or Encoder instance or list thereof.
 * <p>
 * CLONEEMPTY expects no other parameter on the stack than the GTS instances
 */
public class CLONEEMPTY extends ElementOrListStackFunction {

  private final ElementStackFunction cloneemptyFunction = new ElementStackFunction() {
    @Override
    public Object applyOnElement(Object element) throws WarpScriptException {
      if (element instanceof GeoTimeSerie) {
        return ((GeoTimeSerie) element).cloneEmpty();
      } else if (element instanceof GTSEncoder) {
        return ((GTSEncoder) element).cloneEmpty();
      } else {
        throw new WarpScriptException(getName() + " expects a GeoTimeSeries, a GTSEncoder or a list thereof on top of the stack.");
      }
    }
  };

  public CLONEEMPTY(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return cloneemptyFunction;
  }

}
