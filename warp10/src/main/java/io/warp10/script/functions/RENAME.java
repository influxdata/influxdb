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

/**
 * Apply rename on GTS or Encoder instance or list thereof.
 * <p>
 * RENAME expects the following parameters on the stack:
 * <p>
 * 1: name The new name
 */
public class RENAME extends ElementOrListStackFunction {

  public RENAME(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    final Object top = stack.pop();

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          return GTSHelper.rename((GeoTimeSerie) element, top.toString());
        } else if (element instanceof GTSEncoder) {
          GTSEncoder encoder = (GTSEncoder) element;

          GeoTimeSerie gts = new GeoTimeSerie();
          gts.setMetadata(encoder.getMetadata());

          gts = GTSHelper.rename(gts, top.toString());

          encoder.setMetadata(gts.getMetadata());

          return encoder;
        } else {
          throw new WarpScriptException(getName() + " expects a GeoTimeSeries, a GTSEncoder or a list thereof under the new name.");
        }
      }
    };
  }
}
