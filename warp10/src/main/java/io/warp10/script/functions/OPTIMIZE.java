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

import io.warp10.CapacityExtractorOutputStream;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;

/**
 * Optimize storage of GeoTimeSerie or encoder instances
 */
public class OPTIMIZE extends ElementOrListStackFunction {

  public OPTIMIZE(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a ratio on top of the stack.");
    }

    final double ratio = ((Number) top).doubleValue();

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) element;
          GTSHelper.shrink(gts, ratio);
          return gts;
        } else if (element instanceof GTSEncoder) {
          GTSEncoder encoder = (GTSEncoder) element;
          if (encoder.size() > 0) {
            CapacityExtractorOutputStream extractor = new CapacityExtractorOutputStream();
            try {
              encoder.writeTo(extractor);
              if ((double) extractor.getCapacity() / (double) encoder.size() > ratio) {
                encoder.resize(encoder.size());
              }
            } catch (IOException ioe) {
              throw new WarpScriptException(getName() + " encountered an error while shrinking encoder.", ioe);
            }
          }
          return encoder;
        } else {
          throw new WarpScriptException(getName() + "  operates on a Geo Time Series or encoder instance or a list thereof.");
        }
      }
    };
  }

}
