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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Apply tickindex on GTS or encoder instances
 */
public class TICKINDEX extends ElementOrListStackFunction {
  
  private static final ElementStackFunction CONVERTER = new ElementStackFunction() {
    @Override
    public Object applyOnElement(Object element) throws WarpScriptException {
      try {
        if (element instanceof GeoTimeSerie) {
          return GTSHelper.tickindex((GeoTimeSerie) element);
        } else if (element instanceof GTSEncoder) {
          return GTSHelper.tickindex((GTSEncoder) element);
        }
        throw new WarpScriptException("Can only operate on Geo Time Series™ or GTS Encoder instances.");
      } catch (Exception e) {
        throw new WarpScriptException("Caught an exception while converting encoder.",e);
      }
    }  
  };
  
  public TICKINDEX(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return CONVERTER;
  }
}
