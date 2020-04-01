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
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

import com.geoxp.GeoXPLib.GeoXPShape;

/**
 * Checks whether a GTS is entirely contained within a shape
 */
public class GEOWITHIN extends GTSStackFunction  {

  private static final String SHAPE = "shape";

  public GEOWITHIN(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
  
    if (!(top instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " expects a geo shape on top of the stack.");
    }
    
    Map<String,Object> params = new HashMap<String, Object>();

    params.put(SHAPE, top);
    
    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    GeoXPShape shape = (GeoXPShape) params.get(SHAPE);

    return GTSHelper.geowithin(shape, gts);
  }
}
