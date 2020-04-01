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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Collection;
import java.util.Map;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;

/**
 * Pushes on the stack the size of an object (map, list or GTS). Consumes the object.
 */
public class SIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SIZE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {        
    Object obj = stack.pop();

    if (obj instanceof Map) {
      stack.push((long) ((Map) obj).size());
    } else if (obj instanceof Collection) {
      stack.push((long) ((Collection) obj).size());
    } else if (obj instanceof GeoTimeSerie) {
      // Return the number of values, not nticks which would return the number of buckets
      stack.push((long) GTSHelper.nvalues((GeoTimeSerie) obj));
    } else if (obj instanceof GTSEncoder) {
      stack.push(((GTSEncoder) obj).getCount());
    } else if (obj instanceof String) {
      stack.push((long) obj.toString().length());
    } else if (obj instanceof byte[]) {
      stack.push(((byte[]) obj).length);
    } else if (obj instanceof GeoXPShape) {
      stack.push(GeoXPLib.getCells((GeoXPShape) obj).length);
    } else {
      throw new WarpScriptException(getName() + " operates on a map, a collection, a string, a byte array or a GTS.");
    }
    
    return stack;
  }
}
