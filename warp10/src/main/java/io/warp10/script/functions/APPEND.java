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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Append a list to another list or a map to another map
 */
public class APPEND extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public APPEND(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    Object undertop = stack.pop();

    if (top instanceof List && undertop instanceof List) {
      try {
        ((List) undertop).addAll((List) top);
      } catch (UnsupportedOperationException uoe) {
        // This exception may be thrown when attempting to merge append
        // a list to a first list produced by Arrays.asList, in this case
        // we copy undertop to an empty list first and attempt again
        List l = new ArrayList<Object>();
        l.addAll((List) undertop);
        undertop = l;
        ((List) undertop).addAll((List) top);
      }
      stack.push(undertop);
    } else if (top instanceof Map && undertop instanceof Map) {
      ((Map) undertop).putAll((Map) top);
      stack.push(undertop);
    } else if (top instanceof GeoTimeSerie && undertop instanceof GeoTimeSerie) {
      stack.push(GTSHelper.merge((GeoTimeSerie)undertop, (GeoTimeSerie)top));
    } else {
      throw new WarpScriptException(getName() + " can only operate on 2 lists, 2 maps or 2 GTSs.");
    }

    return stack;
  }
}
