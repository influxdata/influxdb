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

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.List;

/**
 * Computes the union of two GeoXPShape
 */
public class GeoUnion extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GeoUnion(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof List) {
      List list = (List) top;
      if (list.isEmpty()) {
        stack.push(GeoXPLib.fromCells(new long[] {}, false));
      } else {
        Object element = list.get(0);
        if (!(element instanceof GeoXPShape)) {
          throw new WarpScriptException(getName() + " expects two GeoShape instances as the top 2 elements of the stack or a list of GeoShape instances.");
        }
        GeoXPShape shape = (GeoXPShape) element;
        for (int i = 1; i < list.size(); i++) {
          element = list.get(i);
          if (!(element instanceof GeoXPShape)) {
            throw new WarpScriptException(getName() + " expects two GeoShape instances as the top 2 elements of the stack or a list of GeoShape instances.");
          }
          shape = GeoXPLib.union(shape, (GeoXPShape) element);
        }
        stack.push(shape);
      }
    } else {
      Object o2 = stack.pop();

      if (!(top instanceof GeoXPShape) || !(o2 instanceof GeoXPShape)) {
        throw new WarpScriptException(getName() + " expects two GeoShape instances as the top 2 elements of the stack or a list of GeoShape instances.");
      }

      //
      // Compute union of 2 elements
      //
      stack.push(GeoXPLib.union((GeoXPShape) top, (GeoXPShape) o2));
    }

    return stack;
  }
}
