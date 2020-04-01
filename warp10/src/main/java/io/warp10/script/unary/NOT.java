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

package io.warp10.script.unary;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Unary NOT of the operand on top of the stack
 */
public class NOT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public NOT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (op instanceof Boolean) {
      stack.push(!((Boolean) op).booleanValue());
    } else if (op instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) op;
      if (GeoTimeSerie.TYPE.BOOLEAN == gts.getType()) {
        GeoTimeSerie result = gts.clone();
        GTSHelper.booleanNot(result);
        stack.push(result);
      } else if (GeoTimeSerie.TYPE.UNDEFINED == gts.getType()) {
        // empty gts
        stack.push(gts.cloneEmpty());
      } else {
        throw new WarpScriptException(getName() + " can only operate on a boolean value or a boolean GTS.");
      }
    } else {
      throw new WarpScriptException(getName() + " can only operate on a boolean value or a boolean GTS.");
    }
    
    return stack;
  }
}
