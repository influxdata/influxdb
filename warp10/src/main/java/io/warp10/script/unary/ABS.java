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

package io.warp10.script.unary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.math.BigDecimal;

/**
 * Compute the absolute value of the number on the stack
 */
public class ABS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ABS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (!(op instanceof Number)) {
      throw new WarpScriptException(getName() + " can only operate on numerical value.");
    }

    if (op instanceof Double || op instanceof Float || op instanceof BigDecimal) {
      stack.push(Math.abs(((Number) op).doubleValue()));
    } else {
      stack.push(Math.abs(((Number) op).longValue()));
    }
    
    return stack;
  }
}
