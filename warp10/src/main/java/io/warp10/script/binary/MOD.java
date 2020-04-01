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

package io.warp10.script.binary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Compute the modulo of the two operands on top of the stack
 */
public class MOD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MOD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();

    if (!(op2 instanceof Number) || !(op1 instanceof Number)) {
      throw new WarpScriptException(getName() + " operates on numeric arguments.");
    }

    if ((op2 instanceof Long || op2 instanceof Integer) && (op1 instanceof Long || op1 instanceof Integer)) {
      stack.push(((Number) op1).longValue() % ((Number) op2).longValue());
    } else {
      stack.push(((Number) op1).doubleValue() % ((Number) op2).doubleValue());
    }

    return stack;
  }
}
