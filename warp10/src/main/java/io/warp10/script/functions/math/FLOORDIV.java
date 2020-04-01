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

package io.warp10.script.functions.math;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Check https://docs.oracle.com/javase/8/docs/api/java/lang/Math.html for the documentation of this function.
 * Last java parameter is on top of the stack.
 */
public class FLOORDIV extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FLOORDIV(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op0 = stack.pop();

    if (!(op0 instanceof Number)) {
      throw new WarpScriptException(getName() + " can only operate on numerical values.");
    }

    Object op1 = stack.pop();

    if (!(op1 instanceof Number)) {
      throw new WarpScriptException(getName() + " can only operate on numerical values.");
    }

    if ((op1 instanceof Long || op1 instanceof Integer)
        && (op0 instanceof Long || op0 instanceof Integer)) {
      try {
        stack.push(Math.floorDiv(((Number) op1).longValue(), ((Number) op0).longValue()));
      } catch (ArithmeticException e) {
        throw new WarpScriptException(e);
      }
    } else {
      throw new WarpScriptException(getName() + " is given values of incorrect type. Expecting [ TOP:long, 2:long ].");
    }

    return stack;
  }
}
