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
public class ATAN2 extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ATAN2(String name) {
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

    stack.push(Math.atan2(((Number) op1).doubleValue(), ((Number) op0).doubleValue()));

    return stack;
  }
}
