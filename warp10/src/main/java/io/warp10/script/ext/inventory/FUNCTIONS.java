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

package io.warp10.script.ext.inventory;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;


public class FUNCTIONS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FUNCTIONS(String name) {
    super(name);
  }

  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    //
    // Apply function and push its outputs onto the stack or raise an exception
    //

    stack.push(WarpScriptLib.getFunctionNames());

    //
    // Return the new state of the stack
    //

    return stack;
  }
}
