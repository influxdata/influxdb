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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Checks if a macro is defined and pushes true or false on the stack accordingly
 */
public class DEFINEDMACRO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean fail;
  
  public DEFINEDMACRO(String name) {
    super(name);
    this.fail = false;
  }
  
  public DEFINEDMACRO(String name, boolean fail) {
    super(name);
    this.fail = fail;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a string on top of the stack.");
    }
    
    try {
      stack.find(o.toString());
      if (!this.fail) {
        stack.push(true);
      }
    } catch (WarpScriptException wse) {
      if (this.fail) {
        throw new WarpScriptException(getName() + " macro '" + o.toString() + "' could not be loaded.");
      }
      stack.push(false);
    }
    
    return stack;
  }
}
