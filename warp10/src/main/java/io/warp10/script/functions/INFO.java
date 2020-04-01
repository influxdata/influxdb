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

import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStopException;

/**
 * Consumes the map on the stack or leave it there and stop the script
 * if the stack is currently in documentation mode
 */
public class INFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public INFO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects an info map on top of the stack.");
    }
    
    if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_INFOMODE))) {
      // Push the documentation back on the stack
      // Perform a SNAPSHOT and an EVAL so we clone the info map
      StringBuilder sb = new StringBuilder();
      SNAPSHOT.addElement(sb, top);
      stack.execMulti(sb.toString());

      // Stop the script
      throw new WarpScriptStopException("");
    }
    
    return stack;
  }
}
