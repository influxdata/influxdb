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
import io.warp10.script.WarpScriptStack.Macro;

public class EVERY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String EVERY_STACK_ATTRIBUTE = "08E9C117-6F3C-4A76-85C1-F3A03CBA5C52.every";
  
  public EVERY(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a repetition delay in ms on top of the stack.");
    }
    
    stack.setAttribute(EVERY_STACK_ATTRIBUTE, top);
    
    top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro below the repetition delay.");
    }
    
    //
    // Execute the macro
    //
    
    stack.exec((Macro) top);
    return stack;
  }
}
