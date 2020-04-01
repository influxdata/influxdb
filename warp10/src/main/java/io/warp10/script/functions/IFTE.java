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

/**
 * Implements 'If-Then-Else' conditional
 * 
 * 3: IF-macro 
 * 2: THEN-macro
 * 1: ELSE-macro
 * IFTE
 * 
 * Macros are popped out of the stack.
 * If-macro is evaluated, it MUST leave a boolean on top of the stack
 * Boolean is consumed, if true, THEN-macro is evaluated, otherwise ELSE-macro is
 */
public class IFTE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public IFTE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object elseMacro = stack.pop(); // ELSE-macro
    Object thenMacro = stack.pop(); // THEN-macro
    Object ifMacro = stack.pop(); // IF-macro
    
    //
    // Check that thenMacro and elseMacro are macros, ifMacro will be checked later
    //

    if (!(elseMacro instanceof Macro) || !(thenMacro instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects three macros or two macros and a boolean on top of the stack.");
    }

    //
    // Execute IF-macro
    //

    boolean ifResult;

    if (ifMacro instanceof Macro) {
      // Execute the IF macro
      stack.exec((Macro) ifMacro);

      // Check that the top of the stack is a boolean
      Object top = stack.pop();
      if (!(top instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects its 'IF' macro to leave a boolean on top of the stack.");
      }

      // Store the result of the macro execution, which is a boolean.
      ifResult = Boolean.TRUE.equals(top);
    } else if (ifMacro instanceof Boolean) {
      // IF is already a boolean, store it.
      ifResult = Boolean.TRUE.equals(ifMacro);
    } else {
      throw new WarpScriptException(getName() + " expects three macros or two macros and a boolean on top of the stack.");
    }
    
    //
    // If IF is 'true', execute the THEN-macro, otherwise execute the ELSE-macro
    //
    
    if (ifResult) {
      stack.exec((Macro) thenMacro);
    } else {
      stack.exec((Macro) elseMacro);
    }

    return stack;
  }
}
