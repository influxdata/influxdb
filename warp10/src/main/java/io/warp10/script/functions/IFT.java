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
 * Implements 'If-Then' conditional
 * 
 * 2: Macro2
 * 1: Macro1
 * IFT
 * 
 * Macro1 and Macro2 are popped out of the stack.
 * Macro2 is evaluated, it MUST leave a boolean on top of the stack
 * Boolean is consumed, if true, Macro1 is evaluated
 */
public class IFT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public IFT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object thenMacro = stack.pop(); // THEN-macro
    Object ifMacro = stack.pop(); // IF-macro
    
    //
    // Check than thenMacro is a macro, ifMacro will be checked later.
    //
    
    if (!(thenMacro instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a *THEN* macro on top of the stack.");
    }
    
    //
    // Execute IF-macro
    //

    boolean ifResult;

    if (ifMacro instanceof Macro) {
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
      throw new WarpScriptException(getName() + " expects an *IF* macro or a boolean below the *THEN* macro.");
    }
    
    //
    // If IF is 'true', execute the THEN-macro
    //
    
    if (ifResult) {
      stack.exec((Macro) thenMacro);
    }

    return stack;
  }
}
