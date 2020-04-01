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

import java.util.List;

/**
 * Implements 'Switch' like conditional
 * 
 * 2N+2: CONDITION MACRO FOR CASE 1
 * 2N+1: EXEC MACRO FOR CASE 1
 * ...
 * 4: CONDITION MACRO FOR CASE N
 * 3: EXEC MACRO FOR CASE N
 * 2: DEFAULT MACRO
 * 1: Number of cases
 * SWITCH
 * 
 * Macros are popped out of the stack.
 * The condition macros are evaluated in order. If a condition macro returns true on the stack, the associated exec macro is evaluated.
 * If no condition macro returns true, the default macro is evaluated.
 * The boolean returned by the condition macros is consumed.
 * Condition macros SHOULD generally leave the stack as they found it so the next macro can be evaluated
 */
public class SWITCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SWITCH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object oncases = stack.pop();
    
    if (!(oncases instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of cases on top of the stack.");
    }
    
    int ncases = ((Number) oncases).intValue();
    
    //
    // Pop default macro
    //
    
    Object defaultMacro = stack.pop();
    
    if (!(defaultMacro instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a default macro after the number of cases.");
    }
    
    Object[] cases = new Object[ncases * 2];
    
    for (int i = ncases - 1; i >= 0; i--) {
      cases[i * 2 + 1] = stack.pop();
      cases[i * 2] = stack.pop();
      
      if (!(cases[i * 2 + 1] instanceof Macro) || !(cases[i * 2] instanceof Macro)) {
        throw new WarpScriptException(getName() + " expects each case to be a set of two macros, a conditional macro with an 'exec' macro on top of it.");
      }
    }

    //
    // Execute conditional macros in order of their definition, stop when one has pushed 'true' on the stack
    //
    
    // Default macro to execute is 'defaultMacro'
    
    Macro exec = (Macro) defaultMacro;
    
    for (int i = 0; i < ncases; i++) {
      stack.exec((Macro) cases[i * 2]);
      Object result = stack.pop();
      
      if (Boolean.TRUE.equals(result)) {
        exec = (Macro) cases[i * 2 + 1];
        break;
      }
    }
    
    //
    // Execute 'exec' macro
    //
    
    stack.exec(exec);
    
    return stack;
  }
}
