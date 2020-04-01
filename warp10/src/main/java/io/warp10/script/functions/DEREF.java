//
//   Copyright 2019  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.warp10.script.MacroHelper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Modifies a Macro so LOAD/PUSHRx operations for the given symbols or registers
 * are replaced by a specific value
 */
public class DEREF extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final NOOP NOOP = new NOOP(WarpScriptLib.NOOP);
  private static final EVAL EVAL = new EVAL(WarpScriptLib.EVAL);
  
  public DEREF(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map of variable names or register id to values on top of the stack.");
    }

    Map<Object,Object> values = (Map<Object,Object>) top;

    top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a Macro.");
    }
        
    //
    // Now loop over the macro statement, replacing occurrences of X LOAD and PUSHRx by the use
    // of the associated value
    //
    
    List<Macro> allmacros = new ArrayList<Macro>();
    allmacros.add((Macro) top);
    
    while(!allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);
    
      if (((Macro) m).isSecure()) {
        throw new WarpScriptException(getName() + " cannot operate on a secure Macro.");
      }

      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
          continue;
        } else if (i > 0 && statements.get(i) instanceof LOAD) {
          Object symbol = statements.get(i - 1);
          if (symbol instanceof String && values.containsKey(symbol)) {
            Object value = values.get(symbol);

            // Treat Macros in a specific way
            if (value instanceof Macro) {
              // If the macro contains a single element push it on the stack as a shortcut
              if (1 == ((Macro) value).size()) {
                statements.set(i - 1, NOOP);
                statements.set(i, ((Macro) value).get(0));
              } else {
                statements.set(i - 1, value);
                statements.set(i, EVAL);
              }
            } else {
              statements.set(i - 1, NOOP);
              statements.set(i, value);              
            }
          }
        } else if (statements.get(i) instanceof PUSHR) {
          long register = (long) ((PUSHR) statements.get(i)).getRegister();
          if (values.containsKey(register)) {
            Object value = values.get(register);
            
            if (value instanceof Macro) {
              statements.set(i, MacroHelper.wrap((Macro) value));
            } else {
              statements.set(i, value);
            }
          }
        }
      }      

      List<Object> macstmt = m.statements();
      // Ignore the NOOPs
      int noops = 0;
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof NOOP) {
          noops++;
          continue;
        }
        macstmt.set(i - noops, statements.get(i));
      }
      m.setSize(statements.size() - noops);
    }
    
    stack.push(top);
    
    return stack;
  }
}
