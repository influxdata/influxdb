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
import io.warp10.script.WarpScriptLoopBreakException;
import io.warp10.script.WarpScriptLoopContinueException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

/**
 * Implements a 'While' loop
 * 
 * 2: WHILE-macro
 * 1: RUN-macro
 * WHILE
 * 
 * Macros are popped out of the stack.
 * Step-1: WHILE-macro is evaluated, it is expected to leave a boolean on the top of the stack
 * Step-2: Boolean is consumed
 * Step-3: if boolean was true, eval RUN-macro and go to step 1, otherwise, stop
 */
public class WHILE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Maximum time in 'ms' we allow the loop to run
   */
  private final long maxtime;
  
  public WHILE(String name) {
    super(name);
    this.maxtime = -1L;
  }
  
  public WHILE(String name, long maxtime) {
    super(name);
    this.maxtime = maxtime;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    boolean pushCounter = false;
    if (top instanceof Boolean) {
      pushCounter = (Boolean) top;
      top = stack.pop();
    }

    Object runMacro = top;// RUN-macro
    Object whileMacro = stack.pop(); // WHILE-macro
    
    //
    // Check that what we popped are macros
    //

    if (!(runMacro instanceof Macro) || !(whileMacro instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects two macros on top of the stack.");
    }

    long now = System.currentTimeMillis();
    
    long maxtime = this.maxtime > 0 ? this.maxtime : (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION);

    long counter = 0;
    while (true) {
      
      if (maxtime > 0 && (System.currentTimeMillis() - now > maxtime)) {
        throw new WarpScriptException(getName() + " executed for too long (> " + maxtime + " ms).");
      }
      
      //
      // Execute WHILE-macro if not check after
      //

        stack.exec((Macro) whileMacro);

        //
        // Check that the top of the stack is a boolean
        //

        top = stack.pop();

        if (!(top instanceof Boolean)) {
          throw new WarpScriptException(getName() + " expects its 'WHILE' macro to leave a boolean on top of the stack.");
        }

        //
        // Stop the loop is condition is false
        //

        if(Boolean.FALSE.equals(top)) {
          break;
        }

      //
      // Execute the RUN-macro
      //

      try {
        if (pushCounter) {
          stack.push(counter++);
        }
        stack.exec((Macro) runMacro);
      } catch (WarpScriptLoopBreakException elbe) {
        break;
      } catch (WarpScriptLoopContinueException elbe) {
        // Do nothing!
      }

    }

    return stack;
  }
}
