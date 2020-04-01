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

import java.util.List;

/**
 * Implements a 'repeat...until' loop
 * 
 * 2: RUN-macro
 * 1: UNTIL-macro
 * UNTIL
 * 
 * Macros are popped out of the stack.
 * Step-1: RUN-macro is evaluated
 * Step-2: UNTIL-macro is evaluated, it is expected to leave a boolean on the top of the stack
 * Step-3: Boolean is consumed
 * Step-4: if boolean was false, go to step 1, otherwise stop
 */
public class UNTIL extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  /**
   * Maximum time in 'ms' we allow the loop to run
   */
  private final long maxtime;
  
  public UNTIL(String name) {
    super(name);
    this.maxtime = -1L;
  }
  
  public UNTIL(String name, long maxtime) {
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

    Object untilMacro = top;// until-macro
    Object runMacro = stack.pop(); // RUN-macro

    //
    // Check that what we popped are macros
    //

    if (!(runMacro instanceof Macro) || !(untilMacro instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects two macros on top of the stack.");
    }
  
    long now = System.currentTimeMillis();
    
    long maxtime = this.maxtime > 0 ? this.maxtime : (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION);

    long counter = 0;
    while (true) {
      
      if (System.currentTimeMillis() - now > maxtime) {
        throw new WarpScriptException(getName() + " executed for too long (> " + maxtime + " ms).");
      }
      
      //
      // Execute RUN-macro
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

      //
      // Execute UNTIL-macro
      //
      
      stack.exec((Macro) untilMacro);
      
      //
      // Check that the top of the stack is a boolean
      //
      
      top = stack.pop();
      
      if (! (top instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects its 'UNTIL' macro to leave a boolean on top of the stack.");
      }

      //
      // If UNTIL-macro left 'true' on top of the stack, exit the loop
      //
      
      if (Boolean.TRUE.equals(top)) {
        break;
      }
    }

    return stack;
  }
}
