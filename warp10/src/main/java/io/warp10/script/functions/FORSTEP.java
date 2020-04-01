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
 * Implements a 'for' loop
 * 
 * 4: FROM
 * 3: TO
 * 2: STEP-macro
 * 1: RUN-macro
 * FOR
 * 
 * Arguments are popped out of the stack. FROM is expected to be <= TO
 * 
 * Step-0: initialize loop counter to FROM
 * Step-1: if loop counter > TO end loop
 * Step-2: push loop counter onto the stack
 * Step-3: eval RUN-macro
 * Step-4: push loop counter onto the stack
 * Step-5: eval STEP-macro and go to step 1
 */
public class FORSTEP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Maximum time in 'ms' we allow the loop to run
   */
  private final long maxtime;

  public FORSTEP(String name) {
    super(name);
    this.maxtime = -1L;
  }
  
  public FORSTEP(String name, long maxtime) {
    super(name);
    this.maxtime = maxtime;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    boolean pushCounter = true;
    if (top instanceof Boolean) {
      pushCounter = (Boolean) top;
      top = stack.pop();
    }

    Object macroRun = top;// RUN-macro

    Object macroStep = stack.pop(); // STEP-macro
    Object to = stack.pop(); // TO
    Object from = stack.pop(); // FROM
    
    if (!(macroRun instanceof Macro) || !(macroStep instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects two macros on top of the stack.");
    }
    
    if (!(to instanceof Number) || !(from instanceof Number)) {
      throw new WarpScriptException(getName() + " expects numbers as its range.");
    }
    
    double dfrom = 0;
    double dto = 0;
    double dcounter = 0;
    
    long lfrom = 0;
    long lto = 0;
    long lcounter = 0;
    
    boolean useDouble = false;

    boolean ascending = true;
    
    if (from instanceof Double || to instanceof Double) {
      dfrom = ((Number) from).doubleValue();
      dto = ((Number) to).doubleValue();
      if (dfrom > dto) {
        ascending = false;
      }
      dcounter = dfrom;
      useDouble = true;
    } else {
      lfrom = ((Number) from).longValue();
      lto = ((Number) to).longValue();      
      if (lfrom > lto) {
        ascending = false;
      }
      lcounter = lfrom;
    }
    
    long now = System.currentTimeMillis();
    
    long maxtime = this.maxtime > 0 ? this.maxtime : (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION);
    
    while (true) {
      if (System.currentTimeMillis() - now > maxtime) {
        throw new WarpScriptException(getName() + " executed for too long (> " + maxtime + " ms).");
      }
      
      if (useDouble) {
        if ((ascending && dcounter > dto) || (!ascending && dcounter < dto)) {
          break;
        }
      } else {
        if ((ascending && lcounter > lto) || (!ascending && lcounter < lto)) {
          break;
        }
      }
      
      //
      // Push counter onto the stack
      //

      if (pushCounter) {
        if (useDouble) {
          stack.push(dcounter);
        } else {
          stack.push(lcounter);
        }
      }
      
      //
      // Execute RUN-macro
      //
      
      try {
        stack.exec((Macro) macroRun);
      } catch (WarpScriptLoopBreakException elbe) {
        break;
      } catch (WarpScriptLoopContinueException elbe) {
        // Do nothing!
      }
      
      //
      // Increment counter
      //
      
      if (useDouble) {
        stack.push(dcounter);
        stack.exec((Macro) macroStep);
        dcounter = ((Number) stack.pop()).doubleValue();
      } else {
        stack.push(lcounter);
        stack.exec((Macro) macroStep);
        lcounter = ((Number) stack.pop()).longValue();
      }
    }

    return stack;
  }
}
