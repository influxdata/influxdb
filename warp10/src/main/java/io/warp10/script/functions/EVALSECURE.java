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
import io.warp10.script.WarpScriptATCException;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptReturnException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Evaluate a secure script
 */
public class EVALSECURE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public EVALSECURE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.peek();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " can only evaluate a secure script stored as a string.");
    }
    
    //
    // Disable exports while in a secure script
    //
    
    Object exported = stack.getAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS, null);
    
    //
    // Clear debug depth so you can't peek into a Secure Script
    //
    
    int debugDepth = (int) stack.getAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH, 0);
    
    //
    // Force secure macro mode
    //
    
    boolean secure = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO));
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO, true);
    
    //
    // Disallow redefined functions
    //
    
    boolean allowredefs = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_ALLOW_REDEFINED));
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_ALLOW_REDEFINED, false);
    
    Throwable error = null;
    
    try {
      new UNSECURE("", false).apply(stack);
      
      o = stack.pop();
            
      stack.execMulti(o.toString());
    } catch (WarpScriptReturnException ere) {
      if (stack.getCounter(WarpScriptStack.COUNTER_RETURN_DEPTH).decrementAndGet() > 0) {
        throw ere;
      }
    } catch (WarpScriptATCException wsatce) {
        throw wsatce;
    } catch (Throwable t) {
      error = t;
      // We need to clear the stack otherwise we may leak some information
      stack.clear();
      throw t;      
    } finally {
      //
      // Set secure macro mode to its original value
      //
      
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO, secure);

      //
      // Reset the redefined function support
      //
      
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_ALLOW_REDEFINED, allowredefs);
      
      if (null == error) {
        //
        // Set debug depth back to its original value
        //
        
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH, debugDepth);

        //
        // Reset list of exported symbols
        //
        
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS, exported);        
      }
    }
    
    return stack;
  }
}
