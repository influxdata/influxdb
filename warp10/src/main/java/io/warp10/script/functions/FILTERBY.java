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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Filters a list according to a macro
 */
public class FILTERBY extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public FILTERBY(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }
    
    Macro macro = (Macro) top;
    
    top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list.");
    }

    //
    // Generate the result of the macro for the various elements
    //
    
    List<Object> filtered = new ArrayList<Object>();
        
    for (Object elt: (List) top) {
      stack.push(elt);
      stack.exec(macro);
      
      Object result = stack.pop();
      
      if (Boolean.TRUE.equals(result)) {
        filtered.add(elt);
      } else if (!(result instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects its macro to leave a BOOLEAN on the stack.");
      }
    }
    
    stack.push(filtered);
    
    return stack;
  }
}
