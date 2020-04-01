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

import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Join N strings together, separating each one with a specified string
 */
public class JOIN extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public JOIN(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (o instanceof String) {
      String sep = o.toString();
      
      o = stack.pop();
      
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a separator and a list of strings to join on the stack.");
      }
      
      StringBuilder sb = new StringBuilder();

      for (Object elt: (List) o) {
        if (0 != sb.length()) {
          sb.append(sep);
        }
        sb.append(String.valueOf(elt));
      }
      
      stack.push(sb.toString());
    } else {
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects the number of strings to join on top of the stack.");
      }
      
      int n = ((Number) o).intValue();
      
      o = stack.pop();
      
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " expects the separator string below the number of strings to join.");
      }
      
      String sep = o.toString();
      
      stack.push(n);
      
      Object[] obj = stack.popn();

      StringBuilder sb = new StringBuilder();


      for (int i = 0; i < obj.length; i++) {
        if (0 != i) {
          sb.append(sep);        
        }
        sb.append(obj[i].toString());
      }      
      
      stack.push(sb.toString());
    }

    
    return stack;
  }
}
