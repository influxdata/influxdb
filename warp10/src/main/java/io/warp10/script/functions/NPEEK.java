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

import java.io.PrintWriter;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class NPEEK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public NPEEK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {    
    PrintWriter out = (PrintWriter) stack.getAttribute(WarpScriptStack.ATTRIBUTE_INTERACTIVE_WRITER);
    boolean json = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_INTERACTIVE_JSON));
        
    if (null != out) {
      Object top = stack.pop();
      
      int count = ((Number) top).intValue();
      
      if (count > 0) {
        if (count > stack.depth()) {
          count = stack.depth();
        }
        
        for (int i = count - 1; i >= 0; i--) {
          Object o = stack.get(i);
          PSTACK.print(out, i, o, json);
        }

        out.flush();        
      }
    }
    
    return stack;
  }
}
