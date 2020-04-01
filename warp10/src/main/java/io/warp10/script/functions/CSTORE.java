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

/**
 * Conditional STORE. Store an object in a symbol only if the symbol does not yet exist.
 */
public class CSTORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public CSTORE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object var = stack.pop();
    
    if (!(var instanceof String) && !(var instanceof Long)) {
      throw new WarpScriptException(getName() + " expects variable name to be a string or a register number.");
    }

    Object o = stack.pop();

    if (var instanceof String) {
      if (!stack.getSymbolTable().containsKey(var.toString())) {
        stack.store(var.toString(), o);      
      }
    } else if (null == stack.load(((Long) var).intValue())) {
      stack.store(((Long) var).intValue(), o);
    }
    
    return stack;
  }
}
