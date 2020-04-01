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

public class LOAD extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LOAD(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object var = stack.pop();
    
    if (!(var instanceof String) && !(var instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a variable name (STRING) or a register number (LONG) on top of the stack.");
    }
    
    Object val = var instanceof Long ? stack.load(((Long) var).intValue()) : stack.load(var.toString());
    
    if (null == val) {
      if (var instanceof String && !stack.getSymbolTable().containsKey(var.toString())) {
        throw new WarpScriptException(getName() + " symbol '" + var + "' does not exist.");
      }
    }
    
    stack.push(val);
    
    return stack;
  }
}
