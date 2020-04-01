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
import io.warp10.script.WarpScriptStack.Macro;

/**
 * (re)define a statement 'à la PostScript'
 */
public class DEF extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public DEF(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object stmt = stack.pop();
    
    if (!(stmt instanceof String)) {
      throw new WarpScriptException(getName() + " expects statement name to be a string.");
    }
    
    Object macro = stack.pop();
    
    if (null != macro && !(macro instanceof Macro)) {
      throw new WarpScriptException(getName() + " needs a macro below the statement name.");
    }
    
    stack.define(stmt.toString(), (Macro) macro);
    
    return stack;
  }
}
