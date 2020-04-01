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

import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Push onto the stack a macro containing a secure script wrapping the specified hook of the given token.
 * If the hook is not defined, an empty macro will be pushed.
 */
public class GETHOOK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GETHOOK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a hook name on top of the stack.");
    }
    
    String hook = o.toString();
    
    o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a read token below the hook name.");
    }
    
    ReadToken token = null;
    
    try {
      token = Tokens.extractReadToken(o.toString());
    } catch (WarpScriptException ee) {
      throw new WarpScriptException(ee);
    }
    
    Macro macro = new Macro();
    
    String script = token.getHooksSize() > 0 ? token.getHooks().get(hook) : null;
    
    if (null != script) {
      macro.add(SECURE.secure(UUID.randomUUID().toString(), hook));
      macro.add(new EVALSECURE(WarpScriptLib.EVALSECURE));
    }
    
    stack.push(macro);
    
    return stack;
  }
}
