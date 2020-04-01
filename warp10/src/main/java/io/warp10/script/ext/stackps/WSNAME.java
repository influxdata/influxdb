//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.script.ext.stackps;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class WSNAME extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final int MAX_SIZE = 128;
  
  /**
   * True when setting the session
   */
  private final boolean session;
  
  public WSNAME(String name, boolean session) {
    super(name);
    this.session = session;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (null != top && !(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a STRING.");
    }
    
    String name = null == top ? null : top.toString();
    
    if (null != name && name.length() > MAX_SIZE) {
      throw new WarpScriptException(getName() + " expects a name less than " + MAX_SIZE + " characters.");
    }
    
    if (session) {
      String session = (String) stack.getAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION);
      if (null == session && null != name) {
        session = name;
        stack.setAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION, name);
      }
      stack.push(session);
    } else {
      String cname = (String) stack.getAttribute(WarpScriptStack.ATTRIBUTE_NAME);
      if (null != name) {
        cname = name;
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, name);
      }
      stack.push(cname);
    }
    
    return stack;
  }
}
