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

import java.util.HashMap;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Set a header which will be returned with the HTTP response
 */
public class HEADER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public HEADER(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    String value = null;
    
    if (null != top) {
      value = top.toString();
    }
    
    top = stack.pop();
    
    if (!(top instanceof String) || null == top) {
      throw new WarpScriptException(getName() + " expects a header name (a string) below the header value.");
    }
        
    String name = (String) top;
        
    Map<String,String> headers = (Map<String,String>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS);
    
    if (null == headers) {
      headers = new HashMap<String, String>();
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_HEADERS, headers);
    }
    
    if (null == value) {
      headers.remove(name);
    } else {
      if (!name.toUpperCase().startsWith("X-")) {
        throw new WarpScriptException(getName() + " only headers beginning with 'X-' can be set.");
      }
      headers.put(name, value);
    }
    
    return stack;
  }
}
