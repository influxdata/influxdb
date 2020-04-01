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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Decode a URL Encoded string using UTF-8 as the encoding.
 */
public class URLDECODE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public URLDECODE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }
    
    try {
      // This call should not be replaced by WarpURLDecoder.decode as we really want
      // to URLDECODE the content
      stack.push(URLDecoder.decode(o.toString(), StandardCharsets.UTF_8.name()));
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }
    
    return stack;
  }
}
