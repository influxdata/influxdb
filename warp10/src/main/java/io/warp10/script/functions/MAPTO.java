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

import java.util.Map;
import java.util.Map.Entry;

/**
 * Deconstructs a map, putting each key/value pair as two elements on the stack.
 * The function then puts the number of new elements on the stack
 * on the stack.
 * 
 */
public class MAPTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public MAPTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Map)) {
      throw new WarpScriptException("Invalid type, expected a map.");
    }
    
    Map<String,Object> map = (Map<String,Object>) o;
    
    for (Entry<String, Object> entry: map.entrySet()) {
      stack.push(entry.getKey());
      stack.push(entry.getValue());
    }
    
    stack.push(map.size() * 2);
    
    return stack;
  }
}
