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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds a map from pairs of values on the stack.
 * 
 * Each pair is a String key and a value.
 * 
 * The number of elements to consider must be on top of the stack
 * when calling TOMAP.
 */
public class TOMAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOMAP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object[] elements = stack.popn();
    
    if (0 != elements.length % 2) {
      throw new WarpScriptException("Odd number of elements, cannot create map.");
    }
    
    Map<Object, Object> map = new LinkedHashMap<Object, Object>(elements.length / 2);
    
    for (int i = 0; i < elements.length; i+=2) {
      //if (! (elements[i] instanceof String)) {
      //  throw new WarpScriptException("Map keys must be strings.");
      //}
      if (map.containsKey(elements[i])) {
        throw new WarpScriptException("Duplicate map key '" + elements[i] + "'");
      }
      map.put(elements[i], elements[i+1]);
    }
    
    stack.push(map);
    return stack;
  }
}
