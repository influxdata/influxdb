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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Create from a map a new map which contains only the keys in the argument list
 */
public class SUBMAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SUBMAP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object keys = stack.pop();
    
    Object map = stack.pop();

    if (!(map instanceof Map)) {
      throw new WarpScriptException(getName() + " operates on a map.");
    }
    
    if (!(keys instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of map keys on top of the stack.");
    }

    Map<Object,Object> submap = new LinkedHashMap<Object, Object>();
    
    for (Entry<Object,Object> entry: ((Map<Object,Object>) map).entrySet()) {
      if (((List) keys).contains(entry.getKey())) {
        submap.put(entry.getKey(), entry.getValue());
      }
    }
    
    stack.push(submap);

    return stack;
  }
}
