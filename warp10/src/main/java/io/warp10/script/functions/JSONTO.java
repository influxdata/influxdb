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

import io.warp10.json.JsonUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Parses a String as JSON and pushes it onto the stack
 */
public class JSONTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public JSONTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a string on top of the stack.");
    }
    
    try {
      Object json = JsonUtils.jsonToObject(o.toString());
      stack.push(transform(json));
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " failed to parse JSON", ioe);
    }

    return stack;
  }
  
  private static final Object transform(Object json) {    
    if (json instanceof List) {
      List<Object> l = (List<Object>) json;
      List<Object> target = new ArrayList<Object>();
      
      for (int i=0; i < l.size(); i++) {
        target.add(transform(l.get(i)));
      }
      return target;
    } else if (json instanceof Map) {
      Map<Object,Object> map = (Map<Object,Object>) json;
      Map<Object,Object> target = new HashMap<Object, Object>();
      for (Entry<Object,Object> entry: map.entrySet()) {
        target.put(transform(entry.getKey()),transform(entry.getValue()));
      }
      return target;
    } else if (json instanceof Integer) {
      return ((Integer) json).longValue();
    } else {
      return json;
    }
  }
}
