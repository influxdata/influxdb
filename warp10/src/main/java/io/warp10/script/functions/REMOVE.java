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

import java.util.List;
import java.util.Map;

/**
 * Remove an entry from a map or from a list
 */
public class REMOVE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public REMOVE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object key = stack.pop();
    Object coll = stack.pop();

    if (coll instanceof Map) {
      if (!(key instanceof String)) {
        throw new WarpScriptException(getName() + " expects a string as key.");
      }      
      
      Object o = ((Map) coll).remove(key);

      stack.push(coll);
      stack.push(o);
    } else if (coll instanceof List) {
      if (!(key instanceof Long) && !(key instanceof Integer)) {
        throw new WarpScriptException(getName() + " expects an integer as key.");
      }
      int idx = ((Number) key).intValue();

      int size = ((List) coll).size();

      if (idx < 0) {
        idx += size;
      }

      Object o = null;
      if (idx >= 0 && idx < size) {
        o = ((List) coll).remove(idx);
      }
      
      stack.push(coll);
      stack.push(o);
    } else {
      throw new WarpScriptException(getName() + " operates on a map or a list.");      
    }
    
    return stack;
  }
}
