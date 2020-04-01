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

package io.warp10.script.ext.debug;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.List;
import java.util.Map;

import io.warp10.script.functions.TYPEOF;


public class TDESCRIBE extends TYPEOF {

  public TDESCRIBE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    stack.push(this.rtypeof(o, 0, stack.DEFAULT_MAX_RECURSION_LEVEL));
    return stack;
  }

  private String rtypeof(Object o, int recursionlevel, int maxrecursionlevel) {
    if (recursionlevel > maxrecursionlevel) {
      return "... (recursion limit reached)";
    } else {
      if (o instanceof List) {
        if (0 == ((List) o).size()) {
          return "LIST []";
        } else {
          //type of the first element in the list
          return "LIST [ " + this.rtypeof(((List) o).get(0), recursionlevel + 1, maxrecursionlevel) + " ]";
        }
      } else if (o instanceof Map) {
        if (0 == ((Map) o).size()) {
          return "MAP {}";
        } else {
          //typeof the first available key/value
          Object key = null;
          Object value = null;
          for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) o).entrySet()) {
            key = entry.getKey();
            value = entry.getValue();
          }
          return "MAP { " + this.rtypeof(key, recursionlevel + 1, maxrecursionlevel) +
                  " : " + this.rtypeof(value, recursionlevel + 1, maxrecursionlevel) +
                  " } ";
        }
      } else {
        return super.typeof(o);
      }
    }
  }
}
