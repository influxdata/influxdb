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

/**
 * Push onto the stack all elements of the list on top
 * of the stack and push the list size.
 * 
 * If the top of the stack is not a list, do nothing.
 * 
 */
public class LISTTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LISTTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object list = stack.pop();

    if (list instanceof Object[]) {
      for (Object o: (Object[]) list) {
        stack.push(o);
      }
      stack.push((long) ((Object[]) list).length);
    } else if (list instanceof List) {
      for (Object o: (List<Object>) list) {
        stack.push(o);
      }
      stack.push((long) ((List<Object>) list).size());
    } else {
      throw new WarpScriptException(getName() + " operates on a list.");
    }
    return stack;
  }
}
