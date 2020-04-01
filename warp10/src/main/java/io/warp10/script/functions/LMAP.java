//
//   Copyright 2019  SenX S.A.S.
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
import io.warp10.script.WarpScriptStack.Macro;

import java.util.ArrayList;
import java.util.List;

/**
 * Produces a list which is the result of the application of 'macro' on each element of 'list'
 * If this.flat==true and if macro returns a list, the list is flattened into the result list.
 * <p>
 * 2: list
 * 1: macro
 * LMAP
 * <p>
 * The macro and the list are popped out of the stack.
 * The macro is called for each element of 'list' with the index of the current element, by default, and the element itself on the stack.
 *
 */
public class LMAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean flat;

  public LMAP(String name) {
    this(name, false);
  }
  
  public LMAP(String name, boolean flat) {
    super(name);
    this.flat = flat;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    boolean pushIndex = true;
    if (top instanceof Boolean) {
      pushIndex = (Boolean) top;
      top = stack.pop();
    }
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }
    
    Object list = stack.pop();
    
    if (!(list instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list below the macro on top of the stack.");
    }
    
    int n = ((List) list).size();
    ArrayList<Object> result = new ArrayList<Object>(n);
    
    for (int i = 0; i < n; i++) {
      stack.push(((List) list).get(i));
      if (pushIndex) {
        stack.push((long) i);
      }
      stack.exec((Macro) top);

      Object o = stack.pop();

      if (!flat || !(o instanceof List)) {
        result.add(o);
      } else {
        for (Object oo: (List) o) {
          result.add(oo);
        }
      }
    }
    
    stack.push(result);
    
    return stack;
  }
}
