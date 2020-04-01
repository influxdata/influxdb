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

import java.util.Collection;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Checks if a list contains an element
 */
public class CONTAINS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public CONTAINS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object elt = stack.pop();    
    Object coll = stack.peek();

    if (coll instanceof Collection) {
      stack.push(((Collection) coll).contains(elt));      
    } else if (coll instanceof String) {
      stack.pop();
      stack.push(coll.toString().contains(elt.toString()));
    } else {
      throw new WarpScriptException(getName() + " operates on a list, set or STRING.");
    }
    

    return stack;
  }
}
