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

package io.warp10.script.binary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.List;
import java.util.Set;

/**
 * Adds the two operands on top of the stack but operates on lists and sets.
 * Operates in place on the first one.
 */
public class INPLACEADD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public INPLACEADD(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op1 instanceof List) {
      ((List) op1).add(op2);
      stack.push(op1);
    } else if (op1 instanceof Set) {
      ((Set) op1).add(op2);
      stack.push(op1);
    } else {
      throw new WarpScriptException(getName() + " can only operate on lists and sets.");
    }
    
    return stack;
  }
}
