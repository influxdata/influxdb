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

import java.util.BitSet;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Gets a bit in a bitset
 */
public class BITGET extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public BITGET(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object idx = stack.pop();
    
    if (!(idx instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a bit index on top of the stack.");
    }

    Object o = stack.pop();

    if (!(o instanceof BitSet)) {
      throw new WarpScriptException(getName() + " expects a bit set below the bit index.");
    }
    
    stack.push(((BitSet) o).get(((Number) idx).intValue()));
    
    return stack;
  }
}
