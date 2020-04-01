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

package io.warp10.script.unary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Reverse the bits of the LONG on top of the stack
 */
public class REVERSEBITS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public REVERSEBITS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " can only operate on a LONG.");
    }

    long v = ((Number) top).longValue();
    
    long reversed = 0L;
    
    for (int i = 0; i < 64; i++) {
      reversed = reversed << 1;
      reversed = reversed | (v & 0x1L);
      v = v >>> 1;
    }
    
    stack.push(reversed);
        
    return stack;
  }
}
