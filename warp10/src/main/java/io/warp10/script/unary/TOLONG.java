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
 * Converts the operand on top of the stack to a Long
 */
public class TOLONG extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOLONG(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (op instanceof Number) {
      stack.push(((Number) op).longValue());
    } else if (op instanceof Boolean) {
      if (Boolean.TRUE.equals(op)) {
        stack.push(1L);
      } else {
        stack.push(0L);
      }
    } else if (op instanceof String) {
      stack.push(Long.decode(op.toString().trim()));
    } else if (op instanceof byte[]) {
      byte[] bytes = (byte[]) op;
      if (bytes.length > 8) {
        throw new WarpScriptException(getName() + " can only operate on byte arrays from 1 to 8 bytes.");
      }
      
      long v = 0L;
      
      for (byte b: bytes) {
        v <<= 8;
        v |= ((long) b) & 0xFFL;
      }
      
      // Now shift left then right again so we have the correct sign
      v <<= 64 - (8 * bytes.length);
      v >>= 64 - (8 * bytes.length);
      
      stack.push(v);
    } else {
      throw new WarpScriptException(getName() + " can only operate on numeric, boolean, byte array or string values.");
    }
    
    return stack;
  }
}
