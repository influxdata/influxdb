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

import io.warp10.crypto.SipHashInline;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.nio.charset.StandardCharsets;

/**
 * Compute a SipHash of its input converted to String
 */
public class HASH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Random key used for computing the SipHash
   */
  
  private static final long[] key = new long[] { 0xF214F9635AF54BBEL, 0x8DD1E8D856D26E46L };
  
  public HASH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    long k0 = key[0];
    long k1 = key[1];
    
    if (o instanceof Long) {
      k1 = ((Number) o).longValue();
      o = stack.pop();
      
      if (o instanceof Long) {
        k0 = ((Number) o).longValue();
        o = stack.pop();
      } else {
        throw new WarpScriptException(getName() + " expects two LONGs as hash key.");
      }
    }
            
    byte[] data = null;
    
    if (o instanceof String) {
      data = o.toString().getBytes(StandardCharsets.UTF_8);
    } else if (o instanceof byte[]) {
      data = (byte[]) o;
    } else {
      throw new WarpScriptException(getName() + " operates on a byte array or STRING.");
    }

    stack.push(SipHashInline.hash24(k0, k1, data, 0, data.length));

    return stack;
  }
}
