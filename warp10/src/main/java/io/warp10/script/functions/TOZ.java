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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.List;

/**
 * Build a morton code (Z-order curve) given N numbers and a bit width
 */
public class TOZ extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOZ(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a bit width on top of the stack.");
    }

    int bitwidth = ((Long) o).intValue();
        
    if (bitwidth > 63 || bitwidth < 0) {
      throw new WarpScriptException(getName() + " expects a bit width <= 63.");
    }
    
    o = stack.pop();

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of positive LONGs.");
    }
    
    long[] longs = new long[((List) o).size()];
    
    int idx = 0;
    
    for (Object oo: (List) o) {
      if (!(oo instanceof Long) || ((Long) oo) < 0) {
        throw new WarpScriptException(getName() + " operates on a list of positive LONGs.");
      }
      longs[idx++] = ((Long) oo).longValue();
    }
    
    int nbits = bitwidth * longs.length;
    int nbytes = (nbits / 8) + (0 == (nbits % 8) ? 0 : 1);
    int bitcount = nbytes * 8 - nbits;

    byte[] encoded = new byte[nbytes];
    
    long value = 0L;
    
    int byteidx = encoded.length;
    
    for (int i = 0; i < bitwidth; i++) {
      for (int j = longs.length - 1; j >= 0; j--) {
        value = value << 1;
        value = value | (longs[j] & 0x1L);
        longs[j] = longs[j] >>> 1;
        bitcount++;
        
        if (8 == bitcount) {
          //
          // Reverse the lower 8 bits
          // @see http://graphics.stanford.edu/~seander/bithacks.html#ReverseByteWith64BitsDiv
          //
          
          value = (value * 0x0202020202L & 0x010884422010L) % 1023L;

          encoded[--byteidx] = (byte) (value & 0xFFL);
          bitcount = 0;
          value = 0L;
        }
      }
    }

    stack.push(encoded);
    
    return stack;
  }
}
