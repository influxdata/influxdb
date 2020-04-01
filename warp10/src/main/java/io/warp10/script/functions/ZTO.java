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

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Morton code into N subelements
 */
public class ZTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ZTO(String name) {
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
    
    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of components below the bit width.");
    }
    
    int nlongs = ((Long) o).intValue();
    
    o = stack.pop();

    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a Morton code encoded as a byte array.");
    }
    
    byte[] encoded = (byte[]) o;

    long[] longs = new long[nlongs];
    
    int byteidx = 0;
    
    int bitcount = 0;
    
    long value = 0L;
    
    for (int i = 0; i < bitwidth; i++) {
      for (int j = 0; j < longs.length; j++) {
        // Retrieve bits to decode       
        if (0 == bitcount) {    
          value = encoded[byteidx++] & 0xFFL;
          bitcount = 8;
          //
          // Reverse the lower 8 bits
          // @see http://graphics.stanford.edu/~seander/bithacks.html#ReverseByteWith64BitsDiv
          //
          
          value = (value * 0x0202020202L & 0x010884422010L) % 1023L;
        }
        
        longs[j] = (longs[j] << 1) | (value & 0x1L);
        value >>>= 1;
        bitcount--;
      }
    }
    
    List<Object> l = new ArrayList<Object>(longs.length);
    
    for (long lo: longs) {
      l.add(lo);
    }
    
    stack.push(l);

    return stack;
  }
}
