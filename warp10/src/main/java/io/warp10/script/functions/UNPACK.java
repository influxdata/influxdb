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
import java.util.BitSet;
import java.util.List;

/**
 * Pack a list of numeric or boolean values according to a specified format
 */
public class UNPACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public UNPACK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a format string on top of the stack.");
    }

    String fmt = o.toString();
    
    o = stack.pop();
    
    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on an array of bytes.");
    }
    
    byte[] data = (byte[]) o;

    //
    // Parse the format
    //

    BitSet bigendians = new BitSet();
    List<Character> types = new ArrayList<Character>();
    List<Integer> lengths = new ArrayList<Integer>();

    PACK.parseFormat(this, fmt, bigendians, types, lengths);

    //
    // Now decode the various values
    //
    
    int bitno = 0;

    //
    // Invert the bits of the input data
    //
    
    byte[] atad = new byte[data.length];
    
    for (int i = 0; i < data.length; i++) {
      // see http://graphics.stanford.edu/~seander/bithacks.html#ReverseByteWith64BitsDiv
      atad[i] = (byte) ((((((long) data[i]) & 0xFFL) * 0x0202020202L & 0x010884422010L) % 1023L) & 0xFFL);
    }
    
    BitSet bits = BitSet.valueOf(atad);
    
    List<Object> values = new ArrayList<Object>();
    
    for (int i = 0; i < types.size(); i++) {

      char type = types.get(i);
      int len = lengths.get(i);
      
      if ('s' == type || 'S' == type) {
        bitno += len;
        continue;
      }
      
      if ('B' == type) {
        values.add(bits.get(bitno++));
        continue;
      }
      
      boolean bigendian = bigendians.get(i);
      
      //
      // Extract bits
      //
      
      long value = 0L;
      
      for (int k = 0; k < len; k++) {
        value <<= 1;
        
        if (!bigendian) {
          value |= (bits.get(bitno + len - 1 - k) ? 1 : 0);
        } else {
          value |= (bits.get(bitno + k) ? 1 : 0);
        }        
      }

      bitno += len;

      switch (type) {
        case 'D': // double
          values.add(Double.longBitsToDouble(value));
          break;
        case 'L': // long
          values.add((value << (64 - len)) >> (64 - len));
          break;
        case 'U': // Unsigned long
          values.add(value);
          break;
      }
    }

    stack.push(values);
    
    return stack;
  }
}
