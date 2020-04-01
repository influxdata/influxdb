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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Pack a list of numeric or boolean values according to a specified format
 */
public class PACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PACK(String name) {
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

    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of numeric or boolean values.");
    }

    List<Object> values = (List<Object>) o;
    
    for (Object value: values) {
      if (!(value instanceof Number) && !(value instanceof Boolean)) {
        throw new WarpScriptException(getName() + " operates on a list of numeric or boolean values.");
      }
    }
        
    //
    // Parse the format
    //

    BitSet bigendians = new BitSet();
    List<Character> types = new ArrayList<Character>();
    List<Integer> lengths = new ArrayList<Integer>();
    
    int totalbits = PACK.parseFormat(this, fmt, bigendians, types, lengths);

    //
    // Now encode the various values
    //
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream(((totalbits + 7) / 8)); // Round up to multiple of 8
    
    int nbits = 0;
    int vidx = 0;
    
    long curbyte = 0;
    
    for (int i = 0; i < types.size(); i++) {
      
      int len = lengths.get(i);
      long value = 0L;
      
      if ('s' == types.get(i)) {
        value = 0L;
      } else if ('S' == types.get(i)) {
        value = 0xFFFFFFFFFFFFFFFFL;
      } else {
        Object v = values.get(vidx++);
        
        if (v instanceof Boolean) {
          if (Boolean.TRUE.equals(v)) {
            v = 1L;
          } else {
            v = 0L;
          }
        }

        if ('D' == types.get(i)) {
          value = Double.doubleToRawLongBits(((Number) v).doubleValue());
        } else if ('L' == types.get(i) || 'U' == types.get(i)) {
          value = ((Number) v).longValue();
        } else if ('B' == types.get(i)) {
          value = 0 != ((Number) v).longValue() ? 1L : 0L;
        }
      }

      // Reverse bits for big endians
      if (bigendians.get(i)) {
        value = Long.reverse(value);
        
        if (len < 64) {
          value >>>= (64 - len);
        }
      }
      
      for (int k = 0; k < len; k++) {
        curbyte <<= 1;
        curbyte |= (value & 0x1L);
        value >>= 1;
        nbits++;
        
        if (0 == nbits % 8) {
          baos.write((int) (curbyte & 0xFFL));
          curbyte = 0L;
        }
      }
    }

    // Right-pad with zeros
    if (0 != nbits % 8) {
      curbyte <<= 8 - (nbits % 8);
      baos.write((int) (curbyte & 0xFFL));
    }
    
    stack.push(baos.toByteArray());
    
    return stack;
  }

  public static int parseFormat(NamedWarpScriptFunction function, String format, BitSet bigendians, List<Character> types, List<Integer> lengths) throws WarpScriptException {
    int idx = 0;
    int totalbits = 0;

    while (idx < format.length()) {

      boolean isBigendian = false;

      char type = format.charAt(idx++);

      int len = 0;

      if ('<' == type || '>' == type) {
        if (idx >= format.length()) {
          throw new WarpScriptException(function.getName() + " encountered an invalid format specification.");
        }

        isBigendian = ('>' == type);

        type = format.charAt(idx++);

        if ('L' == type || 'U' == type) {
          // Parse length
          while (idx < format.length() && format.charAt(idx) <= '9' && format.charAt(idx) >= '0') {
            len *= 10;
            len += (int) (format.charAt(idx++) - '0');
          }

          if (0 == len) {
            // If no specified length, fall back to 64
            len = 64;
          } else if (len > 64) {
            throw new WarpScriptException(function.getName() + " encountered an invalid length for 'L', max length is 64.");
          }
        } else if ('D' == type) {
          len = 64;
        } else {
          throw new WarpScriptException(function.getName() + " encountered an invalid format specification '" + type + "'.");
        }
      } else if ('S' == type || 's' == type) {
        // Parse length
        while (idx < format.length() && format.charAt(idx) <= '9' && format.charAt(idx) >= '0') {
          len *= 10;
          len += (int) (format.charAt(idx++) - '0');
        }

        // If no specified length, error.
        if (0 == len) {
          throw new WarpScriptException(function.getName() + " encountered an invalid Skip specification, length must be a strictly positive number.");
        }
      } else if ('B' == type) {
        len = 1;
      } else {
        throw new WarpScriptException(function.getName() + " encountered an invalid format specification '" + type + "'.");
      }

      // Can't use bigendians.size() nor bigendians.length() because they don't give the number of defined bits (set or cleared).
      // bigendians, types and lengths contains the same number of elements, so we can use either types.size() or lengths.size().
      bigendians.set(types.size(), isBigendian);
      types.add(type);
      lengths.add(len);

      totalbits += len;
    }

    return totalbits;
  }

}
