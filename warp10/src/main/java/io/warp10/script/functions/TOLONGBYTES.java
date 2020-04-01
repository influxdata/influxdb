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

import com.google.common.primitives.Longs;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * Converts a LONG to a byte array
 */
public class TOLONGBYTES extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOLONGBYTES(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (o instanceof Long) {
      int nbBytes = ((Long) o).intValue();
      o = stack.pop();
      if (o instanceof Long && nbBytes > 0 && nbBytes <= 8) {
        //truncate the result to nbBytes when needed
        if (8 != nbBytes) {
          stack.push(Arrays.copyOfRange(Longs.toByteArray(((Long) o).longValue()), 8 - (int) nbBytes, 8));
        } else {
          stack.push(Longs.toByteArray(((Long) o).longValue()));
        }
      } else if (o instanceof List && nbBytes > 0 && nbBytes <= 8) {
        try {
          byte[] b = new byte[nbBytes * ((List) o).size()];
          ByteBuffer bytes = ByteBuffer.wrap(b);
          bytes.order(ByteOrder.BIG_ENDIAN);
          if (8 == nbBytes) {
            for (Object el: (List) o) {
              bytes.putLong((Long) el);
            }
          } else if (4 == nbBytes) {
            for (Object el: (List) o) {
              bytes.putInt(((Long) el).intValue());
            }
          } else if (2 == nbBytes) {
            for (Object el: (List) o) {
              bytes.putShort(((Long) el).shortValue());
            }
          } else if (1 == nbBytes) {
            for (Object el: (List) o) {
              bytes.put(((Long) el).byteValue());
            }
          } else {
            // weird length are not optimized
            long v;
            for (Object el: (List) o) {
              v = ((Long) el).longValue() << (8 - nbBytes) * 8;
              for (int i = 0; i < nbBytes; i++) {
                bytes.put((byte) (v >> 56));
                v <<= 8;
              }
            }
          }
          stack.push(b);
        } catch (Exception e) {
          throw new WarpScriptException(getName() + " operates on a LONG or a list of LONG and expects a number of output bytes per LONG between 1 and 8 on top of the stack.");
        }
      } else {
        throw new WarpScriptException(getName() + " operates on a LONG or a list of LONG and expects a number of output bytes per LONG between 1 and 8 on top of the stack.");
      }
    } else {
      throw new WarpScriptException(getName() + " operates on a LONG or a list of LONG and expects a number of output bytes per LONG between 1 and 8 on top of the stack.");
    }
    
    return stack;
  }
}
