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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes a value into a list or byte array. Modifies the list or byte array on the stack.
 */
public class SET extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SET(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object key = stack.pop();
    Object value = stack.pop();    
    
    Object listOrByteArray = stack.peek();

    if (!(listOrByteArray instanceof List) && !(listOrByteArray instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a list or byte array.");
    }

    if (key instanceof List) {

      for (Object o: (List) key) {
        if (!(o instanceof Long)) {
          throw new WarpScriptException(getName() + " expects the key to be an integer or a list of integers when operating on a List.");
        }
      }

      List<Long> copyIndices = new ArrayList<>((List<Long>) key);
      int lastIdx = copyIndices.remove(copyIndices.size() - 1).intValue();
      Object container;

      try {
        container = GET.nestedGet((List) listOrByteArray, copyIndices);
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " tried to set an element at a nested path that does not exist in the input list.");
      }

      if (container instanceof List) {
        ((List) container).set(lastIdx, value);

      } else {
        throw new WarpScriptException(getName() + " tried to set an element at a nested path that does not exist in the input list.");
      }

    } else {
      if (!(key instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a key which is an LONG or a list of LONG.");
      }

      if (listOrByteArray instanceof List) {
        int idx = GET.computeAndCheckIndex(((Long) key).intValue(), ((List) listOrByteArray).size());
        ((List) listOrByteArray).set(idx, value);
      } else {
        byte[] data = (byte[]) listOrByteArray;

        if (!(value instanceof Long)) {
          throw new WarpScriptException(getName() + " expects the element to be a LONG in either [0,255] or [-128,127], when operating on a byte array.");
        }

        long l = ((Long) value).longValue();

        if (l < -128 || l > 255) {
          throw new WarpScriptException(getName() + " expects the element to be a LONG in either [0,255] or [-128,127], when operating on a byte array.");
        }

        byte elt = (byte) (l & 0xFFL);

        int idx = GET.computeAndCheckIndex(((Long) key).intValue(), data.length);

        data[idx] = elt;
      }
    }
    
    return stack;
  }
}
