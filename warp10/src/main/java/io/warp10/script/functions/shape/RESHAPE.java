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

package io.warp10.script.functions.shape;

import java.util.ArrayList;
import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.FLATTEN;

public class RESHAPE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final FLATTEN FLATTEN = new FLATTEN("FLATTEN");
  
  public RESHAPE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a shape (a list of dimensions).");
    }
    
    List<Object> shape = (List<Object>) top;
    
    long ncells = 1;
    
    for (Object o: shape) {
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects dimensions to be LONGs.");
      }
      
      long dim = ((Number) o).longValue();
      
      if (dim > 0) {
        ncells = ncells * dim;
      } else {
        throw new WarpScriptException(getName() + " dimension cannot be negative or null.");
      }
    }
    
    top = stack.peek();
    
    if (!(top instanceof List)) {
      stack.pop();
      throw new WarpScriptException(getName() + " operates on a list.");
    }
    
    // Flatten the list
    FLATTEN.apply(stack);
    
    List<Object> elts = (List<Object>) stack.pop();
    
    if (elts.size() != ncells) {
      throw new WarpScriptException(getName() + " expected " + ncells + " elements, but found " + elts.size());
    }
    
    // Now perform a reshape of the List

    for (int k = shape.size() - 1; k >= 1; k--) {
      int dim = ((Number) shape.get(k)).intValue();
      
      List<Object> ll = elts;
      int size = elts.size() / dim;
      
      if (0 == size) {
        throw new WarpScriptException(getName() + " invalid shape, cannot create " + dim + " elements from " + elts.size());
      }
 
      elts = new ArrayList<Object>(size);

      int idx = 0;
      while (idx < ll.size()) {
        elts.add(ll.subList(idx, idx + dim));
        idx += dim;
      }
    }

    stack.push(elts);
    
    return stack;
  }
}
