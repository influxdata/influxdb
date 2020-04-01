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

package io.warp10.continuum.gts;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes several lists of GTS as input and output lists of lists of GTS where
 * the ith list is composed of the ith element of each input list, except if one of the lists
 * is a singleton, in which case the singleton is used.
 */
public class ZIP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ZIP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on lists of lists.");
    }

    List<Object> metalist = (List<Object>) top;
    
    //
    // Check the size of the various lists, they are either singletons or of the same size
    //

    int len = 1;
    
    for (Object o: metalist) {
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " operates on lists of lists.");
      }
      
      int size = ((List) o).size();
      
      if (0 == size) {
        throw new WarpScriptException(getName() + " cannot operate on empty lists.");
      }
      
      if (1 == size) {
        continue;
      }
      
      if (1 == len) {
        len = size;
      } else if (size != len) {
        throw new WarpScriptException(getName() + " operates on lists of lists. All lists which are not singletons must be of the same size.");
      }
    }
      
    int i = 0;
    
    List<List<Object>> results = new ArrayList<List<Object>>();
    
    while (i < len) {
      List<Object> l = new ArrayList<Object>();
      
      for (Object list: metalist) {
        Object o;
        
        if (1 == ((List) list).size()) {
          o = ((List) list).get(0);
        } else {
          o = ((List) list).get(i);
        }
        
        l.add(o);
      }
      
      results.add(l);
      
      i++;
    }
    
    stack.push(results);
    
    return stack;
  }
}
