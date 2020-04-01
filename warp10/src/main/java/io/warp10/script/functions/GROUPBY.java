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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Group elements of a list according to a macro
 */
public class GROUPBY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  
  public GROUPBY(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }
    
    Macro macro = (Macro) top;
    
    top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list.");
    }

    //
    // Generate the result of the macro for the various elements of the list, grouping the elements on the fly
    //
    
    Map<Object,List<Object>> groups = new HashMap<Object,List<Object>>();
    
    for (Object elt: (List) top) {
      stack.push(elt);
      stack.exec(macro);
      Object group = stack.pop();
      
      List<Object> grouped = groups.get(group);
      
      if (null == grouped) {
        grouped = new ArrayList<Object>();
        groups.put(group, grouped);
      }
      
      grouped.add(elt);
    }

    List<List<Object>> lgroups = new ArrayList<List<Object>>();
    
    for (Entry<Object,List<Object>> entry: groups.entrySet()) {
      List<Object> list = new ArrayList<Object>();
      list.add(entry.getKey());
      list.add(entry.getValue());
      lgroups.add(list);
    }
    
    stack.push(lgroups);
    
    return stack;
  }
}
