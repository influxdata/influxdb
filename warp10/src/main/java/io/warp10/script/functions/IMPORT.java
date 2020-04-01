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

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class IMPORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public IMPORT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an alias for the imported namespace on top of the stack.");
    }
    
    String alias = top.toString();
    
    top = stack.pop();
    
    if (null != top && !(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the name of the imported namespace or null below its alias.");
    }

    String imported = null == top ? null : top.toString();
    
    Map<String,String> rules = (Map<String,String>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_IMPORT_RULES);
    
    if (null == rules) {
      rules = new TreeMap<String,String>(new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          // Sort in decreasing length order
          return Integer.compare(o2.length(), o1.length());
        }
      });
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_IMPORT_RULES, rules);
    }
    
    if (null == imported) {
      rules.remove(alias);
    } else {
      rules.put(alias, imported);
    }
    
    return stack;
  }
}
