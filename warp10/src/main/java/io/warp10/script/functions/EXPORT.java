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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Update or set the list of exported symbols
 */
public class EXPORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public EXPORT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
        
    Object osymbols = stack.getAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS);
    
    Set symbols = null;
    
    if (osymbols instanceof Set) {
      symbols = (Set) osymbols;
    }
    
    if (o instanceof String) {
      if (null == symbols) {
        symbols = new HashSet<String>();        
      }
      
      symbols.add(o);
    } else if (o instanceof List) {
      for (Object oo: (List) o) {
        if (null != oo && !(oo instanceof String)) {
          throw new WarpScriptException(getName() + " expects a symbol name or a list thereof on top of the stack.");
        }        
      }
      
      symbols = new HashSet<String>();
      
      symbols.addAll((List) o);
      
      //
      // If one of the symbols is 'null', simply set the whole set to 'null'
      //
      
      if (symbols.contains(null)) {
        symbols.clear();
        symbols.add(null);
      }
    } else {
      throw new WarpScriptException(getName() + " expects a symbol name or a list thereof on top of the stack.");      
    }
    
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS, symbols);
    
    return stack;
  }
}
