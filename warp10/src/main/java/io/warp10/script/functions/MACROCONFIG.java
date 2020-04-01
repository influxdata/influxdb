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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.warp10.WarpConfig;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Reads macro configuration keys
 * This function is not intended to be used outside of Macro Repositories, Macro libraries and WarpFleet™ Resolver
 */
public class MACROCONFIG extends NamedWarpScriptFunction implements WarpScriptStackFunction {
   
  private final boolean defaultValue;
  
  public MACROCONFIG(String name, boolean defaultValue) {
    super(name);
    this.defaultValue = defaultValue;
  }
  
  public MACROCONFIG(String name) {
    this(name, false);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    String macro = (String) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MACRO_NAME);

    if (null == macro) {
      throw new WarpScriptException(getName() + " can only be used from named macro.");
    }
        
    Object top = stack.pop();
    
    String defVal = null;
    
    if (this.defaultValue) {
      defVal = null == top ? null : String.valueOf(top);
      top = stack.pop();
    }
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a macro configuration key name.");
    }

    String key = String.valueOf(top).trim();

    String value = null;
    
    List<String> attempts = new ArrayList<String>();

    if (this.defaultValue) {
      attempts.add(key + "@" + macro);
      value = WarpConfig.getProperty(key + "@" + macro, defVal);  
    } else {
      //
      // We will attempt to find key@name/of/macro
      // then fallback to key@name/of
      // then fallback to key@name
      //
            
      String m = macro;
      
      while(true) {
        attempts.add(key + "@"  + m);
        
        value =  WarpConfig.getProperty(key + "@" + m);
            
        if (null != value) {
          break;
        }
            
        if (!m.contains("/")) {
          break;
        }
        
        m = m.replaceAll("/[^/]+$", "");
      }       
    }
       
    if (null == value) {
      StringBuilder sb = new StringBuilder();
      sb.append(getName() + " macro configuration '" + key + "' not found, need to set one of [");
      for (String attempt: attempts) {
        sb.append(" '" + attempt + "'");
      }
      sb.append(" ].");
      throw new WarpScriptException(sb.toString());
    }
    
    stack.push(value);
    
    return stack;
  }
}
