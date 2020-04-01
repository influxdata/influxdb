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

package io.warp10.script.ext.shadow;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.warp10.WarpConfig;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.FAIL;
import io.warp10.script.functions.NOOP;
import io.warp10.warp.sdk.WarpScriptExtension;

public class ShadowWarpScriptExtension extends WarpScriptExtension {
  
  private static final String SHADOW_RENAME = "shadow.rename.";
  private static final String SHADOW_NOOP = "shadow.noop";
  private static final String SHADOW_FAIL = "shadow.fail";
  private static final String SHADOW_FAILMSG = "shadow.failmsg";
  private static final String SHADOW_UNDEF = "shadow.undef";
  private static final String SHADOW_MACRO = "shadow.macro";
  // Set to true (or shadow.unsafemacro.FUNCTION to true) to allow the symbol table to
  // override the macro at runtime
  private static final String SHADOW_UNSAFEMACRO = "shadow.unsafemacro";
  private static final String SHADOW_MACROPREFIX = "shadow.macroprefix";
  
  private static final Map<String,Object> functions;
  
  public static class ShadowFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    public ShadowFunction(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      return stack;
    }
  }
  
  static {
    functions = new HashMap<String,Object>();
    Properties properties = WarpConfig.getProperties();
    
    //
    // Handle the renaming first
    //
    
    for (String key: properties.stringPropertyNames()) {
      if (key.startsWith(SHADOW_RENAME)) {
        final String f = key.substring(SHADOW_RENAME.length());

        String target = WarpConfig.getProperty(key);
        
        Object from = WarpScriptLib.getFunction(f);
        
        // Store the new association
        functions.put(target, from);
        // Remove the original association
        functions.put(f, null);
      }
    }
    
    //
    // Generate the functions shadowed by a macro
    //
    
    String conf = WarpConfig.getProperty(SHADOW_MACRO);
    
    if (null != conf) {
      String prefix = WarpConfig.getProperty(SHADOW_MACROPREFIX);
      boolean globalUnsafe = "true".equals(WarpConfig.getProperty(SHADOW_UNSAFEMACRO));
      
      if (null == prefix) {
        prefix = "";
      } else {
        prefix = prefix + "/";
      }
            
      String[] func = conf.split(",");
            
      for (String f: func) {
        f = f.trim();
      
        boolean unsafe = globalUnsafe;
        
        if (null != WarpConfig.getProperty(SHADOW_UNSAFEMACRO + "." + f)) {
          unsafe = "true".equals(WarpConfig.getProperty(SHADOW_UNSAFEMACRO + "." + f));
        }

        final boolean finalUnsafe = unsafe;
        
        final String macro = prefix + f;
                
        WarpScriptStackFunction function = new ShadowFunction(f) {        
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            if (!finalUnsafe) {
              stack.save();
            }
            try {
              if (!finalUnsafe) {
                stack.forget(macro);
              }
              Macro m = stack.find(macro);
              stack.exec(m);
            } finally {
              if (!finalUnsafe) {
                stack.restore();
              }
            }
            return stack;
          }
        };
        
        functions.put(f, function);
      }    
    }
        
    //
    // Generate the functions shadowed by specific macros
    //
    
    for (String key: properties.stringPropertyNames()) {
      if (key.startsWith(SHADOW_MACRO + ".")) {
        final String f = key.substring(SHADOW_MACRO.length() + 1);
        boolean globalUnsafe = "true".equals(WarpConfig.getProperty(SHADOW_UNSAFEMACRO));
                
        boolean unsafe = globalUnsafe;
        
        if (null != WarpConfig.getProperty(SHADOW_UNSAFEMACRO + "." + f)) {
          unsafe = "true".equals(WarpConfig.getProperty(SHADOW_UNSAFEMACRO + "." + f));
        }

        final boolean finalUnsafe = unsafe;

        final String macro = properties.getProperty(key);
        
        WarpScriptStackFunction function = new ShadowFunction(f) {        
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            if (!finalUnsafe) {
              stack.save();
            }
            try {
              if (!finalUnsafe) {
                stack.forget(macro);
              }
              Macro m = stack.find(macro);
              stack.exec(m);
            } finally {
              if (!finalUnsafe) {
                stack.restore();
              }
            }
            return stack;
          }
        };
        
        functions.put(f, function);
      }
    }
    
    //
    // Generate functions shadowed by a NOOP
    //
    
    conf = WarpConfig.getProperty(SHADOW_NOOP);
    
    if (null != conf) {
      String[] func = conf.split(",");
            
      for (String f: func) {
        f = f.trim();
        
        functions.put(f, new NOOP(f));
      }
    }
    
    //
    // Generate functions shadowed by a FAIL or MSGFAIL
    //
    
    conf = WarpConfig.getProperty(SHADOW_FAIL);
    
    if (null != conf) {
      String[] func = conf.split(",");
      
      final String msg = WarpConfig.getProperty(SHADOW_FAILMSG);
            
      for (String f: func) {
        f = f.trim();

        final String fu = f;
        
        if (null == msg) {
          functions.put(f, new FAIL(f));
        } else {                    
          WarpScriptStackFunction function = new ShadowFunction(f) {            
            @Override
            public Object apply(WarpScriptStack stack) throws WarpScriptException {
              throw new WarpScriptException(msg);
            }
          };
          
          functions.put(f, function);
        }
      }
    }
    
    //
    // Generate functions shadowed by a specific FAILMSG
    //
    
    for (String key: properties.stringPropertyNames()) {
      if (key.startsWith(SHADOW_FAIL + ".")) {
        final String f = key.substring(SHADOW_FAIL.length() + 1);
        
        final String failmsg = properties.getProperty(key);
        
        WarpScriptStackFunction function = new ShadowFunction(f) {        
          @Override
          public Object apply(WarpScriptStack stack) throws WarpScriptException {
            throw new WarpScriptException(failmsg);
          }
        };
        
        functions.put(f, function);
      }
    }

    //
    // Generate the functions which are undefined (their value in the functions map is null)
    //
    
    conf = WarpConfig.getProperty(SHADOW_UNDEF);

    if (null != conf) {
      String[] func = conf.split(",");
      
      for (String f: func) {
        f = f.trim();
        
        functions.put(f, null);
      }
    }
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
