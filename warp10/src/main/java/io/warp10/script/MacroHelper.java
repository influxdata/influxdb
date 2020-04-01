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
package io.warp10.script;

import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * Class containing various methods related to macros.
 */
public class MacroHelper {
  
  private static final class MacroWrapper extends NamedWarpScriptFunction implements WarpScriptStackFunction, Snapshotable {

    private final Macro macro;
    
    public MacroWrapper(String name, Macro macro) {
      super(name);
      this.macro = macro;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      stack.exec(this.macro);
      return stack;
    }
     
    @Override
    public String toString() {
      if (null != getName()) {
        return super.toString();
      } else {
        return this.macro.toString() + " " + WarpScriptLib.EVAL;
      }
    }
    
    @Override
    public String snapshot() {
      if (macro instanceof Snapshotable) {
        return macro.snapshot() + " " + WarpScriptLib.EVAL;
      } else {
        return this.toString();
      }
    }
  }
  
  public static WarpScriptStackFunction wrap(Macro m) {
    return new MacroWrapper(null, m);
  }
  
  public static WarpScriptStackFunction wrap(String name, String mc2, boolean secure) {
    
    if (mc2.startsWith("@")) {
      return wrap(name, getResource(mc2.substring(1)), secure);
    }
    
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();
    
    try {
      stack.execMulti(mc2);
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
    
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new RuntimeException("WarpScript code did not leave a macro on top of the stack.");
    }
    
    ((Macro) top).setSecure(secure);
    
    return new MacroWrapper(name, (Macro) top);
  }

  public static WarpScriptStackFunction wrap(String mc2, boolean secure) {
    return wrap(null, mc2, secure);
  }

  public static WarpScriptStackFunction wrap(String mc2) {
    return wrap(null, mc2, false);
  }

  public static WarpScriptStackFunction wrap(String name, String mc2) {
    return wrap(name, mc2, false);
  }

  public static WarpScriptStackFunction wrap(String name, InputStream in, boolean secure) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    byte[] buf = new byte[1024];

    try {
      while(true) {
        int len = in.read(buf);
        
        if (len < 0) {
          break;
        }
        
        baos.write(buf, 0, len);
      }      
      
      in.close();
      
      String mc2 = new String(baos.toByteArray(), StandardCharsets.UTF_8);
      
      return wrap(name, mc2, secure);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public static WarpScriptStackFunction wrap(InputStream in, boolean secure) {
    return wrap(null, in, secure);
  }

  public static WarpScriptStackFunction wrap(InputStream in) {
    return wrap(null, in, false);
  }

  public static WarpScriptStackFunction wrap(String name, InputStream in) {
    return wrap(name, in, false);
  }

  private static InputStream getResource(String path) {
    InputStream in = MacroHelper.class.getResourceAsStream(path.startsWith("/") ? path : "/" + path);
    if (null == in) {
      throw new RuntimeException("Resource " + path + " was not found.");
    }
    return in;
  }
}
