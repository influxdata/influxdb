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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Reads a file and execute it as WarpScript code.
 * This function is not intended to be used outside of Macro Repositories and Macro libraries
 */
public class INCLUDE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final File root;
  private final JarFile jar;
  private final AtomicBoolean enabled;
  
  public INCLUDE(String name, File root, AtomicBoolean enabled) {
    super(name);
    this.root = root;
    this.jar = null;
    this.enabled = enabled;
  }
  
  public INCLUDE(String name, JarFile jar, AtomicBoolean enabled) {
    super(name);
    this.root = null;
    this.jar = jar;
    this.enabled = enabled;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    if (!enabled.get()) {
      throw new WarpScriptException(getName() + " can only be used when loading macros, not when using them.");
    }
    
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a macro path on top of the stack.");
    }
    
    BufferedReader br = null;
  
    long lineno = 0;
    
    try {
      Reader reader = null;
      
      if (null != this.root) {
        //File f = new File(this.root, new File(top.toString()).getAbsolutePath().substring(this.root.getAbsolutePath().length() + 1));
        File f = new File(this.root, top.toString());
        
        String path = f.getAbsolutePath();
        
        if (!path.startsWith(root.getAbsolutePath())) {
          throw new WarpScriptException(getName() + " invalid path.");
        }
        
        if (!f.exists()) {
          throw new WarpScriptException(getName() + " was unable to load '" + path.substring(root.getAbsolutePath().length() + 1) + "'.");
        }

        reader = new FileReader(f);        
      } else {
        JarEntry entry = this.jar.getJarEntry(top.toString());
        reader = new InputStreamReader(this.jar.getInputStream(entry));
      }
      
      br = new BufferedReader(reader);
        
      while(true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        lineno++;
        stack.exec(line);
      }
            
    } catch (WarpScriptException ee) {
      throw new WarpScriptException("Error line " + lineno, ee);
    } catch (Exception e) {
      throw new WarpScriptException("Error line " + lineno, e);
    } finally {
      if (null != br) {
        try {
          br.close();
        } catch (Exception e) {          
        }
      }
    }
    
    return stack;
  }
}
