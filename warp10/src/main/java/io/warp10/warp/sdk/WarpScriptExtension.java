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

package io.warp10.warp.sdk;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Map;

/**
 * Interface for extending WarpScript.
 * 
 * A class implementing this interface is called a WarpScript extension,
 * it makes available a set of functions which will redefine, remove or augment
 * the functions already defined in WarpScriptLib.
 *
 */
public abstract class WarpScriptExtension {
  /**
   * Return a map of function name to actual function.
   * If a function should be removed from WarpScriptLib,
   * simply associate null as the value.
   */
  public abstract Map<String,Object> getFunctions();
  
  /**
   * Wrap a macro into a function
   * 
   * @param macro Macro to wrap.
   * @return the wrapped macro as a WarpScriptStackFunction instance
   */
  public static final WarpScriptStackFunction wrapMacro(final Macro macro) {
    return new WarpScriptStackFunction() {      
      @Override
      public Object apply(WarpScriptStack stack) throws WarpScriptException {
        stack.exec(macro);
        return stack;
      }
      
      @Override
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(macro.toString());
        sb.append(" ");
        sb.append(WarpScriptLib.EVAL);
        return sb.toString();
      }
    };
  }
  
  public final void register() {
    WarpScriptLib.register(this);
  }
}
