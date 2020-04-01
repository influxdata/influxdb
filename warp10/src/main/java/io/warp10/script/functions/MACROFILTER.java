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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MACROFILTER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static class MacroFilterWrapper extends NamedWarpScriptFunction implements WarpScriptFilterFunction {

    private final WarpScriptStack stack;
    private final Macro macro;
    
    public MacroFilterWrapper(String name, WarpScriptStack stack, Macro macro) {
      super(name);
      this.stack = stack;
      this.macro = macro;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(macro.toString());
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
    
    @Override
    public List<GeoTimeSerie> filter(Map<String, String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {
      //
      // Push arguments onto the stack
      //
      
      stack.push(labels);
      
      List<List<GeoTimeSerie>> lseries = new ArrayList<List<GeoTimeSerie>>();
      
      for (List<GeoTimeSerie> lserie: series) {
        lseries.add(lserie);
      }
      
      stack.push(lseries);
      
      //
      // Execute macro
      //
      
      stack.exec(this.macro);
      
      //
      // Retrieve result
      //

      Object result = stack.pop();
      
      if (!(result instanceof List)) {
        throw new WarpScriptException(getName() + " macro should return a list of Geo Time Series onto the stack.");
      }
      
      return (List<GeoTimeSerie>) result;
    }
    
    public Macro getMacro() {
      return macro;
    }
  }
  
  public MACROFILTER(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }
    
    stack.push(new MacroFilterWrapper(getName(), stack, (Macro) top));
    
    return stack;
  }
}
