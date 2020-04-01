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
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.geoxp.GeoXPLib;

public class MACROFILLER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static class MacroFillerWrapper extends NamedWarpScriptFunction implements WarpScriptFillerFunction {

    private final WarpScriptStack stack;
    private final Macro macro;
    
    private final int preWindow;
    private final int postWindow;
    
    public MacroFillerWrapper(String name, WarpScriptStack stack, Macro macro, int preWindow, int postWindow) {
      super(name);
      this.stack = stack;
      this.macro = macro;
      this.preWindow = preWindow;
      this.postWindow = postWindow;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(macro.toString());
      sb.append(" ");
      sb.append(this.preWindow);
      sb.append(" ");
      sb.append(this.postWindow);
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
    
    @Override
    public int getPreWindow() {
      return preWindow;
    }
    
    @Override
    public int getPostWindow() {
      return postWindow;
    }
    
    @Override
    public Object[] apply(Object[] args) throws WarpScriptException {
      //
      // Push arguments onto the stack
      //
           
      GeoTimeSerie gts = new GeoTimeSerie(0);
      gts.safeSetMetadata((Metadata) ((Object[]) args[0])[0]);
      // Our GTS
      stack.push(gts);
      gts = new GeoTimeSerie(0);
      gts.safeSetMetadata((Metadata) ((Object[]) args[0])[1]);
      // Other GTS
      stack.push(gts);

      long ts = 0L;
                  
      //
      // The args array starting at index 1 contains one element per preWindow tick,
      // the current tick of the other GTS and one element per postWindow tick
      //
      
      List<Object> ticks = new ArrayList<Object>(this.preWindow);

      for (int i = 1; i < args.length; i++) {
        //
        // Build an array with the infos of the current args element.
        // If the timestamp or value is null, create a special array
        //
        
        List<Object> tick = new ArrayList<Object>(5);
        Object[] atick = (Object[]) args[i];
        if (null == atick[0] || null == atick[3]) {
          tick.add(null);
          tick.add(Double.NaN);
          tick.add(Double.NaN);
          tick.add(Double.NaN);
          tick.add(null);
        } else {
          tick.add(atick[0]);
          if (GeoTimeSerie.NO_LOCATION != ((Number) atick[1]).longValue()) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(((Number) atick[1]).longValue());
            tick.add(latlon[0]);
            tick.add(latlon[1]);
          } else {
            tick.add(Double.NaN);
            tick.add(Double.NaN);
          }
          if (GeoTimeSerie.NO_ELEVATION != ((Number) atick[2]).longValue()) {
            tick.add(((Number) atick[2]).longValue());
          } else {
            tick.add(Double.NaN);
          }
          tick.add(atick[3]);
        }        
        
        // If the tick should be added to a window, do so
        if (i <= this.preWindow || i > this.preWindow + 1) {
          ticks.add(tick);
        }
        
        //
        // If 'i' is at the end of the prewindow, emit the prewindow
        // and the current tick, extract the timestamp of the current tick
        // and re-allocate the ticks array
        //
        
        if (this.preWindow + 1 == i) {
          stack.push(ticks);
          stack.push(tick);
          ticks = new ArrayList<Object>(this.postWindow);
          ts = ((Number) tick.get(0)).longValue();
        }        
      }
      
      stack.push(ticks);

      stack.push(ts);
      
      //
      // Execute macro
      //
      
      stack.exec(this.macro);
      
      //
      // Check type of result
      //
      
      Object res = stack.peek();
      
      if (res instanceof List) {
        stack.drop();
        
        return MACROMAPPER.listToObjects((List) res);
      } else {
        throw new WarpScriptException("Expected a [ ts lat lon elev value ] list as result of filler.");
      }
    }
    
    public Macro getMacro() {
      return macro;
    }
  }
  
  public MACROFILLER(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a 'post' window length on top of the stack.");
    }
    
    int postWindow = ((Number) top).intValue();
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a 'pre' window length below the 'post' window length.");
    }
    
    int preWindow = ((Number) top).intValue();
    
    top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a MACRO.");
    }
    
    
    stack.push(new MacroFillerWrapper(getName(), stack, (Macro) top, preWindow, postWindow));
    
    return stack;
  }
}
