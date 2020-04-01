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
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Wrap a Reducer so it is only applied if there are no missing (null) values for a given tick
 */
public class STRICTREDUCER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public STRICTREDUCER(String name) {
    super(name);
  }
  
  private static final class StringentReducer extends NamedWarpScriptFunction implements WarpScriptReducerFunction, WarpScriptAggregatorFunction {
    
    private final WarpScriptReducerFunction reducer;
    
    public StringentReducer(String name, WarpScriptReducerFunction reducer) {
      super(name);
      this.reducer = reducer;
    }
    
    @Override
    public Object apply(Object[] args) throws WarpScriptException {
      
      Object[] values = (Object[]) args[6];
      
      for (Object value: values) {
        if (null == value) {
          return new Object[] { args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
        }
      }
      
      return reducer.apply(args);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(reducer.toString());
      sb.append(" ");
      sb.append(getName());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      return sb.toString();
    }
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof WarpScriptReducerFunction)) {
      throw new WarpScriptException(getName() + " expects a reducer on top of the stack.");
    }
    
    WarpScriptReducerFunction reducer = (WarpScriptReducerFunction) o;
    
    stack.push(new StringentReducer(getName(), reducer));
    
    return stack;
  }
}
