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

import io.warp10.continuum.gts.GeoTimeSerie;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract class for functions which manipulate GTS instances.
 * 
 * The GTS instances can be one of the following:
 * 
 * A single GTS on the stack
 * A list of GTS instances
 * 
 */
public abstract class GTSStackFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Method to apply to individual GTS instances
   */
  protected abstract Object gtsOp(Map<String,Object> params, GeoTimeSerie gts) throws WarpScriptException;
  
  /**
   * Method which extracts the parameters from the top of the stack
   */
  protected abstract Map<String,Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException;
  
  public GTSStackFunction(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Map<String,Object> params = retrieveParameters(stack);
    
    Object top = stack.pop();
    
    if (top instanceof GeoTimeSerie) {
      stack.push(gtsOp(params, (GeoTimeSerie) top));
    } else if (top instanceof List) {
      List<Object> results = new ArrayList<Object>();
      
      for (Object o: (List<Object>) top) {
        if (! (o instanceof GeoTimeSerie)) {
          stack.push(top);
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
        }
        results.add(gtsOp(params, (GeoTimeSerie) o));
      }
      
      stack.push(results);
    } else {
      stack.push(top);
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
    }
    
    return stack;
  }
}
