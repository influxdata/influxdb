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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Apply an operation on GTS instances.
 *
 * Calling parameters can be:
 * 
 * [ [GTS] [GTS] ... [labels] op ] APPLY
 * 
 */
public class APPLY extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean flatten;
  
  public APPLY(String name, boolean flatten) {
    super(name);
    this.flatten = flatten;    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input.");
    }
        
    List<Object> params = (List<Object>) top;
   
    if (params.size() < 3) {
      throw new WarpScriptException(getName() + " expects at least 3 parameters.");
    }
    
    //
    // Identify the operation
    //
    
    int opidx = -1;
    
    for (int i = 0; i < params.size(); i++) {
      if (params.get(i) instanceof WarpScriptNAryFunction) {
        opidx = i;
        break;
      }
    }
    
    if (-1 == opidx) {
      throw new WarpScriptException(getName() + " expects an operation in the parameter list.");
    }
    
    int labelsidx = opidx - 1;

    if (labelsidx < 1 || (null != params.get(labelsidx) && !(params.get(labelsidx) instanceof Collection<?>))) {
      throw new WarpScriptException(getName() + " expects a list of label names under the operation.");                
    } else {
      if (null != params.get(labelsidx)) {
        for (Object o: ((Collection<?>) params.get(labelsidx))) {
          if (! (o instanceof String)) {
            throw new WarpScriptException(getName() + " expects a list of label names as penultimate parameter.");                            
          }
        }        
      }
    }

    for (int i = 0; i < labelsidx; i++) {
      if (!(params.get(i) instanceof List)) {
        throw new WarpScriptException(getName() + " expects lists of Geo Time Series as first parameters.");
      }              
    }      
      
    List<GeoTimeSerie>[] colls = new List[labelsidx];
    Collection<String> bylabels = (Collection<String>) params.get(labelsidx);
        
    for (int i = 0; i < labelsidx; i++) {
      colls[i] = new ArrayList<GeoTimeSerie>();

      for (Object o: (List) params.get(i)) {
        if (o instanceof GeoTimeSerie) {
          colls[i].add((GeoTimeSerie) o);
        } else {
          throw new WarpScriptException(getName() + " expects lists of Geo Time Series as first parameters.");
        }
      }
    }
    
    Macro validator = null;
        
    if (opidx < params.size() - 1) {
      if (params.get(opidx + 1) instanceof Macro) {
        validator = (Macro) params.get(opidx + 1);
      }
    }
    
    if (this.flatten) {
      stack.push(GTSHelper.partitionAndApply(params.get(opidx), stack, validator, bylabels, colls));
    } else {
      stack.push(GTSHelper.partitionAndApplyUnflattened(params.get(opidx), stack, validator, bylabels, colls));
    }      
    
    return stack;
  }
}
