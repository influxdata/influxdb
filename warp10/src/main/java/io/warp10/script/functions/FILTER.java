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
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Filter some GTS instances.
 *
 * Calling parameters can be:
 * 
 * [ [GTS] [GTS] ... [labels] filter ] FILTER
 * 
 */
public class FILTER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean flatten;
  
  public FILTER(String name, boolean flatten) {
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
    
    for (int i = 0; i < params.size() - 2; i++) {
      if (!(params.get(i) instanceof List)) {
        throw new WarpScriptException(getName() + " expects lists of Geo Time Series as first parameters.");
      }              
    }
      
    if (null != params.get(params.size() - 2) && !(params.get(params.size() - 2) instanceof Collection<?>)) {
      throw new WarpScriptException(getName() + " expects a list of label names or null as penultimate parameter.");                
    } else {
      if (null != params.get(params.size() - 2)) {
        for (Object o: ((Collection<?>) params.get(params.size() - 2))) {
          if (! (o instanceof String)) {
            throw new WarpScriptException(getName() + " expects a list of label names as penultimate parameter.");                            
          }
        }        
      }
    }
      
    if (!(params.get(params.size() - 1) instanceof WarpScriptFilterFunction)) {
      throw new WarpScriptException(getName() + " expects a filter function as last parameter.");        
    }

    List<GeoTimeSerie>[] colls = new List[params.size() - 2];
    Collection<String> bylabels =  (Collection<String>) params.get(params.size() - 2);

    for (int i = 0; i < params.size() - 2; i++) {
      colls[i] = new ArrayList<GeoTimeSerie>();

      for (Object o: (List) params.get(i)) {
        if (o instanceof GeoTimeSerie) {
          colls[i].add((GeoTimeSerie) o);
        } else {
          throw new WarpScriptException(getName() + " expects lists of Geo Time Series as first parameters.");
        }
      }
    }
    
    if (flatten) {
      stack.push(GTSHelper.partitionAndApply(params.get(params.size() - 1), null, null, bylabels, colls));
    } else {
      stack.push(GTSHelper.partitionAndApplyUnflattened(params.get(params.size() - 1), null, null, bylabels, colls));
    }

    return stack;
  }
}
