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
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Apply a mapper on some GTS instances
 */
public class MAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String PARAM_MAPPER = "mapper";
  private static final String PARAM_PREWINDOW = "pre";
  private static final String PARAM_POSTWINDOW = "post";
  private static final String PARAM_OCCURENCES = "occurences";
  private static final String PARAM_STEP = "step";
  private static final String PARAM_OVERRIDE = "override";
  private static final String PARAM_OUTPUTTICKS = "ticks";
  
  public MAP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (top instanceof Map) {
      
      //This Case handle the new (20150826) parameter passing mechanism      
      return applyWithParamsFromMap(stack, (Map<String, Object>) top);
    }
        
    //This Case handle the original parameter passing mechanism
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input or a map of parameters on top of a list of GTS.");
    }
    
    List<Object> params = (List<Object>) top;
    
    if (5 > params.size()) {
      throw new WarpScriptException(getName() + " needs a list of at least 5 parameters as input.");
    }
    
    int nseries = 0;
        
    for (int i = 0; i < params.size(); i++) {
      if (!(params.get(i) instanceof GeoTimeSerie) && !(params.get(i) instanceof List)) {
        break;
      }
      nseries++;
    }
    
    if (0 == nseries) {
      throw new WarpScriptException(getName() + " expects Geo Time Series or lists thereof as first parameters.");
    }
    
    if (!(params.get(nseries) instanceof WarpScriptMapperFunction) && !(params.get(nseries) instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a mapper function or a macro after Geo Time Series.");
    }
    
    if (!(params.get(nseries + 1) instanceof Long) || !(params.get(nseries + 2) instanceof Long) || !(params.get(nseries + 3) instanceof Long)) {
      throw new WarpScriptException(getName() + " expects prewindow, postwindow and occurrences as 3 parameters following the mapper function.");
    }
    
    int step = 1;
    
    if (params.size() > nseries + 4) {
      if (!(params.get(nseries + 4) instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a step parameter that is an integer number.");
      } else {
        step = ((Number) params.get(nseries + 4)).intValue();
        
        if (step <= 0) {
          throw new WarpScriptException(getName() + " expects a step parameter which is strictly positive.");
        }
      }      
    }
    
    boolean overrideTick = false;
    
    if (params.size() > nseries + 5) {
      if (!(params.get(nseries + 5) instanceof Boolean)) {
        throw new WarpScriptException(getName() + " expects a boolean as 'override tick' parameter.");
      } else {
        overrideTick = Boolean.TRUE.equals(params.get(nseries + 5));
      }            
    }
    
    List<Object> series = new ArrayList<Object>();

    for (int i = 0; i < nseries; i++) {      
      series.add(params.get(i));
    }
    stack.push(series);
    
    Map<String,Object> mapParams = new HashMap<String, Object>();
    
    mapParams.put(PARAM_MAPPER, params.get(nseries));
    mapParams.put(PARAM_PREWINDOW, params.get(nseries + 1));
    mapParams.put(PARAM_POSTWINDOW, params.get(nseries + 2));
    mapParams.put(PARAM_OCCURENCES, params.get(nseries + 3));
    mapParams.put(PARAM_STEP, (long) step);
    mapParams.put(PARAM_OVERRIDE, overrideTick);
    
    return applyWithParamsFromMap(stack, mapParams);
  }
  
  private Object applyWithParamsFromMap(WarpScriptStack stack, Map<String,Object> params) throws WarpScriptException {
    
    if (!params.containsKey(PARAM_MAPPER)) {
      throw new WarpScriptException(getName() + " Missing '" + PARAM_MAPPER + "' parameter.");
    }
    
    Object mapper = params.get(PARAM_MAPPER);
    long prewindow = !params.containsKey(PARAM_PREWINDOW) ? 0 : (long) params.get(PARAM_PREWINDOW);
    long postwindow = !params.containsKey(PARAM_POSTWINDOW) ? 0 : (long) params.get(PARAM_POSTWINDOW);
    long occurrences = !params.containsKey(PARAM_OCCURENCES) ? 0 : (long) params.get(PARAM_OCCURENCES);
    int step = !params.containsKey(PARAM_STEP) ? 1 : (int) ((long) params.get(PARAM_STEP));
    boolean overrideTick = params.containsKey(PARAM_OVERRIDE) && (boolean) params.get(PARAM_OVERRIDE);
    Object outputTicks = params.get(PARAM_OUTPUTTICKS);
    
    // Handle gts and nested list of gts
    
    Object top = stack.pop();
    
    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
    
    // top is expected to be a GTS, a list of GTS or a list of list of GTS
    if (top instanceof List) {
      for (Object o : (List) top) {
        if (o instanceof List) {
          // top is a list of list, o must be a list of gts
          for (Object oo : (List) o) {
            // o must be a gts
            if (!(oo instanceof GeoTimeSerie)) {
              throw new WarpScriptException(getName() + " operates on lists of Geo Time Series.");
            } else {
              series.add((GeoTimeSerie) oo);
            }
          }
        } else {
          // top is a list of gts, o must be a gts
          if (!(o instanceof GeoTimeSerie)) {
            throw new WarpScriptException(getName() + " operates on lists of Geo Time Series.");
          } else {
            series.add((GeoTimeSerie) o);
          }
        }
      }
    } else {
      // top must be a gts
      if (!(top instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " operates on lists of Geo Time Series.");
      } else {
        series.add((GeoTimeSerie) top);
      }
    }
    
    // Call MAP
    
    List<Object> mapped = new ArrayList<Object>();

    // Make sure Math.abs(occurrences) will return a positive value.
    if (Long.MIN_VALUE == occurrences) {
      occurrences = Long.MIN_VALUE + 1;
    }
    
    for (GeoTimeSerie gts: series) {
      List<GeoTimeSerie> res = GTSHelper.map(gts, mapper, prewindow, postwindow, Math.abs(occurrences), occurrences < 0, step, overrideTick, mapper instanceof Macro ? stack : null,
              (List<Long>) outputTicks);

      if (res.size() < 2) {
        mapped.addAll(res);
      } else {
        mapped.add(res);
      }
    }
    
    stack.push(mapped);
    
    return stack;
  }  
}
