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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Partitions a set of GTS instances
 */
public class PARTITION extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean strict;
  
  public PARTITION(String name) {
    super(name);
    strict = false;
  }

  public PARTITION(String name, boolean strict) {
    super(name);
    this.strict = strict;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    List<String> bylabels = null;
    
    try {
      bylabels = (List<String>) top;
    } catch (ClassCastException cce) {
      throw new WarpScriptException(getName() + " expects a list of labels or null on the top of the stack.", cce);
    }
    
    top = stack.pop();
    
    List<Object> params = null;
    
    try {
      params = (List<Object>) top;
    } catch (ClassCastException cce) {
      throw new WarpScriptException(getName() + " expects a list of Geo Time Series instances under the top of the stack.", cce);
    }

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    for (int i = 0; i < params.size(); i++) {      
      if (params.get(i) instanceof GeoTimeSerie) {
        series.add((GeoTimeSerie) params.get(i));
      } else if (params.get(i) instanceof List) {
        for (Object o: (List) params.get(i)) {
          if (!(o instanceof GeoTimeSerie)) {
            throw new WarpScriptException(getName() + " expects a list of Geo Time Series as first parameter.");
          }
          series.add((GeoTimeSerie) o);
        }      
      }      
    }
    
    Map<Map<String,String>, List<GeoTimeSerie>> eqclasses = GTSHelper.partition(series, bylabels);
    
    if (this.strict && null != bylabels) {
      Map<Map<String,String>,List<GeoTimeSerie>> stricteqclasses = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
      
      for (Entry<Map<String,String>, List<GeoTimeSerie>> entry: eqclasses.entrySet()) {
        Map<String,String> key = entry.getKey();
        Map<String,String> newkey = new HashMap<String,String>();
        for (String label: bylabels) {
          if (null != key.get(label)) {
            newkey.put(label, key.get(label));
          }
        }
        stricteqclasses.put(newkey, entry.getValue());
      }
      
      stack.push(stricteqclasses);
    } else {
      stack.push(eqclasses);
    }
    
    return stack;
  }
}
