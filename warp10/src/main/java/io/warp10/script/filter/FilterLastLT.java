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

package io.warp10.script.filter;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterLastLT extends NamedWarpScriptFunction implements WarpScriptFilterFunction {
  
  private final TYPE type;
  private final Object threshold;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object threshold = stack.pop();
      stack.push(new FilterLastLT(getName(), threshold));
      return stack;
    }
  }
  
  public FilterLastLT(String name, Object threshold) throws WarpScriptException {
    super(name);
    if (threshold instanceof Long) {
      this.type = TYPE.LONG;
      this.threshold = threshold;
    } else if (threshold instanceof Double) {
      this.type = TYPE.DOUBLE;
      this.threshold = threshold;      
    } else if (threshold instanceof String) {
      this.type = TYPE.STRING;
      this.threshold = threshold;      
    } else {
      throw new WarpScriptException("Invalid threshold type.");
    }
  }

  @Override
  public List<GeoTimeSerie> filter(Map<String,String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {
    List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> gtsinstances: series) {
      for (GeoTimeSerie serie: gtsinstances) {
        Object last = GTSHelper.getLastValue(serie);
        
        if (null == last) {
          continue;
        }
        
        switch (type) {
          case LONG:
            if (((Long) threshold).compareTo(((Number) last).longValue()) > 0) {
              retained.add(serie);
            }
            break;
          case DOUBLE:
            if (((Double) threshold).compareTo(((Number) last).doubleValue()) > 0) {
              retained.add(serie);
            }
            break;
          case STRING:
            if (((String) threshold).compareTo(last.toString()) > 0) {
              retained.add(serie);
            }
            break;
        }
      }      
    }
    
    return retained;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.threshold));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
