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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterByClass extends NamedWarpScriptFunction implements WarpScriptFilterFunction {
  
  private final String selParam;
  private final Pattern selector;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object arg = stack.pop();
      if (!(arg instanceof String)) {
        throw new WarpScriptException("Invalid class selector");
      }
      stack.push(new FilterByClass(getName(), (String) arg));
      return stack;
    }
  }
  
  public FilterByClass(String name, String selector) {
    super(name);
    this.selParam = selector;
    if (selector.startsWith("=")) {
      this.selector = Pattern.compile(Pattern.quote(selector.substring(1)));
    } else if (selector.startsWith("~")) {
      this.selector = Pattern.compile(selector.substring(1));
    } else {
      this.selector = Pattern.compile(Pattern.quote(selector));
    }
  }
  
  @Override  
  public List<GeoTimeSerie> filter(Map<String,String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {
    
    List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();
    
    Matcher m = this.selector.matcher("");
    
    for (List<GeoTimeSerie> serie: series) {
      for (GeoTimeSerie gts: serie) {
        if (m.reset(gts.getName()).matches()) {
          retained.add(gts);
        }        
      }
    }
    
    return retained;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.selParam));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
