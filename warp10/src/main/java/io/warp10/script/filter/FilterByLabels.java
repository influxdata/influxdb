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
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterByLabels extends NamedWarpScriptFunction implements WarpScriptFilterFunction {
  
  private final Map<String,String> selParam;
  private final Map<String,Pattern> selectors;
  
  private final boolean checkLabels;
  private final boolean checkAttributes;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    private final boolean checkLabels;
    private final boolean checkAttributes;
    
    public Builder(String name, boolean checkLabels, boolean checkAttribtues) {
      super(name);
      this.checkLabels = checkLabels;
      this.checkAttributes = checkAttribtues;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object arg = stack.pop();
      if (!(arg instanceof Map<?,?>)) {
        throw new WarpScriptException("Invalid labels/attributes selector");
      }
      stack.push(new FilterByLabels(getName(), (Map<String,String>) arg, checkLabels, checkAttributes));
      return stack;
    }
  }

  public FilterByLabels(String name, Map<String,String> selectors, boolean checkLabels, boolean checkAttributes) {
    
    super(name);
    
    this.checkLabels = checkLabels;
    this.checkAttributes = checkAttributes;
    
    this.selParam = selectors;
    this.selectors = new HashMap<String, Pattern>();
    
    for (Entry<String,String> entry: selectors.entrySet()) {
      String lname = entry.getKey();
      String selector = entry.getValue();
      Pattern pattern;
      
      if (selector.startsWith("=")) {
        pattern = Pattern.compile(Pattern.quote(selector.substring(1)));
      } else if (selector.startsWith("~")) {
        pattern = Pattern.compile(selector.substring(1));
      } else {
        pattern = Pattern.compile(Pattern.quote(selector));
      }
      
      this.selectors.put(lname, pattern);
    }
  }
  
  @Override
  public List<GeoTimeSerie> filter(Map<String,String> commonlabels, List<GeoTimeSerie>... series) throws WarpScriptException {
    
    List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();
    
    Map<String,Matcher> matchers = new HashMap<String,Matcher>();
    
    for (Entry<String,Pattern> entry: this.selectors.entrySet()) {
      matchers.put(entry.getKey(), entry.getValue().matcher(""));
    }
    
    for (List<GeoTimeSerie> serie: series) {
      for (GeoTimeSerie gts: serie) {
        
        boolean matched = true;

        Map<String,String> labels = gts.getMetadata().getLabels();        
        Map<String,String> attributes = gts.getMetadata().getAttributes();
        
        for (Map.Entry<String, Matcher> labelAndMatcher: matchers.entrySet()) {
          String label = labelAndMatcher.getKey();
          Matcher matcher = labelAndMatcher.getValue();
          
          boolean hasLabel = false;
          
          // Check if labels or attributes contain the given key
          if (checkLabels) {
            if (labels.containsKey(label)) {
              hasLabel = true;
            }
          }
          
          // If the GTS does not have the named label, check if it has a named attribute
          if (!hasLabel && checkAttributes) {
            if (!attributes.containsKey(label)) {
              matched = false;
            }
          } else {
            matched = hasLabel;
          }
          
          if (!matched) {
            break;
          }

          // Now check the label or attribute value
          if (hasLabel && checkLabels) {
            if (!matcher.reset(labels.get(label)).matches()) {
              matched = false;
              break;
            }            
          } else if (checkAttributes) {
            if (!matcher.reset(attributes.get(label)).matches()) {
              matched = false;
              break;
            }
          }          
        }          
        
        if (matched) {
          retained.add(gts);
        }        
      }
    }
    
    return retained;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(WarpScriptLib.MAP_START);
    sb.append(" ");
    for (Entry<String,String> entry: this.selParam.entrySet()) {
      sb.append(StackUtils.toString(entry.getKey()));
      sb.append(" ");
      sb.append(StackUtils.toString(entry.getValue()));
    }
    sb.append(WarpScriptLib.MAP_END);
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
