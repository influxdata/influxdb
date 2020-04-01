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
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filter GTS by retaining those whose metadata is included in a set
 */
public class FilterByMetadata extends NamedWarpScriptFunction implements WarpScriptFilterFunction {
  
  private final Set<Metadata> metadatas;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
        
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object arg = stack.pop();
      
      if (!(arg instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of GTS on top of the stack.");
      }
      
      for (Object o: (List) arg) {
        if (!(o instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " expects a list of GTS on top of the stack.");
        }
      }
      
      stack.push(new FilterByMetadata(getName(), (List<GeoTimeSerie>) arg));
      return stack;
    }
  }
  
  public FilterByMetadata(String name, List<GeoTimeSerie> selector) {
    super(name);
    
    this.metadatas = new HashSet<Metadata>();
    
    for (GeoTimeSerie gts: selector) {
      this.metadatas.add(gts.getMetadata());
    }
  }
  
  @Override  
  public List<GeoTimeSerie> filter(Map<String,String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {
    
    List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> serie: series) {
      for (GeoTimeSerie gts: serie) {
        Metadata meta = gts.getMetadata();
        
        if (this.metadatas.contains(meta)) {
          retained.add(gts);
        }        
      }
    }
    
    return retained;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    
    sb.append(WarpScriptLib.LIST_START);
    sb.append(" ");
    for (Metadata metadata: this.metadatas) {
      sb.append(StackUtils.toString(GTSHelper.buildSelector(metadata, false)));
      sb.append(" ");
      sb.append(WarpScriptLib.PARSESELECTOR);
      sb.append(" ");
      sb.append(WarpScriptLib.NEWGTS);
      sb.append(" ");
      sb.append(WarpScriptLib.SWAP);
      sb.append(" ");
      sb.append(WarpScriptLib.RELABEL);
      sb.append(" ");
      sb.append(WarpScriptLib.SWAP);
      sb.append(" ");
      sb.append(WarpScriptLib.RENAME);
      sb.append(" ");
    }
    sb.append(WarpScriptLib.LIST_END);
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
