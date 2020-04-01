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
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Bucketizes some GTS instances.
 */
public class BUCKETIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public BUCKETIZE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input.");
    }
    
    List<Object> params = (List<Object>) top;
    
    if (5 > params.size()) {
      throw new WarpScriptException(getName() + " needs a list of at least 5 parameters as input.");
    }
    
    for (int i = 0; i < params.size() - 4; i++) {
      if (!(params.get(i) instanceof GeoTimeSerie) && !(params.get(i) instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of Geo Time Series as first parameter.");
      }      
    }
    
    if (!(params.get(params.size() - 4) instanceof WarpScriptBucketizerFunction) && !(params.get(params.size() - 4) instanceof Macro) && null != params.get(params.size() - 4)) {
      throw new WarpScriptException(getName() + " expects a bucketizer function, a macro, or NULL as fourth to last parameter.");
    }
    
    if (!(params.get(params.size() - 3) instanceof Long) || !(params.get(params.size() - 2) instanceof Long) || !(params.get(params.size() - 1) instanceof Long)) {
      throw new WarpScriptException(getName() + " expects lastbucket, bucketspan and bucketcount as last 3 parameters.");
    }
    
    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    
    for (int i = 0; i < params.size() - 4; i++) {
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
    
    Object bucketizer = params.get(params.size() - 4);
    long lastbucket = (long) params.get(params.size() - 3);
    long bucketspan = (long) params.get(params.size() - 2);
    int bucketcount = (int) ((long) params.get(params.size() - 1));
    
    List<GeoTimeSerie> bucketized = new ArrayList<GeoTimeSerie>();
    
    long maxbuckets = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS);
    
    for (GeoTimeSerie gts: series) {
      GeoTimeSerie b = GTSHelper.bucketize(gts, bucketspan, bucketcount, lastbucket, bucketizer, maxbuckets, bucketizer instanceof Macro ? stack : null);
      
      bucketized.add(b);
    }
    
    stack.push(bucketized);
    return stack;
  }
}
