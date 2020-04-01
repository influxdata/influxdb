//
//   Copyright 2019  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Build a histogram of value occurrences
 */
public class VALUEHISTOGRAM extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public VALUEHISTOGRAM(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (top instanceof GeoTimeSerie) {
      stack.push(GTSHelper.valueHistogram((GeoTimeSerie) top));
    } else if (top instanceof GTSEncoder) {
      stack.push(GTSHelper.valueHistogram((GTSEncoder) top));
    } else if (top instanceof List) {
      List<Object> histograms = new ArrayList<Object>();
      
      for (Object o: (List<Object>) top) {
        if (o instanceof GeoTimeSerie) {
          histograms.add(GTSHelper.valueHistogram((GeoTimeSerie) o));          
        } else if (o instanceof GTSEncoder) {
          histograms.add(GTSHelper.valueHistogram((GTSEncoder) o));
        } else {
          stack.push(top);
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series™ or GTS Encoder instances.");
        }
      }
      stack.push(histograms);
    } else {
      stack.push(top);
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
    }
    
    return stack;
  }
}
