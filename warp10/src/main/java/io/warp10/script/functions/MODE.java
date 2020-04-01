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
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Compute the mode(s) for a given GTS
 * 
 */
public class MODE extends GTSStackFunction {
  
  public MODE(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    return null;
  }
  
  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    List<Object> modes = new ArrayList<Object>();

    int n = GTSHelper.nvalues(gts);

    if (0 == n) {
      return modes;
    }

    //
    // Sort the GTS by values
    //
    
    GTSHelper.valueSort(gts);
    
    int count = 0;
    int modeCount = 0;
        
    Object lastValue = null;
    
    for (int i = 0; i < n; i++) {
      Object value = GTSHelper.valueAtIndex(gts, i);
      if (null == lastValue) {
        count = 1;
        lastValue = value;
      } else if (lastValue != value) {
        if (count > modeCount) {
          modeCount = count;
          modes.clear();
          modes.add(lastValue);
        } else if (count == modeCount) {
          modes.add(lastValue);
        }
        count = 1;          
        lastValue = value;
      } else {
        count++;
      }
      if (n - 1 == i) {
        if (count > modeCount) {
          modeCount = count;
          modes.clear();
          modes.add(lastValue);
        } else if (count == modeCount) {
          modes.add(lastValue);
        }        
      }
    }
    
    return modes;
  }
}
