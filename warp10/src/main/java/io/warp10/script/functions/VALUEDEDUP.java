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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Remove datapoints with duplicate values.
 *
 * VALUEDEPUD expects a boolean indicating whether we keep the oldest or most recent datapoint for a given value
 */
public class VALUEDEDUP extends  GTSStackFunction  {

  private static final String PARAM_FIRST = "p";
  
  public VALUEDEDUP(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a boolean on top of the stack.");
    }

    Map<String, Object> params = new HashMap<String, Object>();
    
    params.put(PARAM_FIRST, Boolean.TRUE.equals(o));
    
    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    boolean first = Boolean.TRUE.equals(params.get(PARAM_FIRST));

    //
    // Sort the GTS by values
    //
    
    GTSHelper.valueSort(gts, false);
    GeoTimeSerie dedupped = gts.cloneEmpty();
    
    //
    // Scan the values, retaining the first and last datapoints for a given value
    //
    
    int idx = 0;
    Object currentValue = null;
    int firstOcc = -1;
    int lastOcc = -1;
    
    int n = GTSHelper.nvalues(gts);
    
    while (idx < n) {
      Object value = GTSHelper.valueAtIndex(gts, idx);

      if (null != currentValue && currentValue.equals(value)) {
        lastOcc = idx++;
        continue;
      } else {
        // Store first or last occurrence
        if (null != currentValue) {
          if (first) {
            long location = GTSHelper.locationAtIndex(gts, firstOcc);
            long elevation = GTSHelper.elevationAtIndex(gts, firstOcc);
            long timestamp = GTSHelper.tickAtIndex(gts, firstOcc);
            GTSHelper.setValue(dedupped, timestamp, location, elevation, currentValue, false);            
          } else {
            long location = GTSHelper.locationAtIndex(gts, lastOcc);
            long elevation = GTSHelper.elevationAtIndex(gts, lastOcc);
            long timestamp = GTSHelper.tickAtIndex(gts, lastOcc);
            GTSHelper.setValue(dedupped, timestamp, location, elevation, currentValue, false);
          }
        }
        firstOcc = idx;
        lastOcc = idx++;
        currentValue = value;
        continue;
      }
    }

    if (first) {
      long location = GTSHelper.locationAtIndex(gts, firstOcc);
      long elevation = GTSHelper.elevationAtIndex(gts, firstOcc);
      long timestamp = GTSHelper.tickAtIndex(gts, firstOcc);
      GTSHelper.setValue(dedupped, timestamp, location, elevation, currentValue, false);            
    } else {
      long location = GTSHelper.locationAtIndex(gts, lastOcc);
      long elevation = GTSHelper.elevationAtIndex(gts, lastOcc);
      long timestamp = GTSHelper.tickAtIndex(gts, lastOcc);
      GTSHelper.setValue(dedupped, timestamp, location, elevation, currentValue, false);
    }
    
    return dedupped;
  }
}
