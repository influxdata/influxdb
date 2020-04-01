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

package io.warp10.continuum.gts;

import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

import com.geoxp.GeoXPLib;

/**
 * Only retain values more than X meters from the previous one.
 * Retain at least first and last value.
 */
public class LOCATIONOFFSET extends GTSStackFunction {
  
  private static final String DIST = "dist";
  
  public LOCATIONOFFSET(String name) {
    super(name);
  }
  
  @Override
  protected Map<String,Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a distance in meters on top of the stack.");
    }
    
    Map<String,Object> params = new HashMap<String,Object>();
    
    params.put(DIST, ((Number) o).doubleValue());
    
    return params;
  }
  
  @Override
  protected GeoTimeSerie gtsOp(Map<String,Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    double dist = (double) params.get(DIST);
    
    GeoTimeSerie offsetGTS = gts.cloneEmpty();
    
    //
    // Sort GTS
    //
    
    GTSHelper.sort(gts);
    
    if (0 == gts.values) {
      return offsetGTS;
    }

    long lastlocation = GTSHelper.locationAtIndex(gts, 0);

    GTSHelper.setValue(offsetGTS, GTSHelper.tickAtIndex(gts, 0), lastlocation, GTSHelper.elevationAtIndex(gts, 0), GTSHelper.valueAtIndex(gts, 0), false);
    
    for (int i = 1; i < gts.values - 1; i++) {
      long location = GTSHelper.locationAtIndex(gts, i);
      
      if (GeoTimeSerie.NO_LOCATION == location) {
        continue;
      }
      
      //
      // If this is the first value with a location, copy it
      //
      
      if (GeoTimeSerie.NO_LOCATION == lastlocation) {
        lastlocation = location;
        GTSHelper.setValue(offsetGTS, GTSHelper.tickAtIndex(gts, i), lastlocation, GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
        continue;
      }
      
      //
      // Compute distance
      //
      
      double distance = GeoXPLib.loxodromicDistance(lastlocation, location);
    
      //
      // Add value if its distance from previous one is >= 'dist'
      //
      
      if (distance >= dist) {
        lastlocation = location;
        GTSHelper.setValue(offsetGTS, GTSHelper.tickAtIndex(gts, i), lastlocation, GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);        
      }
    }

    //
    // Add the last value
    //
    
    if (gts.values > 1) {
      GTSHelper.setValue(offsetGTS, GTSHelper.tickAtIndex(gts, gts.values - 1), GTSHelper.locationAtIndex(gts, gts.values - 1), GTSHelper.elevationAtIndex(gts, gts.values - 1), GTSHelper.valueAtIndex(gts, gts.values - 1), false);
    }
    
    return offsetGTS;
  }
}
