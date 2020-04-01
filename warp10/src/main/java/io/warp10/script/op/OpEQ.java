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

package io.warp10.script.op;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Checks values from N time series for equality. The elevation and location are cleared.
 */
public class OpEQ extends NamedWarpScriptFunction implements WarpScriptNAryFunction {
  
  public OpEQ(String name) {
    super(name);
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    String[] names = (String[]) args[1];
    Map<String,String>[] labels = (Map<String,String>[]) args[2];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    //
    // The type of result is determined by the first non null value
    //
            
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    Set<Object> vals = new HashSet<Object>();
    
    for (int i = 0; i < values.length; i++) {      
      // If one of the values is 'null' (absent), return null as the value
      if (null == values[i]) {
        return new Object[] { tick, location, elevation, false };
      }
      // Add first value to the 'vals' set
      if (vals.isEmpty()) {
        vals.add(values[i]);
        continue;
      }
      // If 'vals' does not contain the current value it means it differs from the previous one
      if (!vals.contains(values[i])) {
        return new Object[] { tick, location, elevation, false };
      }
    }
    
    return new Object[] { tick, location, elevation, true };
  }
}
