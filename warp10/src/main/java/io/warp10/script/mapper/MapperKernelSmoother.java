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

package io.warp10.script.mapper;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;

public class MapperKernelSmoother extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {

  private final long step;
  private final long width;
  private final double[] weights;
  
  public MapperKernelSmoother(String name, long step, long width, double[] weights) {
    super(name);
    this.step = step;
    this.width = width;
    this.weights = weights;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    
    long tick = (long) args[0];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    //
    // Compute Nadaraya-Watson kernel-weighted average
    //
    
    double weightedValue = 0.0D;
    double weightedLatitude = 0.0D;
    double weightedLongitude = 0.0D;
    double weightedElevation = 0.0D;
    
    double valueDividend = 0.0D;
    double locationDividend = 0.0D;
    double elevationDividend = 0.0D;
    
    for (int i = 0; i < ticks.length; i++) {
      int idx = (int) (Math.abs(ticks[i] - tick) / step);
      
      if (idx < this.weights.length && values[i] instanceof Number) {
        weightedValue += this.weights[idx] * ((Number) values[i]).doubleValue();
        valueDividend += this.weights[idx];
        
        if (GeoTimeSerie.NO_LOCATION != locations[i]) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(locations[i]);
          weightedLatitude += this.weights[idx] * latlon[0];
          weightedLongitude += this.weights[idx] * latlon[1];
          locationDividend += this.weights[idx];
        }
        
        if (GeoTimeSerie.NO_ELEVATION != elevations[i]) {
          weightedElevation += this.weights[idx] * elevations[i];
          elevationDividend += this.weights[idx];
        }
      }
    }
    
    long elevation = GeoTimeSerie.NO_ELEVATION;
    long location = GeoTimeSerie.NO_LOCATION;
    Object value = null;
    
    if (0.0D != valueDividend) {
      value = weightedValue / valueDividend;
        
      if (0.0D != elevationDividend) {
        elevation = (long) (weightedElevation / elevationDividend);
      }
            
      if (0.0D != locationDividend) {
        location = GeoXPLib.toGeoXPPoint(weightedLatitude / locationDividend, weightedLongitude / locationDividend);
      }
    }
    
    return new Object[] { tick, location, elevation, value };
  }
  
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(step));
    sb.append(" ");
    sb.append(StackUtils.toString(width));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
