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

package io.warp10.script.filler;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;

public class FillerInterpolate extends NamedWarpScriptFunction implements WarpScriptFillerFunction {
  
  public FillerInterpolate(String name) {
    super(name);
  }
  
  @Override
  public Object[] apply(Object[] args) throws WarpScriptException {
    
    Object[] results = new Object[4];
    
    Object[] prev = (Object[]) args[1];
    Object[] other = (Object[]) args[2];
    Object[] next = (Object[]) args[3];

    // We cannot interpolate on the edges
    if (null == prev[0] || null == next[0]) {
      return results;
    }

    long tick = ((Number) other[0]).longValue();
    
    long prevtick = ((Number) prev[0]).longValue();
    long prevloc = ((Number) prev[1]).longValue();
    long prevelev = ((Number) prev[2]).longValue();
    Object prevvalue = prev[3];

    long nexttick = ((Number) next[0]).longValue();
    long nextloc = ((Number) next[1]).longValue();
    long nextelev = ((Number) next[2]).longValue();
    Object nextvalue = next[3];

    // We cannot interpolate STRING or BOOLEAN values
    if (prevvalue instanceof String || prevvalue instanceof Boolean) {
      return results;
    }
    
    //
    // Compute the interpolated value
    //
    
    long span = nexttick - prevtick;
    long delta = tick - prevtick;
    double rate = (((Number) nextvalue).doubleValue() - ((Number) prevvalue).doubleValue())/span;
    
    double interpolated = ((Number) prevvalue).doubleValue() + rate * delta;
    
    if (prevvalue instanceof Long) {
      results[3] = (long) Math.round(interpolated);
    } else {
      results[3] = interpolated;
    }
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (GeoTimeSerie.NO_LOCATION != prevloc && GeoTimeSerie.NO_LOCATION != nextloc) {
      double[] prevlatlon = GeoXPLib.fromGeoXPPoint(prevloc);
      double[] nextlatlon = GeoXPLib.fromGeoXPPoint(nextloc);
      
      double lat = prevlatlon[0] + delta * ((nextlatlon[0] - prevlatlon[0]) / span);
      double lon = prevlatlon[1] + delta * ((nextlatlon[1] - prevlatlon[1]) / span);
      
      location = GeoXPLib.toGeoXPPoint(lat, lon);
    }
    
    if (GeoTimeSerie.NO_ELEVATION != prevelev && GeoTimeSerie.NO_ELEVATION != nextelev) {
      elevation = (long) Math.round(prevelev + delta * ((nextelev - prevelev) / (double) span));
    }
    
    results[0] = tick;
    results[1] = location;
    results[2] = elevation;
    
    return results;
  }
  
  @Override
  public int getPreWindow() {
    return 1;
  }
  
  @Override
  public int getPostWindow() {
    return 1;
  }
}
