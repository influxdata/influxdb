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

/**
 * Fills the gaps by interpolating using both pre and post trends
 */
public class FillerTrend extends NamedWarpScriptFunction implements WarpScriptFillerFunction {
  
  public FillerTrend(String name) {
    super(name);
  }
  
  @Override
  public Object[] apply(Object[] args) throws WarpScriptException {
    
    Object[] results = new Object[4];
    
    Object[] prevprev = (Object[]) args[1];
    Object[] prev = (Object[]) args[2];
    Object[] other = (Object[]) args[3];
    Object[] next = (Object[]) args[4];
    Object[] nextnext = (Object[]) args[5];

    long tick = ((Number) other[0]).longValue();
    
    //
    // Compute the 'pre' trend
    //
    
    double prerate = Double.NaN;
    
    if (null != prevprev[3] && null != prev[3]) {
      // We compute the rate between the two previous datapoints
      prerate = (((Number) prev[3]).doubleValue() - ((Number) prevprev[3]).doubleValue()) / (((Number) prev[0]).longValue() - ((Number) prevprev[0]).longValue());
    } else if (null != prev[3] && null != next[3]) {
      // We compute the rate between the previous and next datapoints
      prerate = (((Number) next[3]).doubleValue() - ((Number) prev[3]).doubleValue()) / (((Number) next[0]).longValue() - ((Number) prev[0]).longValue()); 
    } else if (null != next[3] && null != nextnext[3]) {
      // We compute the rate between the next two datapoints
      prerate = (((Number) nextnext[3]).doubleValue() - ((Number) next[3]).doubleValue()) / (((Number) nextnext[0]).longValue() - ((Number) next[0]).longValue());
    } else {
      prerate = Double.NaN;
    }
    
    //
    // Compute the 'post' trend    
    //
    
    double postrate = Double.NaN;
    
    if (null != nextnext[3] && null != next[3]) {
      postrate = (((Number) nextnext[3]).doubleValue() - ((Number) next[3]).doubleValue()) / (((Number) nextnext[0]).longValue() - ((Number) next[0]).longValue());
    } else  if (null != prev[3] && null != next[3]) {
      // We compute the rate between the previous and next datapoints
      postrate = (((Number) next[3]).doubleValue() - ((Number) prev[3]).doubleValue()) / (((Number) next[0]).longValue() - ((Number) prev[0]).longValue()); 
    } else if (null != prevprev[3] && null != prev[3]) {
      // We compute the rate between the two previous datapoints
      postrate = (((Number) prev[3]).doubleValue() - ((Number) prevprev[3]).doubleValue()) / (((Number) prev[0]).longValue() - ((Number) prevprev[0]).longValue());
    } else {
      postrate = Double.NaN;
    }

    if (Double.isNaN(prerate) && !Double.isNaN(postrate)) {
      prerate = postrate;
    }
    
    if (Double.isNaN(postrate) && !Double.isNaN(prerate)) {
      postrate = prerate;
    }
    
    if (Double.isNaN(postrate) || Double.isNaN(prerate)) {
      return results;
    }

    //
    // Compute the interpolated value
    //
    
    if (null != next[0] && null != prev[0]) {
      // We will compute the average of the projected previous and next datapoints
      long span = ((Number) next[0]).longValue() - ((Number) prev[0]).longValue();
      long delta = ((Number) other[0]).longValue() - ((Number) prev[0]).longValue();
      double alpha = (double) delta / span;
      
      double projectedPrevious = ((Number) prev[3]).doubleValue() + delta * prerate;
      double projectedNext = ((Number) next[3]).doubleValue() - (span - delta) * postrate;
      
      results[3] = alpha * projectedPrevious + (1.0D - alpha) * projectedNext;            
    } else if (null != prev[0]) {
      long delta = ((Number) other[0]).longValue() - ((Number) prev[0]).longValue();
      
      results[3] = ((Number) prev[3]).doubleValue() + prerate * delta;
    } else if (null != next[0]) {
      long delta = ((Number) next[0]).longValue() - ((Number) other[0]).longValue();

      results[3] = ((Number) next[3]).doubleValue() - postrate * delta;
    }
    
    //
    // Determine the interpolated lat/lon and elevation via a
    // recursive call
    //            

    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    // Save the current loc/elev values
    
    long prevloc = null == prev[1] ? GeoTimeSerie.NO_LOCATION : ((Number) prev[1]).longValue();
    long prevelev = null == prev[2] ? GeoTimeSerie.NO_ELEVATION : ((Number) prev[2]).longValue();
    long prevprevloc = null == prevprev[1] ? GeoTimeSerie.NO_LOCATION : ((Number) prevprev[1]).longValue();
    long prevprevelev = null == prevprev[2] ? GeoTimeSerie.NO_ELEVATION : ((Number) prevprev[2]).longValue();
    long nextloc = null == next[1] ? GeoTimeSerie.NO_LOCATION : ((Number) next[1]).longValue();
    long nextelev = null == next[2] ? GeoTimeSerie.NO_ELEVATION : ((Number) next[2]).longValue();
    long nextnextloc = null == nextnext[1] ? GeoTimeSerie.NO_LOCATION : ((Number) nextnext[1]).longValue();
    long nextnextelev = null == nextnext[2] ? GeoTimeSerie.NO_ELEVATION : ((Number) nextnext[2]).longValue();

    prev[1] = GeoTimeSerie.NO_LOCATION;
    prev[2] = GeoTimeSerie.NO_ELEVATION;
    prev[3] = null;
    prevprev[1] = GeoTimeSerie.NO_LOCATION;
    prevprev[2] = GeoTimeSerie.NO_ELEVATION;
    prevprev[3] = null;
    next[1] = GeoTimeSerie.NO_LOCATION;
    next[2] = GeoTimeSerie.NO_ELEVATION;
    next[3] = null;
    nextnext[1] = GeoTimeSerie.NO_LOCATION;
    nextnext[2] = GeoTimeSerie.NO_ELEVATION;
    nextnext[3] = null;

    if (GeoTimeSerie.NO_LOCATION != prevloc) {
      double[] latlon = GeoXPLib.fromGeoXPPoint(prevloc);
      prev[3] = latlon[0];
    }
      
    if (GeoTimeSerie.NO_LOCATION != nextloc) {
      double[] latlon = GeoXPLib.fromGeoXPPoint(nextloc);
      next[3] = latlon[0];
    }
      
    if (GeoTimeSerie.NO_LOCATION != prevprevloc) {
      double[] latlon = GeoXPLib.fromGeoXPPoint(prevprevloc);
      prevprev[3] = latlon[0];
    }
      
    if (GeoTimeSerie.NO_LOCATION != nextnextloc) {
      double[] latlon = GeoXPLib.fromGeoXPPoint(nextnextloc);
      nextnext[3] = latlon[0];
    }

    Double lat = null;
    Double lon = null;
    
    Object[] llres = this.apply(args);
    lat = (Double) llres[3];

    if (null != lat) {
      prev[3] = null;
      prevprev[3] = null;
      next[3] = null;
      nextnext[3] = null;

      if (GeoTimeSerie.NO_LOCATION != prevloc) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(prevloc);
        prev[3] = latlon[1];
      }
        
      if (GeoTimeSerie.NO_LOCATION != nextloc) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(nextloc);
        next[3] = latlon[1];
      }
        
      if (GeoTimeSerie.NO_LOCATION != prevprevloc) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(prevprevloc);
        prevprev[3] = latlon[1];
      }
        
      if (GeoTimeSerie.NO_LOCATION != nextnextloc) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(nextnextloc);
        nextnext[3] = latlon[1];
      }

      llres = this.apply(args);
      lon = (Double) llres[3];
      
      if (null != lon) {
        location = GeoXPLib.toGeoXPPoint(lat, lon);
      }
    }

    prev[3] = GeoTimeSerie.NO_ELEVATION != prevelev ? prevelev : null;
    prevprev[3] = GeoTimeSerie.NO_ELEVATION != prevprevelev ? prevprevelev : null;
    next[3] = GeoTimeSerie.NO_ELEVATION != nextelev ? nextelev : null;
    nextnext[3] = GeoTimeSerie.NO_ELEVATION != nextnextelev ? nextnextelev : null;

    Object[] elevres = this.apply(args);
    
    if (null != elevres[3]) {
      location = Math.round(((Number) elevres[3]).doubleValue());
    }
    
    results[0] = tick;
    results[1] = location;
    results[2] = elevation;
    
    return results;
  }
  
  @Override
  public int getPreWindow() {
    return 2;
  }
  
  @Override
  public int getPostWindow() {
    return 2;
  }
}
