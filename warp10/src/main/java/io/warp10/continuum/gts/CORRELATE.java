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

import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes auto or cross correlation of GTS instances
 */

public class CORRELATE extends NamedWarpScriptFunction {
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction  {
    
    private final CORRELATE correlate;
    
    public Builder(String name) {
      super(name);
      correlate = new CORRELATE(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
            
      Object top = stack.pop();
      
      if (!(top instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of offsets on the top of the stack.");
      }
      
      List<Long> offsets = new ArrayList<Long>();
      
      for (Object o: ((List) top)) {
        offsets.add(((Number) o).longValue());
      }
      
      top = stack.pop();
      
      List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
      
      if (top instanceof GeoTimeSerie) {
        series.add((GeoTimeSerie) top);
      } else if (top instanceof List) {
        for (Object o: (List<Object>) top) {
          if (! (o instanceof GeoTimeSerie)) {
            stack.push(top);
            throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
          }
          series.add((GeoTimeSerie) o);
        }
      } else {
        stack.push(top);
        throw new WarpScriptException(getName() + " can only operate on Geo Time Series instances.");
      }

      top = stack.pop();
      
      if (!(top instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " expects a Geo Time Series two levels below the top of the stack.");
      }
      
      stack.push(correlate.correlate((GeoTimeSerie) top, series, offsets));
      return stack;
    }
  }  

  public CORRELATE(String name) {
    super(name);
  }
  
  public List<GeoTimeSerie> correlate(GeoTimeSerie gts, List<GeoTimeSerie> gts2, List<Long> offsets) throws WarpScriptException {
    
    //
    // Check that all GTS instances are bucketized with the same bucketspan
    //
    
    if (!GTSHelper.isBucketized(gts) || gts.values != gts.bucketcount || (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type)) {
      throw new WarpScriptException(getName() + " operates on bucketized, filled numeric Geo Time Series.");
    }
    
    long bucketspan = gts.bucketspan;
    
    for (GeoTimeSerie g: gts2) {
      long bs = g.bucketspan;
      
      if (!GTSHelper.isBucketized(g) || g.values != g.bucketcount || (TYPE.DOUBLE != g.type && TYPE.LONG != g.type)) {
        throw new WarpScriptException(getName() + " operates on bucketized, filled numeric Geo Time Series.");
      }
      
      if (bs != bucketspan) {
        throw new WarpScriptException(getName() + " operates on bucketized Geo Time Series with all the same bucketspan. The expected bucketspan is " + bucketspan);
      }
    }
    
    //
    // Check that the offsets are multiple of the bucketspan
    //
    
    for (long offset: offsets) {
      if (offset % bucketspan != 0) {
        throw new WarpScriptException(getName() + " expects offsets to be multiples of the bucketspan (" + bucketspan + ").");
      }
    }
    
    //
    // Standardize the first GTS instance
    // 
    
    //gts = GTSHelper.standardize(gts);
    GTSHelper.sort(gts);

    //
    // Now compute the correlations for all pairs
    //
    
    List<GeoTimeSerie> crosscorrelations = new ArrayList<GeoTimeSerie>();
    
    for (int i = 0; i < gts2.size(); i++) {
      
      //
      // Standardize and sort GTS
      //
      
      GeoTimeSerie standardized = GTSHelper.standardize(gts2.get(i));
      GTSHelper.sort(standardized);

      GeoTimeSerie crosscorrelation = new GeoTimeSerie(offsets.size());      
      crosscorrelation.setMetadata(standardized.getMetadata());
      crosscorrelations.add(crosscorrelation);
      
      for (long offset: offsets) {
        
        //
        // Extract both GTS to check
        //
        
        GeoTimeSerie gtsA = gts;
        GeoTimeSerie gtsB = gts2.get(i);
        GTSHelper.sort(gtsB);
        
        //
        // Find the index offset to use
        //
        
        int idxA = 0;
        int idxB = 0;
        
        while (idxA < gtsA.values && idxB < gtsB.values) {
          if (gtsA.ticks[idxA] + offset < gtsB.ticks[idxB]) {
            idxA++;
            continue;
          }
          if (gtsA.ticks[idxA] + offset > gtsB.ticks[idxB]) {
            idxB++;
            continue;
          }
          break;
        }
        
        if (idxA == gtsA.values || idxB == gtsB.values) {
          GTSHelper.setValue(crosscorrelation, offset, 0.0D);          
          continue;
        }
        
        //
        // Clip both GTS to the useful range
        //
        
        long lastTickB = gtsB.ticks[gtsB.values - 1];
        long lastTickA = gtsA.ticks[gtsA.values - 1];
        
        long end = Math.min(lastTickA, lastTickB - offset);
        gtsA = GTSHelper.timeclip(gtsA, gtsA.ticks[idxA], end);
        gtsB = GTSHelper.timeclip(gtsB, gtsB.ticks[idxB], end + offset);

        gtsA = GTSHelper.standardize(gtsA);
        gtsB = GTSHelper.standardize(gtsB);
        
        idxA = 0;
        idxB = 0;
        
        double sum = 0.0D; 
        int count = 0;
        
        while (idxA < gtsA.values && idxB < gtsB.values) {
          double v0 = ((Number) GTSHelper.valueAtIndex(gtsA, idxA)).doubleValue();
          Object v1 = GTSHelper.valueAtIndex(gtsB, idxB);
          
          if (null != v1) {
            double v1d = ((Number) v1).doubleValue();          
            
            sum += v0 * v1d;
            count++;
          }
          idxA++;
          idxB++;
        }
        
        if (count > 1) {
          GTSHelper.setValue(crosscorrelation, offset, sum / (double) (count - 1));
        } else if (count > 0) {
          GTSHelper.setValue(crosscorrelation, offset, sum / (double) count);
        } else {
          GTSHelper.setValue(crosscorrelation, offset, 0.0D);
        }
      }
    }
    
    return crosscorrelations;
  }
}
