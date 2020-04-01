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
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;

/**
 * Computes a FFT of a numeric GTS
 * 
 * Code implements Cooley-Tuckey radix-2, inspired by the following:
 * @see http://introcs.cs.princeton.edu/java/97data/FFT.java.html
 */
public class FFT {
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction  {
    
    private final boolean complex;
    
    public Builder(String name, boolean complex) {
      super(name);
      this.complex = complex;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object top = stack.pop();
      
      if (top instanceof GeoTimeSerie) {
        stack.push(fft((GeoTimeSerie) top, this.complex));
      } else if (top instanceof List) {
        List<List<Object>> series = new ArrayList<List<Object>>();
        
        for (Object o: (List<Object>) top) {
          if (! (o instanceof GeoTimeSerie)) {
            stack.push(top);
            throw new WarpScriptException("FFT can only operate on Geo Time Series instances.");
          }
          series.add(fft((GeoTimeSerie) o, this.complex));
        }
        stack.push(series);
      } else {
        stack.push(top);
        throw new WarpScriptException("FFT can only operate on Geo Time Series instances.");
      }
      
      return stack;
    }
  }
  
  public static List<Object> fft(GeoTimeSerie gts, boolean complex) throws WarpScriptException {
    List<Object> fft = new ArrayList<Object>();
    
    //
    // FFT can only be applied to numeric and bucketized GTS instances
    //
    
    if (TYPE.LONG != gts.type && TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("FFT can only be applied to numeric Geo Time Series.");
    }
    
    if (!GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException("FFT can only be applied to bucketized Geo Time Series.");
    }
    
    //
    // It is still possible to create a GTS with values at ticks not at bucket boundaries or with duplicate ticks.
    // This will be checked inline
    //
    
    //
    // Sort the GTS
    //
    
    GTSHelper.sort(gts);
    
    //
    // Allocate two arrays for the real and imaginary parts of the FFT coefficients
    // and one for the initial values
    //
    
    double[] x = new double[gts.values * 2];
    
    long bucketboundary = gts.lastbucket % gts.bucketspan;
    
    long lasttick = Long.MAX_VALUE;
    
    for (int i = 0; i < gts.values; i++) {
      // Check that the tick is a bucket boundary
      if (gts.ticks[i] % gts.bucketspan != bucketboundary) {
        throw new WarpScriptException("Found a tick not on a bucket boundary.");
      }      
      // Check for duplicate ticks
      if (lasttick == gts.ticks[i]) {
        throw new WarpScriptException("Found duplicate tick.");
      }
      if (Long.MAX_VALUE != lasttick && gts.ticks[i] - lasttick != gts.bucketspan) {
        throw new WarpScriptException("Found a displaced tick, should have been 'bucketspan' away from previous.");
      }
      lasttick = gts.ticks[i];
      x[i*2] = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
    }
    
    DoubleFFT_1D dfft = new DoubleFFT_1D(x.length / 2);
    dfft.complexForward(x);

    GeoTimeSerie amplitudeGTS = null;
    GeoTimeSerie phaseGTS = null;    
    GeoTimeSerie reGTS = null;
    GeoTimeSerie imGTS = null;
    
    if (complex) {
      reGTS = new GeoTimeSerie(gts.values);
      reGTS.setName(gts.getName());
      reGTS.setLabels(gts.getLabels());
      imGTS = new GeoTimeSerie(gts.values);
      imGTS.setName(gts.getName());
      imGTS.setLabels(gts.getLabels());            
      fft.add(reGTS);
      fft.add(imGTS);
    } else {
      amplitudeGTS = new GeoTimeSerie(gts.values);
      amplitudeGTS.setName(gts.getName());
      amplitudeGTS.setLabels(gts.getLabels());
      phaseGTS = new GeoTimeSerie(gts.values);
      phaseGTS.setName(gts.getName());
      phaseGTS.setLabels(gts.getLabels());      
      fft.add(amplitudeGTS);
      fft.add(phaseGTS);
    }
    
    int n = gts.values;
    
    for (int k = 0; k < gts.values; k++) {
      if (complex) {
        GTSHelper.setValue(reGTS,  k, x[2*k]);
        GTSHelper.setValue(imGTS, k, x[2*k+1]);
      } else {
        double amp = Math.sqrt(x[2*k] * x[2*k] + x[2*k+1] * x[2*k+1]);
        double phase = Math.atan2(x[2*k+1], x[2*k]);
        
        // Pseudo timestamp is the period associated with coefficient k,
        // i.e. Tk = n/k * bucketspan
        // 0 == k ? gts.bucketspan : (n * gts.bucketspan / k)
        
        GTSHelper.setValue(amplitudeGTS, k, amp);
        GTSHelper.setValue(phaseGTS, k, phase);        
      }
    }
    
    fft.add(((double) Constants.TIME_UNITS_PER_S) / (n * gts.bucketspan));
    
    return fft;
  }
}
