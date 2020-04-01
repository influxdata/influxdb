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
 * Computes an inverse FFT given Re/Im or Amp/Phase pseudo GTS
 * 
 * Code implements Cooley-Tuckey radix-2, inspired by the following:
 * @see http://introcs.cs.princeton.edu/java/97data/FFT.java.html
 */
public class IFFT {
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction  {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object im = stack.pop();
      
      if (!(im instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " expects a Geo Time Series of imaginary parts on top of the stack.");
      }
      
      Object re = stack.pop();
      
      if (!(re instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " expects a Geo Time Series of real parts below the imaginary parts GTS.");
      }
      
      if (TYPE.DOUBLE != ((GeoTimeSerie) re).getType() || TYPE.DOUBLE != ((GeoTimeSerie) im).getType()) {
        throw new WarpScriptException(getName() + " real and imaginary parts GTS MUST be of type DOUBLE.");
      }
      
      stack.push(ifft((GeoTimeSerie) re, (GeoTimeSerie) im));
      
      return stack;
    }
  }
  
  public static GeoTimeSerie ifft(GeoTimeSerie re, GeoTimeSerie im) throws WarpScriptException {
    
    GTSHelper.sort(re);
    GTSHelper.sort(im);
    
    if (GTSHelper.nvalues(re) != GTSHelper.nvalues(im)) {
      throw new WarpScriptException("Real and imaginary Geo Time Series MUST have the same number of values.");
    }

    int n = GTSHelper.nvalues(re);
    double[] x = new double[n + n];

    for (int i = 0; i < n; i++) {
      x[2 * i] = ((Number) GTSHelper.valueAtIndex(re, i)).doubleValue();
      x[2 * i + 1] = ((Number) GTSHelper.valueAtIndex(im, i)).doubleValue();
    }
    
    DoubleFFT_1D dfft = new DoubleFFT_1D(n);
    dfft.complexInverse(x, true);
    
    GeoTimeSerie gts = new GeoTimeSerie(n);
    gts.setName(re.getMetadata().getName());
    gts.setLabels(re.getLabels());
    
    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, x[2*i]);
    }
    
    return gts;
  }
}
