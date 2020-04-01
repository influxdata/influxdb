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
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

/**
 * Apply a windowing function to a GTS or list of GTS, typically prior to applying an FFT
 */
public class FFTWINDOW extends GTSStackFunction {
  
  private static final String WINDOWING_ALGORITHM = "win.alg";
  
  private static final String WINDOW_TRIANGULAR = "triangular";
  private static final String WINDOW_PARZEN = "parzen";
  private static final String WINDOW_WELCH = "welch";
  private static final String WINDOW_SINE = "sine";
  private static final String WINDOW_HANN = "hann";
  private static final String WINDOW_HAMMING = "hamming";
  private static final String WINDOW_BLACKMAN = "blackman";
  private static final String WINDOW_NUTTALL = "nuttall";
  private static final String WINDOW_BLACKMAN_NUTTALL = "blackman-nuttall";
  private static final String WINDOW_BLACKMAN_HARRIS = "blackman-harris";
  private static final String WINDOW_FLATTOP = "flattop";
  private static final String WINDOW_RECTANGULAR = "rectangular";
  
  public FFTWINDOW(String name) {
    super(name);
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String, Object>();
    
    Object window = stack.pop();
    
    params.put(WINDOWING_ALGORITHM, window);
    
    return params;
  }
  
  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    String algorithm = params.get(WINDOWING_ALGORITHM).toString().toLowerCase();
    
    GTSHelper.sort(gts);
    
    int N = gts.size();
    
    GeoTimeSerie windowed = new GeoTimeSerie(GTSHelper.getLastBucket(gts), GTSHelper.getBucketCount(gts), GTSHelper.getBucketSpan(gts), N);
    
    //
    // Check that 'gts' is numerical
    //
    
    if (TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) {
      throw new WarpScriptException("Geo Time Series " + GTSHelper.buildSelector(gts, false) + " is not numeric.");
    }
    
    double window = 0.0D;

    for (int n = 0; n < N; n++) {
      //    
      // @see https://en.wikipedia.org/wiki/Window_function
      //
      if (WINDOW_BLACKMAN.equals(algorithm)) {
        // α = 0.16 (a0 = 0.42, a1 = 0.5, a2 = 0.08)
        window = 0.42D - 0.5D * Math.cos(2.0D * Math.PI * n / (N - 1.0D)) + 0.08 * Math.cos(4.0D * Math.PI * n / (N - 1.0D));
      } else if (WINDOW_BLACKMAN_HARRIS.equals(algorithm)) {
        // a_{0}=0.35875;\quad a_{1}=0.48829;\quad a_{2}=0.14128;\quad a_{3}=0.01168\,
        window = 0.35875D - 0.48829D * Math.cos(2.0D * Math.PI * n / (N - 1.0D)) + 0.14128D * Math.cos(4.0D * Math.PI * n / (N - 1.0D)) - 0.01168D * Math.cos(6.0D * Math.PI * n / (N - 1.0D));
      } else if (WINDOW_BLACKMAN_NUTTALL.equals(algorithm)) {
        // a_{0}=0.3635819;\quad a_{1}=0.4891775;\quad a_{2}=0.1365995;\quad a_{3}=0.0106411\,
        window = 0.3635819D - 0.4891775D * Math.cos(2.0D * Math.PI * n / (N - 1.0D)) + 0.1365995D * Math.cos(4.0D * Math.PI * n / (N - 1.0D)) - 0.0106411D * Math.cos(6.0D * Math.PI * n / (N - 1.0D));
      } else if (WINDOW_FLATTOP.equals(algorithm)) {
        // a_{0}=1;\quad a_{1}=1.93;\quad a_{2}=1.29;\quad a_{3}=0.388;\quad a_{4}=0.028\,
        window = 1.0D - 1.93D * Math.cos(2.0D * Math.PI * n / (N - 1.0D)) + 1.29D * Math.cos(4.0D * Math.PI * n / (N - 1.0D)) - 0.388D * Math.cos(6.0D * Math.PI * n / (N - 1.0D)) + 0.028D * Math.cos(8.0D * Math.PI * n / (N - 1.0D));
      } else if (WINDOW_HAMMING.equals(algorithm)) {
        window = 0.54D - 0.46D * Math.cos(2.0D * Math.PI * n / (N - 1.0D));        
      } else if (WINDOW_HANN.equals(algorithm)) {
        window = Math.sin(Math.PI * n / (N - 1.0D));
        window = window * window;
      } else if (WINDOW_NUTTALL.equals(algorithm)) {
        // a_{0}=0.355768;\quad a_{1}=0.487396;\quad a_{2}=0.144232;\quad a_{3}=0.012604\,
        window = 0.355768D - 0.487396D * Math.cos(2.0D * Math.PI * n / (N - 1.0D)) + 0.144232D * Math.cos(4.0D * Math.PI * n / (N - 1.0D)) - 0.012604D * Math.cos(6.0D * Math.PI * n / (N - 1.0D));
      } else if (WINDOW_PARZEN.equals(algorithm)) {
        if (n <= (N / 4.0D)) {
          window = 1.0D - 6.0D * Math.pow(n / (N / 2.0D), 2.0D) * (1.0D - (n / (N / 2.0D)));
        } else {
          window = 2.0D * Math.pow((1.0D - (n / (N / 2.0D))), 3.0D);          
        }
      } else if (WINDOW_RECTANGULAR.equals(algorithm)) {
        window = 1.0D;
      } else if (WINDOW_SINE.equals(algorithm)) {
        window = Math.sin(Math.PI * n / (N - 1.0D));
      } else if (WINDOW_TRIANGULAR.equals(algorithm)) {
        window = 1.0D - Math.abs((n - ((N - 1.0D) / 2.0D))/(N / 2.0D));
      } else if (WINDOW_WELCH.equals(algorithm)) {
        window = 1.0D * ((n - ((N - 1.0D) / 2.0D)) / ((N - 1.0D) / 2.0D));        
      } else {
        throw new WarpScriptException("Unknown windowing function '" + algorithm + "'.");
      }
        
      long ts = GTSHelper.tickAtIndex(gts, n);
      long geoxppoint = GTSHelper.locationAtIndex(gts, n);
      long elevation = GTSHelper.elevationAtIndex(gts, n);
      double value = ((Number) GTSHelper.valueAtIndex(gts, n)).doubleValue();
           
      GTSHelper.setValue(windowed, ts, geoxppoint, elevation, value * window, false);
    }
    
    return windowed;
  }  
}
