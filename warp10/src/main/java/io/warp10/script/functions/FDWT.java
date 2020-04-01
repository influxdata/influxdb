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
import io.warp10.script.fwt.FWT;
import io.warp10.script.fwt.Wavelet;
import io.warp10.script.fwt.wavelets.WaveletRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Apply Forward Discrete Wavelet Transform
 */
public class FDWT extends GTSStackFunction {
  
  private static final String WAVELET = "wavelet";
  
  public FDWT(String name) {
    super(name);
  }  
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a wavelet name on top of the stack.");
    }
    
    Wavelet wavelet = WaveletRegistry.find(top.toString());
    
    if (null == wavelet) {
      throw new WarpScriptException(getName() + " could not find wavelet '" + top.toString() + "'.");
    }
    
    Map<String,Object> params = new HashMap<String, Object>();
    params.put(WAVELET, wavelet);
    
    return params;
  }
  
  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    Wavelet wavelet = (Wavelet) params.get(WAVELET);

    //
    // Check GTS type
    //
    
    if (TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) {
      throw new WarpScriptException(getName() + " can only be applied to numeric Geo Time Series.");
    }

    //
    // Check GTS size, MUST be a power of 2
    //
    
    int len = gts.size();
    
    if (0 != (len & (len - 1))) {
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series whose length is a power of 2.");
    }
    
    //
    // Sort GTS
    //
    
    GTSHelper.sort(gts);
    
    //
    // Extract ticks, locations, elevations, values
    //
    
    double[] values = GTSHelper.getValuesAsDouble(gts);
    
    double[] hilbert = FWT.forward(wavelet, values);
    
    GeoTimeSerie transformed = gts.cloneEmpty();
    
    for (int i = 0; i < len; i++) {
      GTSHelper.setValue(transformed, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), hilbert[i], false);
    }
    
    return transformed;
  }
}
