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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Split a GTS representing a wavelet transform into separate wavelets per level
 */
public class DWTSPLIT extends GTSStackFunction {
  
  private static final String LEVEL_LABEL = "levellabel";
  
  public DWTSPLIT(String name) {
    super(name);
  }  
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a level label name on top of the stack.");
    }
    
    Map<String,Object> params = new HashMap<String, Object>();
    params.put(LEVEL_LABEL, top.toString());
    
    return params;
  }
  
  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    String levellabel = params.get(LEVEL_LABEL).toString();
    
    //
    // Check GTS type
    //
    
    if (TYPE.DOUBLE != gts.getType()) {
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
    // Check that GTS does not contain 'levellabel'
    //
    
    if (gts.getLabels().containsKey(levellabel)) {
      throw new WarpScriptException(getName() + " input cannot contain label '" + levellabel + "'");
    }
    
    //
    // Sort GTS
    //
    
    GTSHelper.sort(gts);

    int tmp = len >> 1;
    int levels = 0;
    
    while (tmp != 0) {
      levels++;
      tmp = tmp >>> 1;
    }
    
    int idx = 0;
    
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    for (int level = 0; level < levels; level++) {
      int levellen = 1 << level;
      
      GeoTimeSerie levelgts = new GeoTimeSerie(levellen);
      
      // Copy labels
      levelgts.setMetadata(gts.getMetadata());
      
      // Add level label
      levelgts.getMetadata().putToLabels(levellabel, Integer.toString(levels - level));
      
      result.add(levelgts);
      
      while (levellen > 0) {
        GTSHelper.setValue(levelgts, GTSHelper.tickAtIndex(gts, idx), GTSHelper.locationAtIndex(gts, idx), GTSHelper.elevationAtIndex(gts, idx), GTSHelper.valueAtIndex(gts, idx), false);
        levellen--;
        idx++;
      }
    }
    
    return result;
  }
}
