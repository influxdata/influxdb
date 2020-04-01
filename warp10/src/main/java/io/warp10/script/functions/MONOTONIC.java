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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a GTS to a monotic one (i.e. for which two consecutive values are in
 * a constant order, either increasing or decreasing)
 */
public class MONOTONIC  extends GTSStackFunction {
  
  private static final String PARAM_ORDER = "order";
  
  public MONOTONIC(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    // Pop a boolean from the stack
    Object o = stack.pop();
    
    if (!(o instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a boolean on top of the stack.");
    }

    Map<String,Object> params = new HashMap<String, Object>();
    params.put(PARAM_ORDER, Boolean.TRUE.equals(o));
    
    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    boolean decreasing = Boolean.TRUE.equals(params.get(PARAM_ORDER));
    
    //
    // Sort values
    //
    
    GTSHelper.sort(gts, false);
    
    //
    // Clone empty
    //
    
    GeoTimeSerie monotonic = gts.cloneEmpty();
    
    int n = GTSHelper.nvalues(gts);

    TYPE type = gts.getType();

    long lvalue;
    double dvalue;
    String svalue;
    boolean bvalue;
    
    if (decreasing) {
      lvalue = Long.MAX_VALUE;
      dvalue = Double.POSITIVE_INFINITY;
      svalue = null;
      bvalue = true;
    } else {
      lvalue = Long.MIN_VALUE;
      dvalue = Double.NEGATIVE_INFINITY;
      svalue = "";
      bvalue = false;
    }
    
    for (int i = 0; i < n; i++) {
      long tick = GTSHelper.tickAtIndex(gts, i);
      long location = GTSHelper.locationAtIndex(gts, i);
      long elevation = GTSHelper.elevationAtIndex(gts, i);
      Object value = GTSHelper.valueAtIndex(gts, i);
      
      switch (type) {
        case LONG:
          long lv = ((Number) value).longValue(); 
          if (decreasing) {
            if (lv < lvalue) {
              lvalue = lv;
            }
          } else {
            if (lv > lvalue) {
              lvalue = lv;
            }
          }
          value = lvalue;
          break;
          
        case DOUBLE:
          double dv = ((Number) value).doubleValue();
          if (decreasing) {
            if (dv < dvalue) { 
              dvalue = dv;
            }
          } else {
            if (dv > dvalue) {
              dvalue = dv;
            }
          }
          value = dvalue;
          break;
          
        case STRING:
          String sv = value.toString();
          if (decreasing) {
            if (null == svalue || sv.compareTo(svalue) < 0) {
              svalue = sv;
            }
          } else {
            if (sv.compareTo(svalue) > 0) {
              svalue = sv;
            }
          }
          value = svalue;
          break;
          
        case BOOLEAN:
          boolean bv = Boolean.TRUE.equals(value);
          if (decreasing) {
            if (Boolean.FALSE.equals(bv)) {
              bvalue = bv;
            }
          } else {
            if (Boolean.TRUE.equals(bv)) {
              bvalue = bv;
            }
          }
          value = bvalue;
          break;
        default:
      }
      
      GTSHelper.setValue(monotonic, tick, location, elevation, value, false);
    }
    
    return monotonic;
  }
}
