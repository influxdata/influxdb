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
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Apply Seasonal Trend decomposition based on Loess procedure
 * @see http://www.wessa.net/download/stl.pdf
 */
public class STL extends GTSStackFunction {
  
  //
  // Mandatory parameter
  //

  public static final String PERIOD_PARAM = "PERIOD";
  
  //
  // Optional parameters
  //

  public static final String PRECISION_PARAM = "PRECISION";
  public static final String ROBUSTNESS_PARAM = "ROBUSTNESS";

  //
  // High Level optional parameter
  // if true, set PRECISION to 1, ROBUSTNESS to 15
  // if false, set PRECISION to 2, ROBUSTNESS to 0
  //

  public static final String ROBUST_PARAM = "ROBUST";
  
  //
  // Optional lowess parameters
  //

  public static final String BANDWIDTH_S_PARAM = "BANDWIDTH_S";
  public static final String DEGREE_S_PARAM = "DEGREE_S";
  public static final String SPEED_S_PARAM = "SPEED_S";

  public static final String BANDWIDTH_L_PARAM = "BANDWIDTH_L";
  public static final String DEGREE_L_PARAM = "DEGREE_L";
  public static final String SPEED_L_PARAM = "SPEED_L";

  public static final String BANDWIDTH_T_PARAM = "BANDWIDTH_T";
  public static final String DEGREE_T_PARAM = "DEGREE_T";
  public static final String SPEED_T_PARAM = "SPEED_T";

  public static final String BANDWIDTH_P_PARAM = "BANDWIDTH_P";
  public static final String DEGREE_P_PARAM = "DEGREE_P";
  public static final String SPEED_P_PARAM = "SPEED_P";
    
  public STL(String name) {
    super(name);
  }

  /**
   * The function STL expects on top of the stack:
   * - a gts or list of gts
   * - followed by a map with the set {key:value} as {PARAM:VALUE}
   * 
   * At least PERIOD must be set
   * For each pair "key":value in the map where "key" is a word, every parameter starting with "key" will be set to value.
   * 
   * All unspecified parameters will be set to their default.
   */
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String,Object>();
    
    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map of parameters below input GTS");
    }
    
    //
    // Handle Map
    //
    
    String[] field_names_1 = {"PERIOD","PRECISION","ROBUSTNESS"};
    String[] field_names_2 = {"BANDWIDTH","DEGREE","SPEED"};
    String[] suffixes = {"_S","_L","_T","_P"};
    
    Map<String,Object> last_params = (Map<String, Object>) top;
    
    for (Map.Entry<String, Object> entry : last_params.entrySet()) {
      
      // retrieve key
      String key = entry.getKey();
      String body = key.substring(0, key.length() - 2);
      String suffix = key.substring(key.length() - 2, key.length());
      
      // retrieve value
      Object value = entry.getValue();
      
      // handle boolean parameter
      if (key.equals(ROBUST_PARAM)) {
        if (!(value instanceof Boolean)) {
          throw new WarpScriptException(getName() + " expects argument " + key + " to be of type BOOLEAN.");
        }

        params.put(key, ((Boolean) value).booleanValue());
        continue;
      }

      // put to params if correct
      if (!(Arrays.asList(field_names_1).contains(key) || (Arrays.asList(field_names_2).contains(body)) && Arrays.asList(suffixes).contains(suffix) || Arrays.asList(field_names_2).contains(key))) {
        throw new WarpScriptException(getName() + " does not expect argument " + key);          
      } else {
        if (!(value instanceof Long)) {
          throw new WarpScriptException(getName() + " expects argument " + key + " to be of type LONG.");
        } else {
          if (null == params.get(key)) {
            params.put(key, ((Number) value).intValue());
          }
        }
      }        
    }
    
    // Handle multinomial fields if any (ie BANDWITDTH, DEGREE and SPEED without suffixes are set to every lowess call)
    for (int u = 0; u < 3; u++) {
      Object o;
      if (null != (o = params.get(field_names_2[u]))) {
        for (int v = 0; v < 4; v++) {
          String to_put = field_names_2[u] + suffixes[v];
          
          // put only if had not been previously put
          if (null == params.get(to_put)) {
            params.put(to_put, ((Number) o).intValue());
          }
        }
      }    
    }
    
    return params;
  }
  
  // This method is used for the default values of some parameters
  private int nextOdd(int a) {
    if (a > 0) {
      return 1 == a / 2 ? a : a + 1;
    } else {
      return 1;
    }
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    
    //
    // Extract parameters or set to their default value
    //
    
    // default values are the same as for R's implementation (except for ns and ds)
    // @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/stl.html
    
    if (null == params.get(PERIOD_PARAM)) {
      throw new WarpScriptException(getName() + " expects map of parameters to at least contains field PERIOD");
    }
    
    // only buckets_per_period is mandatory
    int buckets_per_period = ((Number) params.get(PERIOD_PARAM)).intValue();

    // If ROBUST_PARAM is not set, consider it false
    if (null == params.get(ROBUST_PARAM)) {
      params.put(ROBUST_PARAM, false);
    }

    // number of inner and outer loop: 1 and 15 if robust, 2 and 0 otherwise.
    int inner = (boolean) params.get(ROBUST_PARAM) ? 1 : 2;
    int outer = (boolean) params.get(ROBUST_PARAM) ? 15 : 0;
    if (null != params.get(PRECISION_PARAM)) {
      inner = ((Number) params.get(PRECISION_PARAM)).intValue();
    }
    if (null != params.get(ROBUSTNESS_PARAM)) {
      outer = ((Number) params.get(ROBUSTNESS_PARAM)).intValue();
    }
    
    // authors recommend ns to be odd and at least 7
    int ns = null == params.get(BANDWIDTH_S_PARAM) ? 7 : ((Number) params.get(BANDWIDTH_S_PARAM)).intValue();
    int ds = null == params.get(DEGREE_S_PARAM) ? 1 : ((Number) params.get(DEGREE_S_PARAM)).intValue();
    int js = null == params.get(SPEED_S_PARAM) ? ns / 10 : ((Number) params.get(SPEED_S_PARAM)).intValue();
    
    int nl = null == params.get(BANDWIDTH_L_PARAM) ? nextOdd(buckets_per_period) : ((Number) params.get(BANDWIDTH_L_PARAM)).intValue();
    int dl = null == params.get(DEGREE_L_PARAM) ? 1 : ((Number) params.get(DEGREE_L_PARAM)).intValue();
    int jl = null == params.get(SPEED_L_PARAM) ? nl / 10 : ((Number) params.get(SPEED_L_PARAM)).intValue();
    
    int value = (int) Math.ceil(1.5 * buckets_per_period / (1 - (1.5 / ns)));
    
    int nt = null == params.get(BANDWIDTH_T_PARAM) ? nextOdd(value) : ((Number) params.get(BANDWIDTH_T_PARAM)).intValue();
    int dt = null == params.get(DEGREE_T_PARAM) ? 1 : ((Number) params.get(DEGREE_T_PARAM)).intValue();
    int jt = null == params.get(SPEED_T_PARAM) ? nt / 10 : ((Number) params.get(SPEED_T_PARAM)).intValue();
    
    // default is no post seasonal smoothing
    int np = null == params.get(BANDWIDTH_P_PARAM) ? 0 : ((Number) params.get(BANDWIDTH_P_PARAM)).intValue();
    int dp = null == params.get(DEGREE_P_PARAM) ? 2 : ((Number) params.get(DEGREE_P_PARAM)).intValue();
    int jp = null == params.get(SPEED_P_PARAM) ? np / 10 : ((Number) params.get(SPEED_P_PARAM)).intValue();

    //
    // Sanity check on arguments
    //

    if (buckets_per_period < 2) {
      throw new WarpScriptException("Seasonal periods must be composed by at least 2 buckets.");
    }

    if (inner < 1) {
      throw new WarpScriptException(PRECISION_PARAM + " must be positive.");
    }

    if (outer < 0) {
      throw new WarpScriptException(ROBUSTNESS_PARAM + " can not be negative.");
    }

    if (0 == ns) {
      throw new WarpScriptException(BANDWIDTH_S_PARAM + " can not be equal to zero.");
    }

    if (ds < 0) {
      throw new WarpScriptException(DEGREE_S_PARAM + " can not be negative.");
    }

    if (js < 0) {
      throw new WarpScriptException(SPEED_S_PARAM + " can not be negative.");
    }

    if (nl < 0) {
      throw new WarpScriptException(BANDWIDTH_L_PARAM + " can not be negative..");
    }

    if (dl < 0) {
      throw new WarpScriptException(DEGREE_L_PARAM + " can not be negative.");
    }

    if (jl < 0) {
      throw new WarpScriptException(SPEED_L_PARAM + " can not be negative.");
    }

    if (nt < 0) {
      throw new WarpScriptException(BANDWIDTH_T_PARAM + " can not be negative..");
    }

    if (dt < 0) {
      throw new WarpScriptException(DEGREE_T_PARAM + " can not be negative.");
    }

    if (jt < 0) {
      throw new WarpScriptException(SPEED_T_PARAM + " can not be negative.");
    }

    if (np < 0) {
      throw new WarpScriptException(BANDWIDTH_P_PARAM + " can not be negative.");
    }

    if (dp < 0) {
      throw new WarpScriptException(DEGREE_P_PARAM + " can not be negative.");
    }

    if (jp < 0) {
      throw new WarpScriptException(SPEED_P_PARAM + " can not be negative.");
    }
    
    //
    // Call STL
    //
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    results = GTSHelper.stl(gts, buckets_per_period, inner, outer, ns, ds, js, nl, dl, jl, nt, dt, jt, np, dp, jp);
    
    return results;
  }
  
  // getter of previous method
  public Object doGtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    return gtsOp(params, gts);
  }
}