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

import java.math.BigDecimal;

import io.warp10.DoubleUtils;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Perform Dynamic Time Warping distance computation
 * between values of two GTS.
 * 
 */
public class DTW extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Should we normalize?
   */
  private final boolean normalize;
  
  /**
   * Should we do Z-Normalization or 0-1 normalization?
   */
  private final boolean znormalize;
  
  public DTW(String name, boolean normalize, boolean znormalize) {
    super(name);
    this.normalize = normalize;
    this.znormalize = znormalize;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a numeric threshold on top of the stack.");
    }
    
    double threshold = ((Number) o).doubleValue();
    
    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects two Geo Time Series below the threshold.");
    }

    GeoTimeSerie gts1 = (GeoTimeSerie) o;

    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects two Geo Time Series below the threshold.");
    }

    GeoTimeSerie gts2 = (GeoTimeSerie) o;

    stack.push(compute(gts1,gts2,threshold));

    return stack;
  }
  
  /**
   * Compute Dynamic Time Warping on two GTS, aborting if the threshold is exceeded
   * 
   * @param gts1 First GTS
   * @param gts2 Second GTS
   * @param threshold Threshold used to abort, use 0.0D if you don't want to abort the DTW
   * @return The computed DTW distance or -1 if the threshold is exceeded
   * 
   * @throws WarpScriptException If an error occurs
   */
  public final double compute(GeoTimeSerie gts1, GeoTimeSerie gts2, double threshold) throws WarpScriptException {
        
    //
    // Check that the type of the GTS is numerical
    //
    
    if (TYPE.LONG != gts1.getType() && TYPE.DOUBLE != gts1.getType()) {
      throw new WarpScriptException(getName() + " can only operate on numerical Geo Time Series.");
    }
    
    if (TYPE.LONG != gts2.getType() && TYPE.DOUBLE != gts2.getType()) {
      throw new WarpScriptException(getName() + " can only operate on numerical Geo Time Series.");
    }

    //
    // Sort GTS in chronological order
    //
    
    GTSHelper.sort(gts1);
    GTSHelper.sort(gts2);
    
    //
    // Extract values, compute min/max and quantize values (x - max/(max - min))
    //
    
    double[] values1 = GTSHelper.getValuesAsDouble(gts1);        
    double[] values2 = GTSHelper.getValuesAsDouble(gts2);

    if (this.normalize) {
      if (this.znormalize) {
        //
        // Perform Z-Normalization of values1 and values2
        //
        
        double[] musigma = DoubleUtils.musigma(values1, true);
        
        for (int i = 0; i < values1.length; i++) {
          values1[i] = (values1[i] - musigma[0]) / musigma[1];
        }
        
        musigma = DoubleUtils.muvar(values2);
        for (int i = 0; i < values2.length; i++) {
          values2[i] = (values2[i] - musigma[0]) / musigma[1];
        }
      } else {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        
        for (int i = 0; i < values1.length; i++) {
          if (values1[i] < min) {
            min = values1[i];
          }
          if (values1[i] > max) {
            max = values1[i];
          }
        }
        
        if (min == max) {
          throw new WarpScriptException(getName() + " cannot (yet) operate on constant GTS.");
        }
        
        double range = max - min;
        
        for (int i = 0; i < values1.length; i++) {
          values1[i] = (values1[i] - min) / range;
        }

        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < values2.length; i++) {
          if (values2[i] < min) {
            min = values2[i];
          }
          if (values2[i] > max) {
            max = values2[i];
          }
        }
        
        if (min == max) {
          throw new WarpScriptException(getName() + " cannot (yet) operate on constant GTS.");
        }
        
        range = max - min;
        
        for (int i = 0; i < values2.length; i++) {
          values2[i] = (values2[i] - min) / range;
        }      
      }      
    }

    return compute(values1, 0, values1.length, values2, 0, values2.length, threshold);
  }
  
  public double compute(double[] values1, int offset1, int len1, double[] values2, int offset2, int len2, double threshold) {
    return compute(values1, offset1, len1, values2, offset2, len2, threshold, true);
  }
  
  public double compute(double[] values1, int offset1, int len1, double[] values2, int offset2, int len2, double threshold, boolean manhattan) {
    //
    // Make sure value1 is the shortest array
    //
    
    if (len1 > len2) {
      double[] tmp = values1;
      values1 = values2;
      values2 = tmp;
      int tmpint = offset1;
      offset1 = offset2;
      offset2 = tmpint;
      tmpint = len1;
      len1 = len2;
      len2 = tmpint;
    }
    
    //
    // Now run DTW.
    // We allocate two columns so we can run DTW, only allocating
    //
    
    double[] a = new double[len1];
    double[] b = new double[len1];
    
    int w = values2.length;
    
    boolean belowThreshold = false;
    
    for (int i = 0; i < len2; i++) {
      int start = Math.max(0, i - w);
      int end = Math.min(len1 - 1, i + w);
      
      belowThreshold = false;
      
      for (int j = start; j <= end; j++) {
        
        //
        // Compute distance.
        // DTW simply considers the delta in values, not the delta in indices
        //
        
        double d = manhattan ? Math.abs(values1[offset1 + j] - values2[offset2 + i]) : Math.pow(values1[offset1 + j] - values2[offset2 + i], 2.0D);
        
        //
        // Extract surrounding values
        //
        
        double left = a[j];
        double bottom = j > start ? b[j - 1] : Double.POSITIVE_INFINITY;
        double sw = j > start ? a[j - 1] : Double.POSITIVE_INFINITY;
        
        b[j] = d + Math.min(left, Math.min(bottom, sw));
        //System.out.println(i + " " + j + " " + d + " " + left + " " + bottom + " " + sw + " >>> " + b[j]);
        
        if (!belowThreshold && (0.0D == threshold || b[j] <= threshold)) {
          belowThreshold = true;
        }
      }
      
      // Exit if no value is below threshold
      if (!belowThreshold) {
        break;
      }
      
      // Shift b into a
      double[] tmp = a;
      a = b;           
      b = tmp;
    }
    
    if (!belowThreshold) {
      return -1.0D;
    }
    
    double dtwDist = Double.POSITIVE_INFINITY;
    
    for (int i = 0; i < a.length; i++) {
      if (a[i] < dtwDist) {
        dtwDist = a[i];
      }
    }
    
    if (!manhattan) {
      dtwDist = Math.sqrt(dtwDist);
    }
    
    return dtwDist;
  }
}
