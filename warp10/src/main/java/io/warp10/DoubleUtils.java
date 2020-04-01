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
package io.warp10;

import sun.misc.DoubleConsts;

public class DoubleUtils {
  public static boolean isFinite(double d) {
    return Math.abs(d) <= DoubleConsts.MAX_VALUE;
  }
  
  /**
   * Compute mean and variance without applying Bessel's correction
   * 
   * @param values
   * @return
   */
  public static double[] muvar(double[] values, int offset, int len) {
    double sum = 0.0D;
    double sumsq = 0.0D;
    
    int n = len;
    
    // Choose shifting value to avoid cancellation during
    // variance computation
    // @see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance 
    
    double shift = values[offset];
    
    int i = 0;
    
    // Choose the first shifting value which is not zero
    while(0.0D != shift && i < offset + len) {
      shift = values[i++];
    }
    
    // Compute shifted variance so
    for (i = 0; i < n; i++) {
      double term = values[i] - shift;
      sum += term;
      sumsq += term * term;
    }
    
    double mean = sum / n;
    double var = (sumsq / n) - (mean * mean);
    
    double mu = mean + (shift / n);
    
    double[] muvar = new double[2];
    muvar[0] = mu;
    muvar[1] = var;
    
    return muvar;
  }
  
  public static double[] muvar(double[] values) {
    return muvar(values, 0, values.length);
  }
  
  public static double[] musigma(double[] values, int offset, int len, boolean bessel) {
    double[] musigma = muvar(values, offset, len);
    
    if (bessel && len > 1) {
      musigma[1] = musigma[1] * len / (len - 1);      
    }
        
    musigma[1] = Math.sqrt(musigma[1]);
    
    return musigma;
  }
  
  public static double[] musigma(double[] values, boolean bessel) {
    return musigma(values, 0, values.length, bessel);
  }
}
