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
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.STL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.TDistribution;

/**
 * Class that implements tests to detect outliers in GTS
 * 
 */
public class GTSOutliersHelper {
  
  //
  // Tests should first check if GTS is of type double
  //
  
  protected static void doubleCheck(GeoTimeSerie gts) throws WarpScriptException {
    if (GeoTimeSerie.TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("GTS must be of type Double.");
    }
  }
  
  //
  // Basic methods
  //
  
  protected static double mean(GeoTimeSerie gts) throws WarpScriptException {
    double mean = 0.0D;
    for (int i = 0; i < gts.values; i++) {
      mean += gts.doubleValues[i];
    }
    
    if (0 == gts.values) {
      throw new WarpScriptException("Can't compute mean as gts is empty");
    }
    
    return mean / gts.values;
  }

  protected static double median(GeoTimeSerie gts) {    
    // FIXME(JCV): if size of gts too big, we should better work inplace by making a quickSortByValue and sort by ticks after
    double[] copy = Arrays.copyOf(gts.doubleValues, gts.values);
    Arrays.sort(copy);
    
    return gts.values % 2 == 0 ? (copy[gts.values/2] + copy[gts.values/2 - 1])/2 : copy[gts.values/2];
  }
  
  protected static double medianAbsoluteDeviation(GeoTimeSerie gts, double median) {
    double[] copy = Arrays.copyOf(gts.doubleValues, gts.values);
    for (int i = 0; i < gts.values; i++) {
     copy[i] -= median;
     copy[i] = Math.abs(copy[i]);
    }
    Arrays.sort(copy);
    
    return gts.values % 2 == 0 ? (copy[gts.values/2] + copy[gts.values/2 - 1])/2 : copy[gts.values/2];
  }
  
  protected static double max(GeoTimeSerie gts) throws WarpScriptException {
    double max = gts.doubleValues[0];
    if (Double.isNaN(max)) {
      throw new WarpScriptException("Method max: GTS contains NaN");
    }
    
    for (int i = 1; i < gts.values; i++) {
      if (max < gts.doubleValues[i]) {
        max = gts.doubleValues[i];
      }
    }
    return max;
  }
  
  protected static double min(GeoTimeSerie gts) throws WarpScriptException {
    double min = gts.doubleValues[0];
    if (Double.isNaN(min)) {
      throw new WarpScriptException("Method min: GTS contains NaN");
    }    
    
    for (int i = 1; i < gts.values; i++) {
      if (min > gts.doubleValues[i]) {
        min = gts.doubleValues[i];
      }
    }
    return min;
  }
  
  /**
   * Compute mu and sigma or median and mad, given useMedian is true or false 
   * Mad is returned modified as an estimate of sigma
   */
  protected static double[] madsigma(GeoTimeSerie gts, boolean useMedian) {
    double[] madsigma = null;
    
    if (!useMedian) {
      madsigma = GTSHelper.musigma(gts, true);
    } else {
      madsigma = new double[2];
      madsigma[0] = median(gts);
      madsigma[1] = medianAbsoluteDeviation(gts, madsigma[0]);
      
      // we want mad to be an estimate of sigma, so we divide it by 0.6745D if its != 0.0D
      if (0.0D != madsigma[1]) {
        madsigma[1] /= 0.6745D;
      } else {
        madsigma[1] = GTSHelper.musigma(gts, true)[1];
      }
    }
    
    return madsigma;
  }
  
  /**
   * Compute Z-score or modified Z-score
   * @see http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm
   * 
   * @param gts
   * @param useMedian       Should we compute Z-score using median/mad rather than mean/std
   * @param inplace   Should the output gts be the input instance or a new one
   * 
   * @return gts_
   * 
   * @throws WarpScriptException
   */
  public static GeoTimeSerie zScore(GeoTimeSerie gts, boolean useMedian, boolean inplace) throws WarpScriptException {
    doubleCheck(gts);
    
    double[] musigma = madsigma(gts, useMedian);
    double m = musigma[0];
    double std = musigma[1];
    
    if (0.0D == std) {
      throw new WarpScriptException((useMedian ? "Standard": "Median Absolute") + " Deviation is null");
    }    
    
    GeoTimeSerie gts_ = inplace ? gts : gts.clone();
    
    for (int i = 0; i < gts_.values; i++) { 
      gts_.doubleValues[i] = (gts_.doubleValues[i] - m) / std;
    }
    
    return gts_;
  }
  
  //
  // Commonly used tests
  //
  
  /**
   * Applying a simple threshold test.
   * 
   * @param gts
   * @param threshold
   * @param abs         Should we compare threshold with absolute value
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> thresholdTest(GeoTimeSerie gts, double threshold, boolean abs) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();
   
    for (int i = 0; i < gts.values; i++) {
      if (TYPE.DOUBLE == gts.type) {
        double temp = gts.doubleValues[i];
        if (abs) {
          temp = Math.abs(temp);
        }
        if (temp >= threshold) {
          anomalous_ticks.add(gts.ticks[i]);
        }        
      } else if (TYPE.LONG == gts.type) {
        long temp = gts.longValues[i];
        if (abs) {
          temp = Math.abs(temp);
        }
        if (temp >= threshold) {
          anomalous_ticks.add(gts.ticks[i]);
        }
      }
    }
    
    return anomalous_ticks;
  }
  
  public static List<Long> thresholdTest(GeoTimeSerie gts, double threshold) throws WarpScriptException {
    return thresholdTest(gts, threshold, true);
  }
  
  /**
   * Applying Z-score test
   * If useMedian is true, then use modified z-score.
   * @see http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm
   * 
   * @param gts
   * @param useMedian   Should we compute Z-score using median/mad rather than mean/std
   * @param d     Threshold. Default set at 3.5
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> zScoreTest(GeoTimeSerie gts, boolean useMedian, double d) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();

    double[] musigma = madsigma(gts, useMedian);
    double m = musigma[0];
    double std = musigma[1];
    if (0.0D == std) {
      return anomalous_ticks;
    }
    
    for (int i = 0; i < gts.values; i++) { 
      double z = (gts.doubleValues[i] - m) / std;
      if (Math.abs(z) >= d) {
        anomalous_ticks.add(gts.ticks[i]);
      }
    }
    
    return anomalous_ticks;
  }
    
  /**
   * Applying Grubbs' test using mean/std or median/mad
   * @see http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h1.htm
   * 
   * @param gts
   * @param useMedian     Should the test use median/mad instead of mean/std
   * @param alpha   Significance level with which to accept or reject anomalies. Default is 0.05
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> grubbsTest(GeoTimeSerie gts, boolean useMedian, double alpha) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();
    

    int N = gts.values;
    if (N < 3) {
      // no anomalous tick in this case
      return anomalous_ticks;
    }
    
    double[] musigma = madsigma(gts, useMedian);
    double m = musigma[0];
    double std = musigma[1];
    if (0.0D == std) {
      return anomalous_ticks;
    }
    
    double z = 0.0D;
    double max = Double.NEGATIVE_INFINITY;
    long suspicious_tick = 0L;
    for (int i = 0; i < N; i++) {
      z = Math.abs((gts.doubleValues[i] - m) / std);
      if (z > max) {
        max = z;
        suspicious_tick = gts.ticks[i];
      }
    }
    
    //
    // Calculate critical value
    //
    
    double t = new TDistribution(N - 2).inverseCumulativeProbability(alpha / (2 * N));
    
    //
    // Calculate threshold
    //
    
    double Ginf = (N - 1) * Math.abs(t) / Math.sqrt(N * (N - 2 + t * t));
    
    //
    // Test
    //
    
    if (max > Ginf) {
      anomalous_ticks.add(suspicious_tick);
    }
    
    return anomalous_ticks;    
  }
  
  public static List<Long> grubbsTest(GeoTimeSerie gts, boolean useMedian) throws WarpScriptException {
    return grubbsTest(gts, useMedian, 0.05D);
  }
  
  /**
   * Applying Tietjen and Moore's test using mean/std or median/mad
   * @see http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h2.htm
   * 
   * @param gts
   * @param k       Number of outliers
   * @param useMedian     Should the test use median instead of mean
   * @param alpha   Significance level with which to accept or reject anomalies. Default is 0.05
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  // Not yet implemented as description of test is unclear. Consider using ESDTest instead.
  public static List<Long> tietjenMooreTest(GeoTimeSerie gts, int k, boolean useMedian, double alpha) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();
    
    // WIP
    if (true) throw new WarpScriptException("tietjenMooreTest: Work in Progress. Consider using ESDTest instead.");
    
    return anomalous_ticks;    
  }
  
  /**
   * Applying generalized extreme Studentized deviate test using mean/std or median/mad
   * @see http://www.itl.nist.gov/div898/handbook/eda/section3/eda35h3.htm
   * 
   * @param gts
   * @param k       Upper bound of suspected number of outliers
   * @param useMedian     Should the test use median/mad instead of mean/std
   * @param alpha   Significance level with which to accept or reject anomalies. Default is 0.05
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> ESDTest(GeoTimeSerie gts, int k, boolean useMedian, double alpha) throws WarpScriptException {
    doubleCheck(gts);
    
    // Clone GTS (not necessary but simplifies implementation) -> copy only needed fields
    //GeoTimeSerie clone = gts.clone();
    GeoTimeSerie clone = new GeoTimeSerie();
    clone.type = gts.type;
    clone.values = gts.values;
    clone.doubleValues = Arrays.copyOf(gts.doubleValues, gts.values);
    clone.ticks = Arrays.copyOf(gts.ticks, gts.values);
    
    List<Long> anomalous_ticks = new ArrayList<Long>();
    
    int greater_j_test_passed = -1;
    for (int j = 0; j < k; j++) {
      
      int N = clone.values;
      if (N < 3) {
        // In this case there are no more outlier left
        break;
      }
      
      double[] musigma = madsigma(clone, useMedian);
      double m = musigma[0];
      double std = musigma[1];
      
      if (0.0D == std) {
        // In this case there are no more outlier left
        break;
      }
      
      double z = 0.0D;
      double max = Double.NEGATIVE_INFINITY;
      int suspicious_idx = 0;
      
      for (int i = 0; i < N; i++) {
        z = Math.abs((clone.doubleValues[i] - m) / std);
        if (z > max) {
          max = z;
          suspicious_idx = i;
        }
      }
      
      //
      // Calculate critical value
      //
      
      double p =  1 - alpha / (2 * N);
      double t = new TDistribution(N - 2).inverseCumulativeProbability(p);
      
      //
      // Calculate threshold
      //
      
      double lambda = (N - 1) * t / Math.sqrt((N - 2 + t * t) * N);
      
      //
      // Test
      //
      
      if (max > lambda) {
        greater_j_test_passed = j;
      }
      
      //
      // Removing potential outlier before next loop
      //
      
      clone.values--;
      
      // We swap it with last point
      long tmp_tick = clone.ticks[suspicious_idx];
      clone.ticks[suspicious_idx] = clone.ticks[clone.values];
      clone.ticks[clone.values] = tmp_tick;
      
      // We don't need to keep the value of the potential outlier
      clone.doubleValues[suspicious_idx] = clone.doubleValues[clone.values];
    }
    
    // adding to output
    for (int j = 0; j <= greater_j_test_passed; j++) {
      anomalous_ticks.add(clone.ticks[gts.values - 1 - j]);
    }
    
    return anomalous_ticks;
  }
  
  public static List<Long> ESDTest(GeoTimeSerie gts, int k, boolean useMedian) throws WarpScriptException {
    return ESDTest(gts, k, useMedian, 0.05D);
  }
  
  //
  // Tests based on seasonal extraction
  // Consider using methods of the next section instead
  // as piecewise approximation yields better results.
  //

  /**
   * Applying STL-ESD test.
   * ESD test is passed on residual of STL decomposition.
   * 
   * @param gts
   * @param k       Upper bound of suspected number of outliers
   * @param alpha   Significance level with which to accept or reject anomalies. Default is 0.05
   * @param params  Optional map of parameters used for stl calls
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> STLESDTest(GeoTimeSerie gts, int buckets_per_period, int k, double alpha, Map<String, Object> params) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();

    if (!GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException("GTS must be bucketized");
    }
    
    //
    // Handling parameters of stl calls
    //
    
    if (null == params) {
      params = new HashMap<String,Object>();
    }
    
    // Parameters PERIOD and BANDWIDTH_S must be set    
    if (null != params.get(STL.PERIOD_PARAM)) {
      if (buckets_per_period != (int) params.get(STL.PERIOD_PARAM)) {
        throw new WarpScriptException("Incoherence between PERIOD parameter of test and PERIOD parameter of STL");
      }
    } else {
      params.put(STL.PERIOD_PARAM, buckets_per_period);
    }
    if (null == params.get(STL.BANDWIDTH_S_PARAM)) {
      params.put(STL.BANDWIDTH_S_PARAM, -1);
    }

    // per default, use non-robust version of STL since it is faster
    if (null == params.get(STL.ROBUST_PARAM)) {
      params.put(STL.ROBUST_PARAM, false);
    }
    
    // the other parameters of stl are either already present in params, or their default values fixed in STL class are used
    
    List<GeoTimeSerie> stl_output = (List<GeoTimeSerie>) new STL("STL").doGtsOp(params, gts);
    
    GeoTimeSerie seasonal = stl_output.get(0);
    GeoTimeSerie trend = stl_output.get(1);
    
    // reuse seasonal body to not reallocate arrays
    GeoTimeSerie remainder = seasonal;
    
    int idx = 0;
    for (int i = 0; i < gts.values; i++) {
      idx = Arrays.binarySearch(seasonal.ticks, idx, seasonal.values, gts.ticks[i]);
      
      if (idx < 0) {
        throw new WarpScriptException("Internal bug method STLESDTest");
      } else {
        remainder.doubleValues[i] = gts.doubleValues[idx] - (seasonal.doubleValues[idx] + trend.doubleValues[idx]); 
      }
    }
    
    remainder.values = gts.values;
    remainder.bucketcount = gts.bucketcount;
    remainder.bucketspan = gts.bucketspan;
    remainder.lastbucket = gts.lastbucket;
    
    anomalous_ticks.addAll(ESDTest(remainder, k, true, alpha));
    
    return anomalous_ticks;
  }
  
  /** NOT implemented Yet, See entropyHybridTest  below instead
   * Applying test based on Entropy Seasonal approximation.
   * 
   * @param gts                   Must be bucketized
   * @param buckets_per_period
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> entropyTest(GeoTimeSerie gts, int buckets_per_period) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();

    // TODO(JCV): WIP
    if (true) {
      throw new WarpScriptException("entropyTest: Work In Progress. Consider using entropyHybridTest");
    }
    
    return anomalous_ticks;    
  }
  
  //
  // Tests based on seasonal trend decomposition
  // upon piecewise approximation
  //
  
  /**
   * Applying Seasonal Hybrid ESD.
   * This test is based on piecewise median and STL seasonal extraction, completed by an ESD test.
   * It was developed at Twitter.
   * @see https://www.usenix.org/system/files/conference/hotcloud14/hotcloud14-vallis.pdf
   * @see https://github.com/twitter/AnomalyDetection
   * 
   * @param gts
   * @param k       Upper bound of suspected number of outliers
   * @param alpha   Significance level with which to accept or reject anomalies. Default is 0.05
   * @param params  Optional map of parameters used for stl calls
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> hybridTest(GeoTimeSerie gts, int buckets_per_period, int periods_per_piece, int k, double alpha, Map<String, Object> params) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();

    if (!GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException("GTS must be bucketized");
    }
    
    if (k >= periods_per_piece * buckets_per_period / 2) {
      throw new WarpScriptException("Upper bound of number of outliers must be less than half of the number of observations per piece");
    }

    if (gts.bucketcount / buckets_per_period < 1) {
      throw new WarpScriptException("Not enough buckets to make up at least one seasonal period.");
    }
    
    //
    // SubSerie attributes
    //
    
    GeoTimeSerie subgts = null;
    GeoTimeSerie seasonal = null;
    
    // number of pieces
    long pieces = gts.bucketcount / buckets_per_period / periods_per_piece;

    if (0 == pieces) {
      throw new WarpScriptException("Not enough seasonal periods to make up at least one piece. Please use a lower number of periods per piece.");
    }
    
    // number of buckets per piece
    int bpp = periods_per_piece * buckets_per_period;
    long lb = gts.lastbucket;
    long bs = gts.bucketspan;
    
    //
    // Handling parameters of stl calls
    //
    
    if (null == params) {
      params = new HashMap<String,Object>();
    }
    
    // Parameters PERIOD and BANDWIDTH_S must be set
    if (null != params.get(STL.PERIOD_PARAM)) {
      if (buckets_per_period != (int) params.get(STL.PERIOD_PARAM)) {
        throw new WarpScriptException("Incoherence between PERIOD parameter of test and PERIOD parameter of STL");
      }
    } else {
      params.put(STL.PERIOD_PARAM, buckets_per_period);
    }

    if (null == params.get(STL.BANDWIDTH_S_PARAM)) {
      params.put(STL.BANDWIDTH_S_PARAM, -1);
    }

    // per default, use non-robust version of STL since it is faster
    if (null == params.get(STL.ROBUST_PARAM)) {
      params.put(STL.ROBUST_PARAM, false);
    }
    
    // the other parameters of stl are either already present in params, or their default values fixed in STL class are used
    
    // instantiating STL
    STL stl = new STL("STL");
    
    for (int u = 0; u < pieces; u++) {

      long start = lb - bs * ((pieces - u) * bpp - 1);
      long stop = lb - bs * (pieces - u - 1) * bpp;
      
      // we don't start from the first bucket
      subgts = GTSHelper.subSerie(gts, start, stop, false, false, subgts);
      subgts.lastbucket = stop;
      subgts.bucketcount = bpp;
      subgts.bucketspan = bs;
      
      seasonal = ((List<GeoTimeSerie>) stl.doGtsOp(params, subgts)).get(0);
      
      double m = median(subgts);
      
      int idx = 0;
      for (int i = 0; i < subgts.values; i++) {
        idx = Arrays.binarySearch(seasonal.ticks, idx, seasonal.values, subgts.ticks[i]);
        
        if (idx < 0) {
          throw new WarpScriptException("Internal bug method hybridTest: can't find tick " + subgts.ticks[i] + " in seasonal.ticks");
        } else {
          subgts.doubleValues[i] -= (seasonal.doubleValues[idx] + m); 
        }
      }
      
      anomalous_ticks.addAll(ESDTest(subgts, k, true, alpha));
    }
    
    return anomalous_ticks;
  }
  
  
  /**
   * Applying Seasonal Entropy Hybrid ESD test
   * This test is based on piecewise decomposition where trend components are approximated by median and seasonal components are factored by the entropy of the cycle sub-series.
   * An ESD test is passed upon the residuals.
   * 
   * It differs from hybridTest by approximating seasonal component instead of using STL.
   * But in many cases this approximation is more useful than estimation of STL.
   * 
   * @param gts
   * @param k       Upper bound of suspected number of outliers
   * @param alpha   Significance level with which to accept or reject anomalies. Default is 0.05
   * 
   * @return anomalous_ticks
   * 
   * @throws WarpScriptException
   */
  public static List<Long> entropyHybridTest(GeoTimeSerie gts, int buckets_per_period, int periods_per_piece, int k, double alpha) throws WarpScriptException {
    doubleCheck(gts);
    List<Long> anomalous_ticks = new ArrayList<Long>();

    if (!GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException("GTS must be bucketized");
    }
    
    if (k >= periods_per_piece * buckets_per_period / 2) {
      throw new WarpScriptException("Upper bound of number of outliers must be less than half of the number of observations per piece");
    }

    if (gts.bucketcount / buckets_per_period < 1) {
      throw new WarpScriptException("Not enough buckets to make up at least one seasonal period.");
    }
    
    //
    // SubSerie attributes
    //
    
    GeoTimeSerie subgts = null;
    GeoTimeSerie subsubgts = null;
    GeoTimeSerie seasonal = null;
    
    // number of pieces
    long pieces = gts.bucketcount / buckets_per_period / periods_per_piece;

    if (0 == pieces) {
      throw new WarpScriptException("Not enough seasonal periods to make up at least one piece. Please use a lower number of periods per piece.");
    }
    
    // number of buckets per piece
    int bpp = periods_per_piece * buckets_per_period;
    long lb = gts.lastbucket;
    long bs = gts.bucketspan;
    
    for (int u = 0; u < pieces; u++) {

      long start = lb - bs * ((pieces - u) * bpp - 1);
      long stop = lb - bs * (pieces - u - 1) * bpp;
      
      // we don't start from the first bucket
      subgts = GTSHelper.subSerie(gts, start, stop, false, false, subgts);
      subgts.lastbucket = stop;
      subgts.bucketcount = bpp;
      subgts.bucketspan = bs;
      
      // entropy seasonal extraction
      if (null == seasonal) {
        seasonal = new GeoTimeSerie(bpp);
        seasonal.doubleValues = new double[bpp];
        seasonal.ticks = new long[bpp];
      } else {
        GTSHelper.reset(seasonal);
      }

      seasonal.type = TYPE.DOUBLE;
      
      // TODO(JCV): make this a method ?
      for (int v = 0; v < buckets_per_period; v++) {
        
        subsubgts = GTSHelper.subCycleSerie(subgts, stop - v * bs, buckets_per_period, true, subsubgts);
        
        // compute zscore, then we transform it into a probability before computing the entropy
        double[] musigma = GTSHelper.musigma(subsubgts, true);
        double mu = musigma[0];
        double sigma = musigma[1];
        
        double sum = 0.0D;
        for (int w = 0; w < subsubgts.values; w++) {
          subsubgts.doubleValues[w] = 0.0D != sigma ? Math.abs((subsubgts.doubleValues[w] - mu) / sigma) : 1.0D;

          // we use a variant of softmax to compute probabilities
          // exp separates too much non-outliers from the mean so we use (exp o sqrt)
          subsubgts.doubleValues[w] = Math.exp(Math.sqrt(subsubgts.doubleValues[w]));
          sum += subsubgts.doubleValues[w];
        }
        
        double entropy = 0.0D;
        for (int w = 0; w < subsubgts.values; w++) {
          subsubgts.doubleValues[w] /= sum;
          double tmp = subsubgts.doubleValues[w];
          if (0.0D != tmp) { 
            entropy -= tmp * Math.log(tmp);
          }
        }

        // normalize entropy and handle case where all items are the same
        if (0.0D != entropy) {
          entropy /= Math.log(subsubgts.values);
        } else {
          entropy = 1.0D;
        }
        
        // update seasonal. The more the values in the sub sycle series are similar, the more we want to substract the seasonal part.
        for (int w = 0; w < subsubgts.values; w++) {
          GTSHelper.setValue(seasonal, subsubgts.ticks[w], entropy * mu);
        }
      }
      
      GTSHelper.sort(seasonal);
      double m = median(subgts);
      
      int idx = 0;
      for (int i = 0; i < subgts.values; i++) {
        idx = Arrays.binarySearch(seasonal.ticks, idx, seasonal.values, subgts.ticks[i]);
        
        if (idx < 0) {
          throw new WarpScriptException("Internal bug method entropyHybridTest: can't find tick " + subgts.ticks[i] + " in seasonal.ticks");
        } else {
          subgts.doubleValues[i] -= (seasonal.doubleValues[idx] + m); 
        }
      }
      
      anomalous_ticks.addAll(ESDTest(subgts, k, true, alpha));
    }
    
    return anomalous_ticks;
  }
}

