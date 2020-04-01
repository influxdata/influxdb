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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.math3.util.Pair;

import io.warp10.DoubleUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Perform Dynamic Time Warping distance computation
 * on subsequences of an array and return the indices
 * of subsequences with minimal distance and the associated distance.
 */
public class OPTDTW extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final DTW dtw = new DTW("", false, false);

  public OPTDTW(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a count of best results to return on top of the stack.");
    }
    
    int count = ((Number) o).intValue();
    
    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a numeric list to use as query below the count.");
    }

    double[] query = new double[((List) o).size()];
    int i = 0;
    for (Object oo: (List) o) {
      query[i++] = ((Number) oo).doubleValue();
    }
    
    // Z-Normalize query
    double[] musigma = DoubleUtils.musigma(query, true);
    for (i = 0; i < query.length; i++) {
      query[i] = (query[i] - musigma[0]) / musigma[1];
    }
    
    o = stack.pop();
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a numeric list as the sequence in which to find best matches below the 'query' list.");
    }

    double[] sequence = new double[((List) o).size()];
    i = 0;
    for (Object oo: (List) o) {
      sequence[i++] = ((Number) oo).doubleValue();
    }

    if (sequence.length <= query.length) {
      throw new WarpScriptException(getName() + " expects the query list to be shorter than the sequence list.");
    }
    
    double mindist = 0.0;
    
    PriorityQueue<Pair<Integer, Double>> distances = new PriorityQueue<Pair<Integer,Double>>(new Comparator<Pair<Integer,Double>>() {
      @Override
      public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });
    
    double[] subsequence = new double[query.length];
    
    for (i = 0; i <= sequence.length - query.length; i++) {
      System.arraycopy(sequence, i, subsequence, 0, query.length);
      // Z-Normalize the subsequence
      musigma = DoubleUtils.musigma(subsequence, true);
      for (int j = 0; j < subsequence.length; j++) {
        subsequence[j] = (subsequence[j] - musigma[0]) / musigma[1];
      }
      double dist = dtw.compute(query, 0, query.length, subsequence, 0, query.length, mindist);
      
      if (dist < 0) {
        continue;
      }
      
      distances.add(new Pair<Integer, Double>(i, dist));

      //
      // If the priority queue is of 'count' size, retrieve the largest distance and
      // use it as the threshold for the DTW computation
      //
        
      if (count > 0 && distances.size() >= count) {
        Object adist[] = distances.toArray();
        mindist = ((Pair<Integer,Double>) adist[count - 1]).getValue();
      }
    }
    
    List<List<Object>> results = new ArrayList<List<Object>>();
    
    while(!distances.isEmpty()) {
      
      Pair<Integer,Double> entry = distances.poll();
      
      List<Object> result = new ArrayList<Object>();
      result.add(entry.getKey());
      result.add(entry.getValue());
      results.add(result);
      
      if (count > 0 && count == results.size()) {
        break;
      }
    }
    
    stack.push(results);

    return stack;
  }
}
