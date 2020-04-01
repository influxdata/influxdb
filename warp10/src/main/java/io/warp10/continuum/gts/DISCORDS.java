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

import io.warp10.script.GTSStackFunction;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Arrays;

/**
 * Detects discords in a GTS instance.
 * 
 * The way discords are detected is by computing a histogram of bSAX symbols.
 * Those symbols with the lowest frequency are suspects for being discords, each one
 * is compared to its closest neighbours (we heuristically assume that bSAX symbols appearing
 * close in lexicographic order are the ones containing the nearest neighbours).
 * The 'count' suspects with highest distance to nearest neighbors are considered the discords.
 *
 * The expected parameters on the stack are:
 * 
 * 6: GTS
 * 5: long windowLen
 * 4: long wordLen (must divide windowLen)
 * 3: long alphabetSize (must be a power of 2)
 * 2: long count, number of discords to identify
 * 1: boolean mayOverride
 */
public class DISCORDS extends GTSStackFunction {
    
  private static final String WINDOWLEN = "windowlen";
  private static final String WORDLEN = "wordlen";
  private static final String ALPHABETSIZE = "alphabetsize";
  private static final String COUNT = "count";
  private static final String OVERLAP = "overlap";
  private static final String DISTRATIO = "distratio";
  
  private final boolean standardizePAA;
  
  public DISCORDS(String name, boolean standardizePAA) {
    super(name);
    this.standardizePAA = standardizePAA;
  }
  
  @Override
  protected Object gtsOp(Map<String,Object> params, GeoTimeSerie gts) throws WarpScriptException {
    return discords(gts,
                    (int) params.get(WINDOWLEN),
                    (int) params.get(WORDLEN),
                    (int) params.get(ALPHABETSIZE),
                    (int) params.get(COUNT),
                    (boolean) params.get(OVERLAP),
                    (double) params.get(DISTRATIO),
                    this.standardizePAA);
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {

    Map<String,Object> params = new HashMap<String, Object>();

    Object dratio = stack.pop();
    
    if (!(dratio instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a double distance ratio.");
    }
    
    double dr = (double) dratio;
    
    if (dr < 0.0D) {
      throw new WarpScriptException(getName() + " expects a positive distance ratio.");
    }
    
    params.put(DISTRATIO, (double) dr);
    
    Object overlap = stack.pop();
    
    if (!(overlap instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a boolean flag to indicate tolerance to overlapping.");
    }
    params.put(OVERLAP, (boolean) overlap);
    
    Object count = stack.pop();
    
    if (!(count instanceof Long)) {
      throw new WarpScriptException(getName() + " expects an integer discord count.");
    }
    params.put(COUNT, ((Number) count).intValue());
    
    Object alphabetsize = stack.pop();
    
    if (!(alphabetsize instanceof Long)) {
      throw new WarpScriptException(getName() + " expects an integer quantization scale.");
    }
    int asize = ((Number) alphabetsize).intValue();
    
    if (0 != (asize & (asize - 1)) || asize < 2) {
      throw new WarpScriptException(getName() + " expects a quantization scale which is a power of 2 greater or equal to 2.");
    }
    params.put(ALPHABETSIZE, asize);
    
    Object wordlen = stack.pop();
    Object windowlen = stack.pop();
    
    if (!(wordlen instanceof Long)) {
      throw new WarpScriptException(getName() + " expects an integer pattern length.");
    }
    if (!(windowlen instanceof Long)) {
      throw new WarpScriptException(getName() + " expects an integer detection window length.");
    }
    
    int wolen = ((Number) wordlen).intValue();
    int wilen = ((Number) windowlen).intValue();
    
    if (0 != (wilen % wolen)) {
      throw new WarpScriptException(getName() + " expects pattern length to divide detection window length.");
    }
    params.put(WORDLEN, wolen);
    params.put(WINDOWLEN, wilen);
    
    return params;
  }
  
  /**
   * 
   * @param gts GTS instance to analyze
   * @param windowLen Number of ticks in sliding window
   * @param wordLen Size of SAX symbols
   * @param alphabetSize Size of SAX alphabet (MUST be a power of 2)
   * @param count Maximum number of discords to identify
   * @param mayOverlap Flag indicating whether or not identified discords may overlap
   * @param distRatio Maximum tolerable ratio between NN distances. Once an NN dist is smaller than 'distRatio' times the previous candidate, exit
   * @return
   * @throws WarpScriptException
   */
  public static final GeoTimeSerie discords(GeoTimeSerie gts, int windowLen, int wordLen, int alphabetSize, int count, boolean mayOverlap, double distRatio, boolean standardizePAA) throws WarpScriptException {
    //
    // Compute bSAX symbols
    //
    GeoTimeSerie symbols = GTSHelper.bSAX(gts, alphabetSize, wordLen, windowLen, standardizePAA);

    //
    // Compute Map of symbol to list of locations where it appears
    //
    
    final SortedMap<String, List<Integer>> locationLists = new TreeMap<String, List<Integer>>();
    
    for (int i = 0; i < symbols.values; i++) {
      String symbol = GTSHelper.valueAtIndex(symbols, i).toString();
      if (!locationLists.containsKey(symbol)) {
        locationLists.put(symbol, new ArrayList<Integer>());
      }
      locationLists.get(symbol).add(i);
    }
    
    //
    // Sort symbols according to number of occurrences
    //
    
    List<String> symbolsByOccurrences = new ArrayList<String>();
      
    symbolsByOccurrences.addAll(locationLists.keySet());
    
    String[] rawSymbols = symbolsByOccurrences.toArray(new String[symbolsByOccurrences.size()]);
    
    Collections.sort(symbolsByOccurrences, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int n1 = locationLists.get(o1).size();
        int n2 = locationLists.get(o2).size();
        
        return n1 - n2;
      }
    });
    
    //
    // Convert List<Integer> to int[]
    //
    
    SortedMap<String,int[]> locations = new TreeMap<String,int[]>();
    
    for (Map.Entry<String, List<Integer>> symbolAndIlocs: locationLists.entrySet()) {
      String symbol = symbolAndIlocs.getKey();
      List<Integer> ilocs = symbolAndIlocs.getValue();
      int[] locs = new int[ilocs.size()];
      Collections.sort(ilocs);
      for (int i = 0; i < locs.length; i++) {
        locs[i] = ilocs.get(i);
      }
      locations.put(symbol, locs);
    }
    
    //
    // PriorityQueue for storing possible discords
    //
    
    final class DiscordCandidate {
      final String symbol;
      final int location;
      /**
       * Distance to nearest neighbour
       */
      final double nndist;
      
      public DiscordCandidate(String symbol, int location, double nndist) {
        this.symbol = symbol;
        this.location = location;
        this.nndist = nndist;
      }
      
      @Override
      public String toString() {
        return new StringBuilder().append(this.symbol).append("@").append(this.location).append(":").append(this.nndist).toString();
      }
    };
    
    List<DiscordCandidate> discords = new ArrayList<DiscordCandidate>();
    
    //
    // Worst NN distance so far. Any distance less than that means the candidate is not a discord
    //
    
    double worstNNDist = 0.0D;
    
    //
    // Loop over the symbols, from least common to most common
    //
    
    for (String symbol: symbolsByOccurrences) {
      
      int[] symbolLocations = locations.get(symbol);
      
      //
      // Exit the loop when we've already identified 'count' candidates and when the number
      // of occurrences of the current symbol is greater than 'windowLen' + 'count', meaning that
      // we will probably have a very close neighbor anyway
      //
      
      if (discords.size() >= count && symbolLocations.length > count + windowLen) {
        break;
      }
      
      //
      // Loop over each occurrence of the symbol
      //
      
      int size = symbolLocations.length;
      
      for (int i = 0; i < size; i++) {

        // Initialize NN dist to +infinity
        double nndist = Double.POSITIVE_INFINITY;
        
        //
        // Compute distance to nearest neighbor for the same symbol outside the window
        //
        
        for (int j = 0; j < size; j++) {
          if (Math.abs(symbolLocations[i] - symbolLocations[j]) < windowLen) {
            continue;
          }
          
          //
          // Compute distance to next location of the same symbol
          //
          
          double dist = 0.0D;
          
          for (int k = 0; k < windowLen; k++) {
            dist += Math.pow(((Number) GTSHelper.valueAtIndex(gts, symbolLocations[i] + k)).doubleValue()
                - ((Number) GTSHelper.valueAtIndex(gts, symbolLocations[j] + k)).doubleValue() , 2.0D);
          }
          
          if (dist < nndist) {
            nndist = dist;
          }
          
          //
          // If nndist is less than the worst NN Dist we've encountered and we already have 'count' candidates, exit the loop
          //
          
          if (discords.size() == count && nndist < worstNNDist) {
            break;
          }
        }
        
        //
        // If nndist is less than the worst NN Dist we've encountered and we already have 'count' candidates, exit the loop
        //
        
        if (discords.size() == count && nndist < worstNNDist) {
          continue;
        }

        //
        // Compute distance to neighbors with the symbol just before the current symbol
        //
        int idx = Arrays.binarySearch(rawSymbols, symbol);
        
        int idx2 = idx - 1;
        
        while(idx2 >= 0) {
          int[] symbolLocations2 = locations.get(rawSymbols[idx2]);
          for (int j = 0; j < symbolLocations2.length; j++) {
            if (Math.abs(symbolLocations[i] - symbolLocations2[j]) < windowLen) {
              continue;
            }
            
            //
            // Compute distance to next location of the same symbol
            //
            
            double dist = 0.0D;
            
            for (int k = 0; k < windowLen; k++) {
              dist += Math.pow(((Number) GTSHelper.valueAtIndex(gts, symbolLocations[i] + k)).doubleValue()
                  - ((Number) GTSHelper.valueAtIndex(gts, symbolLocations2[j] + k)).doubleValue() , 2.0D);
            }
            
            if (dist < nndist) {
              nndist = dist;
            }
            
            //
            // If nndist is less than the worst NN Dist we've encountered and we already have 'count' candidates, exit the loop
            //
            
            if (discords.size() == count && nndist < worstNNDist) {
              break;
            }
          }          
          
          //
          // Exit if nndist is no longer +Infinity
          //
          
          if (Double.POSITIVE_INFINITY != nndist) {
            break;
          }
         
          idx2--;
        }

        if (discords.size() == count && nndist < worstNNDist) {
          continue;
        }

        //
        // Compute distance to neighbors with the symbol just after the current symbol
        //

        idx2 = rawSymbols.length + 1;
        
        while(idx2 < rawSymbols.length) {
          
          int[] symbolLocations2 = locations.get(rawSymbols[idx2]);
          for (int j = 0; j < symbolLocations2.length; j++) {
            if (Math.abs(symbolLocations[i] - symbolLocations2[j]) < windowLen) {
              continue;
            }
            
            //
            // Compute distance to next location of the same symbol
            //
            
            double dist = 0.0D;
            
            for (int k = 0; k < windowLen; k++) {
              dist += Math.pow(((Number) GTSHelper.valueAtIndex(gts, symbolLocations[i] + k)).doubleValue()
                  - ((Number) GTSHelper.valueAtIndex(gts, symbolLocations2[j] + k)).doubleValue() , 2.0D);
            }
            
            if (dist < nndist) {
              nndist = dist;
            }
            
            //
            // If nndist is less than the worst NN Dist we've encountered and we already have 'count' candidates, exit the loop
            //
            
            if (discords.size() == count && nndist < worstNNDist) {
              break;
            }
          }
          //
          // Exit if nndist is no longer +Infinity
          //
          
          if (Double.POSITIVE_INFINITY != nndist) {
            break;
          }
         
          idx2++;
        }

        if (discords.size() == count && nndist < worstNNDist) {
          continue;
        }

        //
        // Store nndist
        //
        
        DiscordCandidate candidate = new DiscordCandidate(symbol, symbolLocations[i], nndist);
        
        discords.add(candidate);
        
        //
        // Sort candidates according to decreasing nndist
        //
        
        Collections.sort(discords, new Comparator<DiscordCandidate>() {
          public int compare(DiscordCandidate o1, DiscordCandidate o2) {
            return (int) Math.signum(o2.nndist - o1.nndist);
          };
        });
        
        //
        // Remove overlapping candidates if they are forbidden
        //
        
        if (!mayOverlap) {
          for (int h = discords.size() - 1; h > 0; h--) {
            if (Math.abs(discords.get(h).location - discords.get(h - 1).location) < windowLen) {
              discords.remove(h);
              h--;
            }
          }
        }

        //
        // Only retain 'count' candidates
        //        
                
        while(discords.size() > count) {
          discords.remove(count);
        }
        
        //
        // If 'distRatio' is not 0.0, remove any candidate whose nndist is smaller than '1/distRatio' the first candidate's nndist
        //
        
        if (0.0D != distRatio) {
          while(discords.size() > 1) {
            if (discords.get(discords.size() - 1).nndist * distRatio < discords.get(0).nndist) {
              discords.remove(discords.size() - 1);
            }
          }
        }
        
        //
        // Compute worstNNDist
        //
        
        worstNNDist = discords.get(discords.size() - 1).nndist;
        
        //
        // TODO(hbs): We would need to have an early exit once we think we won't find anymore candidates
        //
      }                
    }

    //System.out.println(discords);
    
    /*
    for (String symbol: symbolsByOccurrences) {
      if (locations.get(symbol).size() > 100) {
        continue;
      }
      System.out.println(symbol + " >>> " + locations.get(symbol).size() + " >>> " + locations.get(symbol));
    }
    */
    
    //
    // Build the resulting GTS
    //
    
    GeoTimeSerie discordsGTS = gts.cloneEmpty();
    
    for (DiscordCandidate discord: discords) {
      for (int i = 0; i < windowLen; i++) {
        GTSHelper.setValue(discordsGTS,
            GTSHelper.tickAtIndex(gts, discord.location + i),
            GTSHelper.locationAtIndex(gts, discord.location + i),
            GTSHelper.elevationAtIndex(gts, discord.location + i),
            GTSHelper.valueAtIndex(gts, discord.location + i),
            false);
      }
    }
        
    return GTSHelper.dedup(discordsGTS);
  }
}
