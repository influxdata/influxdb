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

import java.nio.charset.StandardCharsets;

/**
 * Utility class to convert back and forth between DNA sequences
 * and Time Series.
 *
 * Same representation as the one used by Eamonn Keogh @see http://www.cs.ucr.edu/~eamonn/iSAX/DNA2TimeSeries.m
 * and @see http://www.cs.ucr.edu/~eamonn/iSAX.pdf
 */
public class DNAUtils {
  
  /**
   * Converts a DNA sequence into a GTS instance.
   * 
   * The most recent tick corresponds to the last base in the
   * DNA sequence.
   * 
   * @param dna DNS sequence to convert.
   * 
   * @return a GTS instance corresponding to the DNA sequence
   */
  public static GeoTimeSerie fromDNA(String dna) {
    GeoTimeSerie gts = new GeoTimeSerie(dna.length());
    
    long value = 0;
    int codepoint;
    
    for (int i = 0; i < dna.length(); i++) {
      codepoint = dna.codePointAt(i);
      
      switch(codepoint) {
        case 65: // 'A'
        case 97: // 'a'
          value = value + 2;
          break;
        case 71: // 'G'
        case 103: // 'g'
          value = value + 1;
          break;
        case 67: // 'C'
        case 99: // 'c'
          value = value - 1;
          break;
        case 84: // 'T'
        case 116: // 't'
          value = value - 2;
          break;
        default:
          ;
      }
      
      GTSHelper.setValue(gts, i, value);
    }
    
    return gts;
  }
  
  /**
   * Convert a time serie to a DNA sequence. The GTS instance
   * is assumed to be of type LONG.
   * 
   * Any incoherent value will produce a '.' in the resulting DNA
   * sequence.
   * 
   * @param gts
   * @return A DNA sequence representation of 'gts'
   */
  public static String toDNA(GeoTimeSerie gts) {
    
    //
    // If GTS is not of type 'LONG' or has no values, return the empty string
    //
    
    if (0 == gts.size() || !TYPE.LONG.equals(gts.getType())) {
      return "";
    }
    
    //
    // Sort ticks
    //
    
    GTSHelper.sort(gts);
    
    StringBuilder sb = new StringBuilder();    
    
    byte[] seq = new byte[gts.values];

    long value = 0;
    
    for (int i = 0; i < gts.values; i++) {      
      int delta = (int) (gts.longValues[i] - value);
      value = gts.longValues[i];
      
      switch (delta) {
        case 2:
          seq[i] = 0x41; // 'A'
          break;
        case 1:
          seq[i] = 0x47; // 'G'
          break;
        case -1:
          seq[i] = 0x43; // 'C'
          break;
        case -2:
          seq[i] = 0x54; // 'T'
          break;
        default:
          seq[i] = 0x2E; // '.'
      }
    }
    
    return new String(seq, StandardCharsets.UTF_8);
  }
}
