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

package io.warp10.script.mapper;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.SAXUtils;

import java.util.Map;

/**
 * Mapper which computes the bSAX symbol given a wordlength and a tick step
 */
public class MapperbSAX extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {
  
  /**
   * We voluntarily do not use the name 'bSAX'.
   */
  
  /**
   * Maximum number of symbols in a word
   */
  private static final int MAX_WORDLEN = 256;
  
  /**
   * Number of SAX symbols per bSAX word.
   */
  private final int wordlen;

  /**
   * How many symbols to skip
   */
  private final int step;

  /**
   * Number of levels in SAX symbols
   */
  private final int levels;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      // Size of sax alphabet
      Object a = stack.pop();
      // Number of sax symbols per word
      Object w = stack.pop();
      // Step to use to skip symbols
      Object k = stack.pop();
      
      if (!(a instanceof Number) || !(w instanceof Number) || !(k instanceof Number)) {
        throw new WarpScriptException("Invalid parameters for " + getName());
      }
      
      stack.push(new MapperbSAX(getName(), ((Number) a).intValue(), ((Number) w).intValue(), ((Number) k).intValue()));
      return stack;
    }
  }
  
  public MapperbSAX(String name, int alphabetSize, int wordlen, int step) throws WarpScriptException {
    
    super(name);
    
    if (wordlen < 2 || wordlen > MAX_WORDLEN) {
      throw new WarpScriptException("Word length must be between 2 and " + MAX_WORDLEN);
    }
    
    this.wordlen = wordlen;
    this.step = step;    
    
    //
    // Check if alphabetSize is a power of 2
    //
    
    int levels = 1;
    
    if (0 == alphabetSize) {
      throw new WarpScriptException("Alphabet size MUST be a power of two.");      
    }
    
    while(0 == (alphabetSize & 1)) {
      levels++;
      alphabetSize >>>= 1;
    }
    
    if (0 != alphabetSize) {
      throw new WarpScriptException("Alphabet size MUST be a power of two.");      
    }
    
    if (levels < 2 || levels > SAXUtils.SAX_MAX_LEVELS) {
      throw new WarpScriptException("Alphabet size MUST be a power of two between 2 and 2^" + SAXUtils.SAX_MAX_LEVELS);
    }

    this.levels = levels;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    String[] names = (String[]) args[1];
    Map<String,String>[] labels = (Map<String,String>[]) args[2];
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == values.length) {
      return new Object[] { 0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    
    int[] symbols = new int[this.wordlen];
    
    //
    // Ticks are assumed sorted since they come from a GTS instance created by GTSHelper.subSerie which
    // produces sorted GTS.
    //
    // We add values from oldest to most recent
    //
    
    for (int i = 0; i < this.wordlen; i++) {
      if (i * this.step < values.length) {
        symbols[i] = ((Number) values[i]).intValue();
      } else {
        // Fill missing values with the last one encountered
        if (i > 0) {
          symbols[i] = symbols[i - 1];
        }
      }
    }
    
    //
    // Compute bSAX word
    //
    
    byte[] word = SAXUtils.bSAX(this.levels, symbols);

    //
    // Encode bSAX word using our order preserving b64 implementation
    //
    
    return new Object[] { tick, locations[0], elevations[0], new String(OrderPreservingBase64.encode(word)) };
  }
}
