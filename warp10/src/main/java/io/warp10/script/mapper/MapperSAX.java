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
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.SAXUtils;

import java.util.Map;

/**
 * Mapper which computes the SAX symbol of a value given a SAX alphabet size
 */
public class MapperSAX extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {
  
  /**
   * We voluntarily do not use the name 'SAX'.
   */
  
  private final int levels;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();
      
      if (!(value instanceof Number)) {
        throw new WarpScriptException("Invalid parameter for " + getName());
      }
      
      stack.push(new MapperSAX(getName(), ((Number) value).intValue()));
      return stack;
    }
  }
  
  public MapperSAX(String name, int alphabetSize) throws WarpScriptException {
    super(name);

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
    
    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }
    
    if (! (values[0] instanceof Number)) {
      throw new WarpScriptException(getName() + " can only be applied to numeric values.");
    }
    
    return new Object[] { tick, locations[0], elevations[0], (long) SAXUtils.SAX(this.levels, ((Number) values[0]).doubleValue()) };
  }
}
