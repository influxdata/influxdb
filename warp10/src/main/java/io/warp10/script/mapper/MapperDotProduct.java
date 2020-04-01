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
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.List;
import java.util.Map;

/**
 * Mapper which operates a dot product between an input vector and the sliding window
 */
public class MapperDotProduct extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {

  private final double[] omega;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
        
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object top = stack.pop();
      
      if (!(top instanceof List)) {
        throw new WarpScriptException(getName() + " expects an input vector (list of doubles) on top of the stack.");
      }
            
      double[] omega = new double[((List) top).size()];
      
      for (int i = 0; i < omega.length; i++) {
        double d = (double) ((List) top).get(i);
        omega[i] = d;
      }
      
      stack.push(new MapperDotProduct(getName(), omega));
      
      return stack;
    }
  }

  public MapperDotProduct(String name, double[] omega) {
    super(name);
    this.omega = omega;
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
    long[] window = (long[]) args[7];

    if (0 == values.length || (this.omega.length != values.length)) {
      return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }

    if (!(values[0] instanceof Number)) {
      throw new WarpScriptException(getName() + " can only be applied to LONG or DOUBLE values.");
    }

    int tickidx = (int) window[4];
    
    long location = locations[tickidx];
    long elevation = elevations[tickidx];        
    
    double dotproduct = 0.0D;
    
    for (int i = 0; i < omega.length; i++) {
      dotproduct += omega[i] * ((Number) values[i]).doubleValue(); 
    }
    
    return new Object[] { tick, location, elevation, dotproduct };
  }
  
  public double[] getOmega() {
    return this.omega;
  }
    
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(WarpScriptLib.LIST_START);
    sb.append(" ");
    for (double d: this.omega) {
      sb.append(StackUtils.toString(d));
      sb.append(" ");
    }
    sb.append(WarpScriptLib.LIST_END);
    sb.append(" ");
    sb.append(this.getName());
    
    return sb.toString();
  }
}
