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
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Mapper which multiplies together all (non null) values of its window
 */
public class MapperProduct extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptAggregatorFunction {

  public MapperProduct(String name) {
    super(name);
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
    long[] window = args.length > 7 ? (long[]) args[7] : null;

    if (0 == values.length) {
      return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }

    
    BigDecimal product = BigDecimal.ONE;
    boolean isDouble = false;
    
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof Long) {
        product = product.multiply(new BigDecimal((long) values[i]));
      } else if (values[i] instanceof Double) {
        isDouble = true;
        product = product.multiply(new BigDecimal((double) values[i]));
      } else if (null != values[i]) {
        throw new WarpScriptException(getName() + " can only be applied to LONG or DOUBLE values.");
      }
    }

    int tickidx = null != window ? (int) window[4] : 0;
    
    long location = locations[tickidx];
    long elevation = elevations[tickidx];        
    
    //
    // BEWARE(!!!) The JVM cannot correctly infer the return type of the following commented line, assuming double at all times
    // return new Object[] { tick, location, elevation, 0 == product.scale() ? (long) product.longValue() : (double) product.doubleValue()};
    // Therefore we use an outside conditional and two return statements
    if (!isDouble && 0 == product.scale()) {
      return new Object[] { tick, location, elevation, product.longValue()};
    } else {
      return new Object[] { tick, location, elevation, product.doubleValue()};      
    }
  }
  
}
