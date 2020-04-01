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

package io.warp10.script.aggregator;

import com.geoxp.GeoXPLib;

import io.warp10.DoubleUtils;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Compute the mean of circular quantities
 * 
 * @see https://en.wikipedia.org/wiki/Mean_of_circular_quantities
 */
public class CircularMean extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptBucketizerFunction {
  
  /**
   * Period of the circular quantity
   */
  private final double period;
  
  /**
   * True if we forbid nulls
   */
  private final boolean forbidNulls;
  
  public CircularMean(String name, double period, boolean forbidNulls) {
    super(name);
    this.period = period;
    this.forbidNulls = forbidNulls;
  }
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    private final boolean forbidNulls;
    
    public Builder(String name, boolean forbidNulls) {
      super(name);
      this.forbidNulls = forbidNulls;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object o = stack.pop();
      
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a finite, strictly positive, numeric 'period' parameter.");
      }
      
      double period = ((Number) o).doubleValue();
      
      if (!DoubleUtils.isFinite(period) || period <= 0.0D) {
        throw new WarpScriptException(getName() + " expects a finite, strictly positive, numeric 'period' parameter.");
      }
      
      stack.push(new CircularMean(getName(), period, this.forbidNulls));
      
      return stack;
    }    
  }
  
  @Override
  public Object apply(Object[] args) throws io.warp10.script.WarpScriptException {
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == ticks.length) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    double sinSum = 0.0D;
    double cosSum = 0.0D;

    TYPE type = null;
    
    long latitudes = 0L;
    long longitudes = 0L;
    int locationcount = 0;
    long elev = 0L;
    int elevationcount = 0;

    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    int nticks = 0;
    
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];

      if (null == value && this.forbidNulls) {
        return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
      } else if (null == value) {
        continue;
      }
    
      nticks++;
      
      if (GeoTimeSerie.NO_LOCATION != locations[i]) {
        long[] xy = GeoXPLib.xyFromGeoXPPoint(locations[i]);
        latitudes += xy[0];
        longitudes += xy[1];
        locationcount++;
      }

      if (GeoTimeSerie.NO_ELEVATION != elevations[i]) {
        elev += elevations[i];
        elevationcount++;
      }
      
      if (null == type) {
        // No type detected yet,
        // check value
        
        if (value instanceof Number) {
          type = TYPE.DOUBLE;
          double v = ((Number) value).doubleValue();
          v = Math.PI * 2.0D * (v / this.period);
          sinSum = Math.sin(v);
          cosSum = Math.cos(v);
        } else {
          //
          // Mean of String or Boolean has no meaning
          //
          return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
        }        
      } else {
        double v = ((Number) value).doubleValue(); 
        v = Math.PI * 2.0D * (v / this.period);
        sinSum += Math.sin(v);
        cosSum += Math.cos(v);
      }
    }

    double circularmean = 0 == nticks ? Double.NaN : Math.atan2(sinSum, cosSum);
    
    if (!Double.isNaN(circularmean)) {
      circularmean = circularmean * this.period / (2.0D * Math.PI);
    }

    if (locationcount > 0) {
      latitudes = latitudes / locationcount;
      longitudes = longitudes / locationcount;
      location = GeoXPLib.toGeoXPPoint(latitudes, longitudes);
    }
    
    if (elevationcount > 0) {
      elevation = elev / elevationcount;
    }
    
    return new Object[] { 0L, location, elevation, circularmean };
  }
  
  @Override
  public String toString() {
    return StackUtils.toString(this.period) + " " + this.getName();
  }
}
