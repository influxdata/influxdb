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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
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
 * Return the concatenation of the string representation of values separated by the join string
 * elevation and location are from the latest measure.
 */
public class Join extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptBucketizerFunction, WarpScriptReducerFunction {
  
  private final boolean ignoreNulls;

  private final boolean urlencode;
  
  private final String separator;
  
  private final String nullString;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    private final boolean ignoreNulls;
    
    private final boolean urlencode;
    
    private final String nulLString;
    
    public Builder(String name, boolean ignoreNulls, boolean urlencode, String nullString) {
      super(name);
      this.ignoreNulls = ignoreNulls;
      this.urlencode = urlencode;
      this.nulLString = nullString;
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object sep = stack.pop();
      
      stack.push(new Join(getName(), sep.toString(), ignoreNulls, urlencode, nulLString));
      
      return stack;
    }
    
  }
  
  public Join(String name, String separator, boolean ignoreNulls, boolean urlencode, String nullString) {
    super(name);
    this.separator = separator;
    
    this.ignoreNulls = ignoreNulls;
    this.urlencode = urlencode;
    this.nullString = nullString;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    if (0 == ticks.length) {
      return new Object[] { Long.MAX_VALUE, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };
    }
    
    StringBuilder sb = new StringBuilder();
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    long timestamp = Long.MIN_VALUE;
    
    boolean hasNulls = false;
    
    boolean first =  true;
    
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
    
      if (ticks[i] > timestamp) {
        location = locations[i];
        elevation = elevations[i];
        timestamp = ticks[i];
      }
    
      if (null == value) {
        hasNulls = true;
        
        if (null != nullString) {
          if (!first) {
            sb.append(separator);
          }
          sb.append(nullString);
        }
        first = false;
        continue;
      }

      if (!first) {
        sb.append(separator);
      }

      first = false;

      if (urlencode) {
        try {
          sb.append(WarpURLEncoder.encode(value.toString(), StandardCharsets.UTF_8));
        } catch (UnsupportedEncodingException uee) {
          throw new WarpScriptException(uee);
        }
      } else {
        sb.append(value.toString());
      }     
    }

    String result = null;
    
    if (hasNulls && !this.ignoreNulls) {
      result = null;
    } else {
      result = sb.toString();
    }
    
    return new Object[] { 0L, location, elevation, result };    
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(StackUtils.toString(this.separator));
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }  
}
