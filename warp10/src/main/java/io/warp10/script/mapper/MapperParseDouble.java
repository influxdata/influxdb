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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Map;

/**
* Mapper which returns the absolute value of the value passed as parameter
*/
public class MapperParseDouble extends NamedWarpScriptFunction implements WarpScriptMapperFunction, WarpScriptAggregatorFunction {

  private final NumberFormat format;
  
  public MapperParseDouble(String name) {
    super(name);
    this.format = null;
  }
  
  public MapperParseDouble(String name, Object language) throws WarpScriptException {   
    super(name);
    
    if (language instanceof String) {
      Locale locale = Locale.forLanguageTag((String) language);
      format = NumberFormat.getInstance(locale);
    } else {
      throw new WarpScriptException("Invalid parameter type for " + getName() + ", expects a language tag (a string) on top of the stack.");
    }
  }
  
  /**
   * Builder for case user specify a language tag according to an
   * IETF BCP 47 language tag string of the Locale Java class
   */
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object language = stack.pop();
      
      if (!(language instanceof String)) {
        throw new WarpScriptException("Invalid parameter type for " + getName() + ", expects a language tag (a string) on top of the stack.");
      }
      
      stack.push(new MapperParseDouble(getName(), language));
      return stack;
    }
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

    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }
    
    Object value = null;
    long location = locations[0];
    long elevation = elevations[0];
    
    if (null != values[0]) {
      if (values[0] instanceof String) {
        try {          
          value = format.parse((String) values[0]).doubleValue();
        } catch (NumberFormatException | ParseException e) {
          value = null;
        }
      }
    }
    
    return new Object[] { tick, location, elevation, value };
  }
}
