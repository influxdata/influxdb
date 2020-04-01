//
//   Copyright 2020  SenX S.A.S.
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

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public abstract class ValueEncoder {
  
  private static final List<ValueEncoder> encoders = new ArrayList<ValueEncoder>();
  
  public static void register(ValueEncoder encoder) {
    encoders.add(encoder);
  }

  /**
   * Implementation of the actual parsing for a given ValueEncoder
   * 
   * @param value The string representation of the value to parse
   * @return The parsed value, expected to be either Long, Double, BigDecimal, String, Boolean or byte[]
   * @throws Exception
   */
  public abstract Object parseValue(String value) throws Exception;
  
  /**
   * Attempt to parse the given value using each registered ValueEncoder instance
   * until one returns a non null result.
   * 
   * If none of the ValueEncoder could parse the provided value, a ParseException is raised.
   * 
   * @param value
   * @return
   * @throws Exception
   */
  public static Object parse(String value) throws Exception {    
    for (ValueEncoder encoder: encoders) {
      Object o = encoder.parseValue(value);
      if (null != o) {
        
        if (o instanceof Long || o instanceof Double || o instanceof String || o instanceof Boolean || o instanceof BigDecimal || o instanceof byte[]) {
          return o;
        } else {
          throw new ParseException("Unsupported value type " + o.getClass() + " for value '" + value + "'", 0);          
        }
      }
    }
    throw new ParseException("Unsupported value '" + value + "'", 0);
  }
}
