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

package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.List;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Adds a month to a timestamp
 */
public class ADDMONTHS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ADDMONTHS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
  
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of months on top of the stack.");
    }
    
    int months = ((Number) top).intValue();
    
    top = stack.pop();
    
    String tz = null;
    
    if (top instanceof String) {
      tz = top.toString();
      top = stack.pop();
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
      }
    } else if (!(top instanceof List) && !(top instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
    }
    
    if (top instanceof Long) {
      long instant = ((Number) top).longValue();
        
      if (null == tz) {
        tz = "UTC";
      }

      DateTimeZone dtz = DateTimeZone.forID(tz);
    
      DateTime dt = new DateTime(instant / Constants.TIME_UNITS_PER_MS, dtz);
    
      dt = dt.plusMonths(months);
    
      long ts = dt.getMillis() * Constants.TIME_UNITS_PER_MS + (instant % Constants.TIME_UNITS_PER_MS);
    
      stack.push(ts);
    } else {
      List<Object> elts = new ArrayList<Object>((List<Object>) top);
      
      int year = ((Number) elts.get(0)).intValue();
      int month = ((Number) elts.get(1)).intValue();

      if (months < 0) {
        while(months < 0) {
          months++;
          month = month - 1;
          if (month < 1) {
            month = 12;
            year--;
          }
        }        
      } else {
        while(months > 0) {
          months--;
          month = month + 1;
          if (month > 12) {
            month = 1;
            year++;
          }
        }
      }
      
      elts.set(0, (long) year);
      elts.set(1, (long) month);
      
      // Now check that the month is compatible with the day
      
      if (elts.size() > 2) {
        int day = ((Number) elts.get(2)).intValue();

        if (2 == month && day > 28) {
          if ((0 != year % 4) || (0 == year % 100)) {
            elts.set(2, (long) 28);
          } else {
            elts.set(2, (long) 29);
          }
        } else if (day > 30 && (4 == month || 6 == month || 9 == month || 11 == month)) {
          elts.set(2, (long) 30);
        }
      }
     
      stack.push(elts);
    }
        
    return stack;
  }
}
