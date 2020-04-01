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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Constructs a timestamp for a given timezone given a list of elements (as produced by TSELEMENTS).
 */
public class FROMTSELEMENTS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final int MAX_TZ_CACHE_ENTRIES = 64;
  
  private final Map<String,DateTimeZone> tzones = new LinkedHashMap<String, DateTimeZone>() {
    protected boolean removeEldestEntry(java.util.Map.Entry<String, DateTimeZone> eldest) { return size() > MAX_TZ_CACHE_ENTRIES; };
  };
  
  public FROMTSELEMENTS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object obj = stack.peek();
    
    String tz = null;
    
    if (obj instanceof String) {
      tz = (String) obj;
      stack.pop();
    } else if (!(obj instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of timestamp elements or a list of timestamp elements + timezone.");
    }

    DateTimeZone dtz = this.tzones.get(tz);
    
    if (null == dtz) {
      dtz = DateTimeZone.forID(null == tz ? "UTC" : tz);
      this.tzones.put(tz, dtz);
    }
    
    obj = stack.pop();
        
    if (!(obj instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of timestamp elements or a list of timestamp elements + timezone.");
    }

    List<Object> elts = (List) obj;

    DateTime dt = new DateTime(elts.size() > 0 ? ((Number) elts.get(0)).intValue() : 1970,
        elts.size() > 1 ? ((Number) elts.get(1)).intValue() : 1,
        elts.size() > 2 ? ((Number) elts.get(2)).intValue() : 1,
        elts.size() > 3 ? ((Number) elts.get(3)).intValue() : 0,
        elts.size() > 4 ? ((Number) elts.get(4)).intValue() : 0,
        elts.size() > 5 ? ((Number) elts.get(5)).intValue() : 0,
        dtz);

    // Retrieve timestamp in ms
    
    long tsms = (dt.getMillis() / 1000L) * Constants.TIME_UNITS_PER_S;
    
    if (elts.size() > 6) {
      tsms += ((Number) elts.get(6)).intValue() % Constants.TIME_UNITS_PER_S;
    }
    
    stack.push(tsms);
    
    return stack;
  }
}
