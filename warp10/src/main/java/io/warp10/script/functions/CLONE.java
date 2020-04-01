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
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Clone (deep copy) the GTS on top of the stack or performs a shallow copy of collections
 */
public class CLONE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public CLONE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (o instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) o;

      GeoTimeSerie clone = gts.clone();
          
      stack.push(clone);      
    } else if (o instanceof Vector) {
      stack.push(new Vector<Object>((Vector<Object>) o));
    } else if (o instanceof List) {
      stack.push(new ArrayList<Object>((List<Object>) o));
    } else if (o instanceof Map) {
      stack.push(new LinkedHashMap<Object,Object>((Map<Object,Object>) o));
    } else if (o instanceof Set) {
      stack.push(new HashSet<Object>((Set<Object>) o));
    } else if (o instanceof GTSEncoder) {
      stack.push(((GTSEncoder) o).clone());
    } else {
      throw new WarpScriptException(getName() + " operates on List, Map, Set, Vector, Geo Time Series or GTS Encoder.");
    }
    
    // Push the original element back onto the stack
    stack.push(o);
    stack.swap();
    
    return stack;
  }
}
