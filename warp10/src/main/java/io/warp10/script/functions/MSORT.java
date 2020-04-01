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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sorts a map according to its keys if it is a LinkedHashMap
 */
public class MSORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public MSORT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {        
    Object obj = stack.peek();

    if (!(obj instanceof LinkedHashMap)) {
      throw new WarpScriptException(getName() + " operates on a map created by {} or ->MAP.");
    }

    List<Object> lkeys = new ArrayList<Object>();
    lkeys.addAll(((LinkedHashMap) obj).keySet());
    
    Collections.sort((List) lkeys);
    
    //
    // Build a linked hash map with the ordered entries
    //
    
    LinkedHashMap<Object,Object> entries = new LinkedHashMap<Object,Object>();
    
    for (Object key: lkeys) {
      entries.put(key, ((Map) obj).get(key));
    }
    
    //
    // Clear the original map and fill it with the ordered entries
    //
    
    ((Map) obj).clear();
    
    for (Object key: lkeys) {
      ((Map) obj).put(key, entries.get(key));
    }
    
    return stack;
  }
}
