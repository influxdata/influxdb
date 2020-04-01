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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Compute a UUID for an input map
 * 
 */
public class MAPID extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Random keys for siphash computation
   */
  private static final long[] key1 = new long[] { 0x39A9DD7D71B64E3CL, 0xA14C3749DCAAB408L };
  private static final long[] key2 = new long[] { 0xB5BBEC1071A64C48L, 0xB872C16B37A07597L };
  
  public MAPID(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Map)) {
      throw new WarpScriptException(getName() + " can only operate on a map.");
    }
    
    Map<String,String> map = new HashMap<String,String>();
    
    for(Entry<Object,Object> entry: ((Map<Object,Object>) o).entrySet()) {
      map.put(entry.getKey().toString(), entry.getValue().toString());
    }
    
    //
    // Compute two labelsId, merging both into a UUID
    //
    
    long msb = GTSHelper.labelsId(key1, map);
    long lsb = GTSHelper.labelsId(key2, map);
    
    UUID uuid = new UUID(msb, lsb);
    
    stack.push(uuid.toString());
    
    return stack;
  }
}
