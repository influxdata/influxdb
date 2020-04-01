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

import io.warp10.Revision;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.List;

/**
 * Push a range on the stack. Mimics Python's 'range' functions
 *
 * 1: [ start stop step ] OR [ start stop ] OR [ stop ]
 * 
 * step defaults to 1
 * start defaults to 0
 *
 */
public class RANGE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RANGE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of parameters on top of the stack.");
    }
    
    int n = ((List) top).size();
    
    if (n < 1 || n > 3) {
      throw new WarpScriptException(getName() + " expects a list of 1 to 3 longs on top of the stack.");
    }
    
    long start = 0L;
    long step = 1L;
    long stop = start;
    
    switch (n) {
      case 1:
        stop = ((Number) ((List) top).get(0)).longValue();
        break;
      case 2:
        start = ((Number) ((List) top).get(0)).longValue();
        stop = ((Number) ((List) top).get(1)).longValue();
        break;
      case 3:
        start = ((Number) ((List) top).get(0)).longValue();
        stop = ((Number) ((List) top).get(1)).longValue();
        step = ((Number) ((List) top).get(2)).longValue();
        break;
    }
    
    if (step <= 0L) {
      throw new WarpScriptException(getName() + " step MUST be > 0.");
    }
    
    List<Long> range = new ArrayList<Long>();
    
    while (start < stop) {
      range.add(start);
      start += step;
    }
    
    stack.push(range);
    
    return stack;
  }
}
