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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Inspects the top of the stack. If it is a list of values,
 * inspect each value and replace each value which was a list with its content.
 * Proceed recursively until all lists have been flattened.
 */
public class FLATTEN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final int MAX_NESTING = 16;
  
  public FLATTEN(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (top instanceof List) {
      stack.push(flatten(top, new AtomicInteger(0)));
    } else {
      stack.push(top);
    }
    
    return stack;
  }
  
  public List<Object> flatten(Object o, AtomicInteger nesting) throws WarpScriptException {
    
    if (nesting.addAndGet(1) > MAX_NESTING) {
      throw new WarpScriptException(getName() + " cannot flatten lists nested more than " + MAX_NESTING + " levels");
    }
    
    List<Object> l = new ArrayList<Object>();

    try {
      if (o instanceof List) {
        for (Object oo: (List) o) {
          l.addAll(flatten(oo, nesting));
        }
      } else {
        l.add(o);
        return l;
      }
      
      
      return l;      
    } finally {
      nesting.addAndGet(-1);
    }
  }
}
