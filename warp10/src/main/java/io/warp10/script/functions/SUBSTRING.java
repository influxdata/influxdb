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

import java.util.Arrays;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class SUBSTRING extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SUBSTRING(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a numeric length or 0-based start index on top of the stack.");
    }
    
    int n = ((Number) top).intValue();
    
    top = stack.pop();

    if (top instanceof String) {
      String str = top.toString();
      n = GET.computeAndCheckIndex(n, str.length());
      stack.push(str.substring(n));
      return stack;
    } else if (top instanceof byte[]) {
      byte[] bytes = (byte[]) top;
      n = GET.computeAndCheckIndex(n, bytes.length);
      stack.push(Arrays.copyOfRange(bytes, n, bytes.length));
      return stack;
    }
        
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a 0-based start index below the length.");
    }
    
    int idx = ((Number) top).intValue();
    
    top = stack.pop();
    
    if (top instanceof String) {
      String str = top.toString();
      idx = GET.computeAndCheckIndex(idx, str.length());
      stack.push(top.toString().substring(idx, Math.min(n + idx, str.length())));
    } else if (top instanceof byte[]) {
      byte[] bytes = (byte[]) top;
      idx = GET.computeAndCheckIndex(idx, bytes.length);
      stack.push(Arrays.copyOfRange(bytes, idx, Math.min(n + idx, bytes.length)));
    } else {
      throw new WarpScriptException(getName() + " can only operate on strings or byte arrays.");
    }
        
    return stack;
  }
}
