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

import io.warp10.continuum.TimeSource;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Computes a timestamp which is N timeunits away from now.
 * This allows for handy timestamp specifications such as
 * 1 h AGO
 * AGO is really a synonym for 'NOW SWAP -'
 *
 */
public class AGO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public AGO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects an offset in time units on top of the stack.");
    }
    
    stack.push(TimeSource.getTime()  - ((Number) top).longValue());
    return stack;
  }
}
