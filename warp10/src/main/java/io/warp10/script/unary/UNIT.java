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

package io.warp10.script.unary;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Converts the value on the top of the stck into a different unit.
 */
public class UNIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /* factor from milliseconds to named unit. */
  private final double factor;

  public UNIT(String name, double factor) {
    super(name);
    this.factor = factor;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (!(op instanceof Number)) {
      throw new WarpScriptException(getName() + " can only operate on numeric values.");
    }
    
    long tick = Math.round(((Number) op).doubleValue() * factor * Constants.TIME_UNITS_PER_MS);
    
    stack.push(tick);
    
    return stack;
  }
}
