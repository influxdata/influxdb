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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class RUN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public RUN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object symbol = stack.pop();

    if (symbol instanceof String) {
      stack.run(symbol.toString());
    } else if (symbol instanceof Long) {
      Object reg = stack.load(((Number) symbol).intValue());
      if (reg instanceof WarpScriptStack.Macro) {
        stack.exec((WarpScriptStack.Macro) reg);
      } else {
        throw new WarpScriptException(getName() + " expects register number " + symbol + " to contain a macro.");
      }
    } else {
      throw new WarpScriptException(getName() + " expects macro name to be a STRING or a register number (LONG).");
    }

    return stack;
  }
}
