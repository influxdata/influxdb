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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Set the value of a counter
 */
public class COUNTERSET extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public COUNTERSET(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a value (LONG) on top of the stack.");
    }

    long value = ((Number) top).longValue();

    top = stack.pop();

    if (!(top instanceof AtomicLong)) {
      throw new WarpScriptException(getName() + " expects a counter below the value.");
    }

    ((AtomicLong) top).set(value);

    stack.push(top);

    return stack;
  }
}
