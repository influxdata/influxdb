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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.math.BigInteger;

/**
 * Converts the hex representation in the string operand to a LONG
 */
public class FROMHEX extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FROMHEX(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (!(op instanceof String)) {
      throw new WarpScriptException(getName() + " can only operate on string of hexadecimal representations.");
    }
    
    if (op.toString().length() > 16) {
      throw new WarpScriptException(getName() + " can only operate on hexadecimal representations of 64 bits or less.");
    }
    
    long value = new BigInteger(op.toString(), 16).longValue();

    stack.push(value);
    
    return stack;
  }
}
