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

/**
 * Checks the two operands on top of the stack for equality, accepting a difference under a configurable lambda value
 *
 * ALMOSTEQ expects the following parameters on the stack:
 * 3: lambda The tolerance of the comparision
 * 2: op2
 * 1: op1
 * Both op1 and op2 need to be instances of Number, and at least one of them need to be instance of Double
 * lambda needs to be a Number, and it will be used in absolute value
 */
public class ALMOSTEQ extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ALMOSTEQ(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object lambdaParam = stack.pop();
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    double lambda;

    if (lambdaParam instanceof Number) {
      lambda = Math.abs(((Number) lambdaParam).doubleValue());
    } else {
      throw new WarpScriptException(getName() + " needs three parameters on the stack: 'lambda' (a numeric type) and two operands.");  
    }

    // At least one of the operands must be a Double and both operands need to be numeric types
    if  (!(op2 instanceof Number)  || !(op1 instanceof Number) || !(op2 instanceof Double || op1 instanceof Double)) {
      throw new WarpScriptException(getName() + " can only operate on numeric types and at least one of them must be a Double.");  
    }
    
    if (Double.isNaN(((Number) op1).doubleValue()) || Double.isNaN(((Number) op2).doubleValue())) {
      stack.push(Double.isNaN(((Number) op1).doubleValue()) && Double.isNaN(((Number) op2).doubleValue()));
    } else {
      stack.push(lambda > Math.abs(((Number) op1).doubleValue() - ((Number) op2).doubleValue()));  
    }

    return stack;
  }
}
